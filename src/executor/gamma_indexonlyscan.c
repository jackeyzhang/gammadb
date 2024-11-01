/*
 * Copyright (c) 2024 Gamma Data, Inc. <jackey@gammadb.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/heapam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "lib/hyperloglog.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/datum.h"
#include "utils/logtape.h"
#include "utils/rel.h"

#include "nodes/extensible.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

#include "executor/gamma_indexonlyscan.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "storage/ctable_am.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

/* CustomScanMethods */
static Node *create_gamma_indexonlyscan_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void gamma_indexonlyscan_begin(CustomScanState *node,
							 EState *estate, int eflags);
static void gamma_indexonlyscan_rescan(CustomScanState *node);
static TupleTableSlot* gamma_indexonlyscan_exec(CustomScanState *node);
static void gamma_indexonlyscan_end(CustomScanState *node);

static Plan * gamma_plan_indexonlyscan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);
static TupleTableSlot * gamma_indexonlyscan_access_indexnext(IndexOnlyScanState *node);
static bool gamma_indexonlyscan_access_indexrecheck(IndexOnlyScanState *node,
													TupleTableSlot *slot);

/*
 * GammaIndexOnlyScanScanState - state object of vectorized resultregate on executor.
 */
typedef struct GammaIndexOnlyScanState
{
	CustomScanState	css;
	IndexOnlyScanState *indexstate;
} GammaIndexOnlyScanState;

static CustomPathMethods gamma_indexonlyscan_path_methods = {
	"gamma_indexonlyscan",
	gamma_plan_indexonlyscan,
};

static CustomScanMethods gamma_indexonlyscan_scan_methods = {
	"gamma_indexonlyscan",
	create_gamma_indexonlyscan_state,
};

static CustomExecMethods gamma_indexonlyscan_exec_methods = {
	"gamma_indexonlyscan",
	gamma_indexonlyscan_begin,
	gamma_indexonlyscan_exec,
	gamma_indexonlyscan_end,
	gamma_indexonlyscan_rescan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

void
gamma_indexonlyscan_init(void)
{
	RegisterCustomScanMethods(&gamma_indexonlyscan_scan_methods);
}

const CustomPathMethods*
gamma_indexonlyscan_methods(void)
{
	return &gamma_indexonlyscan_path_methods;
}

bool
gamma_is_indexonlyscan_customscan(CustomScan *cscan)
{
	return ((void *)cscan->methods == (void *)&gamma_indexonlyscan_scan_methods);
}

static Plan *
gamma_plan_indexonlyscan(PlannerInfo *root,
		RelOptInfo *rel,
		CustomPath *best_path,
		List *tlist,
		List *clauses,
		List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	Assert(list_length(custom_plans) == 1);
	Assert(tlist != NULL);

	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.targetlist = (List *) copyObject(tlist);
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = (List *)copyObject(tlist);

	cscan->custom_plans = custom_plans;

	cscan->methods = &gamma_indexonlyscan_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_gamma_indexonlyscan_state(CustomScan *custom_plan)
{
	GammaIndexOnlyScanState *vstate = 
		MemoryContextAllocZero(CurTransactionContext, sizeof(GammaIndexOnlyScanState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &gamma_indexonlyscan_exec_methods;

	return (Node *) vstate;
}

static void
gamma_indexonlyscan_begin(CustomScanState *node, EState *estate, int eflags)
{
	GammaIndexOnlyScanState *vindexstate = (GammaIndexOnlyScanState *) node;
	IndexOnlyScanState *indexstate;

	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	IndexOnlyScan *plan = (IndexOnlyScan *)linitial(cscan->custom_plans);

	indexstate = vindexstate->indexstate = ExecInitIndexOnlyScan(plan, estate, eflags);

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, indexstate);

	return;
}

static void
gamma_indexonlyscan_rescan(CustomScanState *node)
{
	GammaIndexOnlyScanState *vindexstate = (GammaIndexOnlyScanState *) node;
	ExecReScanIndexOnlyScan(vindexstate->indexstate);

	return;
}

static TupleTableSlot *
gamma_indexonlyscan_exec(CustomScanState *node)
{
	GammaIndexOnlyScanState *vindexstate = (GammaIndexOnlyScanState *) node;
	IndexOnlyScanState *indexstate = vindexstate->indexstate;

	if (indexstate->ioss_NumRuntimeKeys != 0 && !indexstate->ioss_RuntimeKeysReady)
		ExecReScan((PlanState *) indexstate);

	return ExecScan(&indexstate->ss,
			(ExecScanAccessMtd) gamma_indexonlyscan_access_indexnext,
			(ExecScanRecheckMtd) gamma_indexonlyscan_access_indexrecheck);
}

static void
gamma_indexonlyscan_end(CustomScanState *node)
{
	GammaIndexOnlyScanState *vindexstate = (GammaIndexOnlyScanState *) node;
	ExecEndIndexOnlyScan(vindexstate->indexstate);

	return;
}

/*
 * StoreIndexTuple
 *		Fill the slot with data from the index tuple.
 *
 * At some point this might be generally-useful functionality, but
 * right now we don't need it elsewhere.
 */
static void
StoreIndexTuple(TupleTableSlot *slot, IndexTuple itup, TupleDesc itupdesc)
{
	/*
	 * Note: we must use the tupdesc supplied by the AM in index_deform_tuple,
	 * not the slot's tupdesc, in case the latter has different datatypes
	 * (this happens for btree name_ops in particular).  They'd better have
	 * the same number of columns though, as well as being datatype-compatible
	 * which is something we can't so easily check.
	 */
	Assert(slot->tts_tupleDescriptor->natts == itupdesc->natts);

	ExecClearTuple(slot);
	index_deform_tuple(itup, itupdesc, slot->tts_values, slot->tts_isnull);
	ExecStoreVirtualTuple(slot);
}

static TupleTableSlot *
gamma_indexonlyscan_access_indexnext(IndexOnlyScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	ItemPointer tid;
	CIndexFetchCTableData *vscandesc;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexOnlyScan *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->ioss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */
		scandesc = index_beginscan(node->ss.ss_currentRelation,
								   node->ioss_RelationDesc,
								   estate->es_snapshot,
								   node->ioss_NumScanKeys,
								   node->ioss_NumOrderByKeys);

		node->ioss_ScanDesc = scandesc;

		/* Set it up for index-only scan */
		node->ioss_ScanDesc->xs_want_itup = true;
		node->ioss_VMBuffer = InvalidBuffer;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->ioss_ScanKeys, node->ioss_NumScanKeys,
						 node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
	}

	vscandesc = (CIndexFetchCTableData *) scandesc->xs_heapfetch;
	vscandesc->indexonlyscan = true;

	/*
	 * OK, now that we have what we need, fetch the next tuple.
	 */
	while ((tid = index_getnext_tid(scandesc, direction)) != NULL)
	{
		bool		tuple_from_heap = false;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Rats, we have to visit the heap to check visibility.
		 */
		InstrCountTuples2(node, 1);
		if (!index_fetch_heap(scandesc, node->ioss_TableSlot))
			continue;		/* no visible tuple, try next index entry */

		ExecClearTuple(node->ioss_TableSlot);

		/*
		 * Only MVCC snapshots are supported here, so there should be no
		 * need to keep following the HOT chain once a visible entry has
		 * been found.  If we did want to allow that, we'd need to keep
		 * more state to remember not to call index_getnext_tid next time.
		 */
		if (scandesc->xs_heap_continue)
			elog(ERROR, "non-MVCC snapshots are not supported in index-only scans");

		/*
		 * Note: at this point we are holding a pin on the heap page, as
		 * recorded in scandesc->xs_cbuf.  We could release that pin now,
		 * but it's not clear whether it's a win to do so.  The next index
		 * entry might require a visit to the same heap page.
		 */

		tuple_from_heap = true;

		/*
		 * Fill the scan tuple slot with data from the index.  This might be
		 * provided in either HeapTuple or IndexTuple format.  Conceivably an
		 * index AM might fill both fields, in which case we prefer the heap
		 * format, since it's probably a bit cheaper to fill a slot from.
		 */
		if (scandesc->xs_hitup)
		{
			/*
			 * We don't take the trouble to verify that the provided tuple has
			 * exactly the slot's format, but it seems worth doing a quick
			 * check on the number of fields.
			 */
			Assert(slot->tts_tupleDescriptor->natts ==
				   scandesc->xs_hitupdesc->natts);
			ExecForceStoreHeapTuple(scandesc->xs_hitup, slot, false);
		}
		else if (scandesc->xs_itup)
			StoreIndexTuple(slot, scandesc->xs_itup, scandesc->xs_itupdesc);
		else
			elog(ERROR, "no data returned for index-only scan");

		/*
		 * If the index was lossy, we have to recheck the index quals.
		 */
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->recheckqual, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		/*
		 * We don't currently support rechecking ORDER BY distances.  (In
		 * principle, if the index can support retrieval of the originally
		 * indexed value, it should be able to produce an exact distance
		 * calculation too.  So it's not clear that adding code here for
		 * recheck/re-sort would be worth the trouble.  But we should at least
		 * throw an error if someone tries it.)
		 */
		if (scandesc->numberOfOrderBys > 0 && scandesc->xs_recheckorderby)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lossy distance functions are not supported in index-only scans")));

		/*
		 * If we didn't access the heap, then we'll need to take a predicate
		 * lock explicitly, as if we had.  For now we do that at page level.
		 */
		if (!tuple_from_heap)
			PredicateLockPage(scandesc->heapRelation,
							  ItemPointerGetBlockNumber(tid),
							  estate->es_snapshot);

		return slot;
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	return ExecClearTuple(slot);
}

static bool
gamma_indexonlyscan_access_indexrecheck(IndexOnlyScanState *node, TupleTableSlot *slot)
{
	elog(ERROR, "EvalPlanQual recheck is not supported in index-only scans");
	return false;				/* keep compiler quiet */
}
