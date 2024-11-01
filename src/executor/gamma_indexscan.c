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
#include "utils/datum.h"
#include "utils/logtape.h"
#include "utils/rel.h"

#include "nodes/extensible.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

#include "executor/gamma_indexscan.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "storage/ctable_am.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

/* CustomScanMethods */
static Node *create_gamma_indexscan_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void gamma_indexscan_begin(CustomScanState *node,
							 EState *estate, int eflags);
static void gamma_indexscan_rescan(CustomScanState *node);
static TupleTableSlot* gamma_indexscan_exec(CustomScanState *node);
static void gamma_indexscan_end(CustomScanState *node);

static Plan * gamma_plan_indexscan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);
static TupleTableSlot * gamma_indexscan_access_indexnext(IndexScanState *node);
static bool gamma_indexscan_access_indexrecheck(IndexScanState *node,
													TupleTableSlot *slot);

/*
 * GammaIndexScanScanState - state object of vectorized resultregate on executor.
 */
typedef struct GammaIndexScanState
{
	CustomScanState	css;
	IndexScanState *indexstate;
	//ExecProcNodeMtd ori_exec_index_scan_proc;
} GammaIndexScanState;

static CustomPathMethods gamma_indexscan_path_methods = {
	"gamma_indexscan",
	gamma_plan_indexscan,
};

static CustomScanMethods gamma_indexscan_scan_methods = {
	"gamma_indexscan",
	create_gamma_indexscan_state,
};

static CustomExecMethods gamma_indexscan_exec_methods = {
	"gamma_indexscan",
	gamma_indexscan_begin,
	gamma_indexscan_exec,
	gamma_indexscan_end,
	gamma_indexscan_rescan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

void
gamma_indexscan_init(void)
{
	RegisterCustomScanMethods(&gamma_indexscan_scan_methods);
}

const CustomPathMethods*
gamma_indexscan_methods(void)
{
	return &gamma_indexscan_path_methods;
}

bool
gamma_is_indexscan_customscan(CustomScan *cscan)
{
	return ((void *)cscan->methods == (void *)&gamma_indexscan_scan_methods);
}

static Plan *
gamma_plan_indexscan(PlannerInfo *root,
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

	cscan->methods = &gamma_indexscan_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_gamma_indexscan_state(CustomScan *custom_plan)
{
	GammaIndexScanState *vstate = 
		MemoryContextAllocZero(CurTransactionContext, sizeof(GammaIndexScanState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &gamma_indexscan_exec_methods;

	return (Node *) vstate;
}

static void
gamma_indexscan_begin(CustomScanState *node, EState *estate, int eflags)
{
	GammaIndexScanState *vindexstate = (GammaIndexScanState *) node;
	IndexScanState *indexstate;

	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	IndexScan *plan = (IndexScan *)linitial(cscan->custom_plans);

	indexstate = vindexstate->indexstate = ExecInitIndexScan(plan, estate, eflags);
	//vindexstate->ori_exec_index_scan_proc = indexstate->ss.ps.ExecProcNode;
	//indexstate->ss.ps.ExecProcNode = gamma_indexscan_exec;

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, indexstate);

	return;
}

static void
gamma_indexscan_rescan(CustomScanState *node)
{
	GammaIndexScanState *vindexstate = (GammaIndexScanState *) node;
	ExecReScanIndexScan(vindexstate->indexstate);

	return;
}

static TupleTableSlot *
gamma_indexscan_exec(CustomScanState *node)
{
	GammaIndexScanState *vindexstate = (GammaIndexScanState *) node;
	IndexScanState *indexstate = vindexstate->indexstate;

	if (indexstate->iss_NumOrderByKeys > 0)
	{
		Assert(indexstate->ss.ps.ExecProcNode != NULL);
		return indexstate->ss.ps.ExecProcNode((PlanState *)indexstate);
	}

	if (indexstate->iss_NumRuntimeKeys != 0 && !indexstate->iss_RuntimeKeysReady)
		ExecReScan((PlanState *) indexstate);

	return ExecScan(&indexstate->ss,
			(ExecScanAccessMtd) gamma_indexscan_access_indexnext,
			(ExecScanRecheckMtd) gamma_indexscan_access_indexrecheck);
}

static void
gamma_indexscan_end(CustomScanState *node)
{
	GammaIndexScanState *vindexstate = (GammaIndexScanState *) node;
	ExecEndIndexScan(vindexstate->indexstate);

	return;
}

static TupleTableSlot *
gamma_indexscan_access_indexnext(IndexScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	CIndexFetchCTableData *vscandesc;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->iss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index scan is not parallel, or if we're
		 * serially executing an index scan that was planned to be parallel.
		 */
		scandesc = index_beginscan(node->ss.ss_currentRelation,
								   node->iss_RelationDesc,
								   estate->es_snapshot,
								   node->iss_NumScanKeys,
								   node->iss_NumOrderByKeys);

		node->iss_ScanDesc = scandesc;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->iss_NumRuntimeKeys == 0 || node->iss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->iss_ScanKeys, node->iss_NumScanKeys,
						 node->iss_OrderByKeys, node->iss_NumOrderByKeys);
	}

	vscandesc = (CIndexFetchCTableData *) scandesc->xs_heapfetch;
	if (vscandesc->bms_proj == NULL)
	{
		Bitmapset *bms_proj = NULL;
		Plan *plan = node->ss.ps.plan;
		pull_varattnos((Node *)plan->targetlist,
						((Scan *)plan)->scanrelid, &bms_proj);
		pull_varattnos((Node *)plan->qual, ((Scan *)plan)->scanrelid, &bms_proj);

		vscandesc->bms_proj = bms_proj;
	}

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	while (index_getnext_slot(scandesc, direction, slot))
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * If the index was lossy, we have to recheck the index quals using
		 * the fetched tuple.
		 */
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->indexqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		return slot;
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	node->iss_ReachedEnd = true;
	return ExecClearTuple(slot);
}

static bool
gamma_indexscan_access_indexrecheck(IndexScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the indexqual condition? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->indexqualorig, econtext);
}
