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
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/logtape.h"
#include "utils/rel.h"

#include "nodes/extensible.h"
#include "executor/nodeSort.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

#include "executor/gamma_vec_sort.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

/* CustomScanMethods */
static Node *create_vec_sort_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void vec_sort_begin(CustomScanState *node,
		EState *estate, int eflags);
static void vec_sort_rescan(CustomScanState *node);
static TupleTableSlot* vec_sort_exec(CustomScanState *node);
static void vec_sort_end(CustomScanState *node);

static Plan * vec_plan_sort(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

/*
 * VecSortScanState - state object of vectorized sortregate on executor.
 */
typedef struct VecSortState
{
	CustomScanState	css;
	SortState *sortstate;
	TupleTableSlot *rowslot;
} VecSortState;

static CustomPathMethods vec_sort_path_methods = {
	"gamma_vec_sort",
	vec_plan_sort,
};

static CustomScanMethods vec_sort_scan_methods = {
	"gamma_vec_sort",
	create_vec_sort_state,
};

static CustomExecMethods vec_sort_exec_methods = {
	"gamma_vec_sort",
	vec_sort_begin,
	vec_sort_exec,
	vec_sort_end,
	vec_sort_rescan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

void
gamma_vec_sort_init(void)
{
	RegisterCustomScanMethods(&vec_sort_scan_methods);
}

const CustomPathMethods*
gamma_vec_sort_path_methods(void)
{
	return &vec_sort_path_methods;
}


static Plan *
vec_plan_sort(PlannerInfo *root,
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

	cscan->custom_plans = custom_plans;//(List *)gamma_vec_convert_plan((Node*)custom_plans);

	cscan->methods = &vec_sort_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_vec_sort_state(CustomScan *custom_plan)
{
	VecSortState *vstate = 
		MemoryContextAllocZero(CurTransactionContext, sizeof(VecSortState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &vec_sort_exec_methods;

	return (Node *) vstate;
}

static void
vec_sort_begin(CustomScanState *node, EState *estate, int eflags)
{
	VecSortState *vsortstate = (VecSortState *) node;
	SortState *sortstate;
	TupleDesc rowscandesc;
	TupleDesc resultdesc;

	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	Sort *plan = (Sort *)linitial(cscan->custom_plans);

	vsortstate->sortstate = sortstate = ExecInitSort(plan, estate, eflags);

	rowscandesc = vsortstate->sortstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	rowscandesc = CreateTupleDescCopy(rowscandesc);
	de_vec_tupledesc(rowscandesc);
	vsortstate->rowslot = MakeTupleTableSlot(rowscandesc, &TTSOpsMinimalTuple);

	/* change the result slot to vectorized mode */
	resultdesc = vsortstate->sortstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	vsortstate->sortstate->ss.ps.ps_ResultTupleSlot =
								MakeTupleTableSlot(resultdesc, &TTSOpsVector);

	node->ss.ps.resultops = &TTSOpsVector;
	node->ss.ps.ps_ResultTupleDesc = resultdesc;
	node->ss.ps.ps_ResultTupleSlot = vsortstate->sortstate->ss.ps.ps_ResultTupleSlot;

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, vsortstate->sortstate);

	return;
}


static void
vec_sort_rescan(CustomScanState *node)
{
	VecSortState *vsortstate = (VecSortState *) node;
	ExecReScanSort(vsortstate->sortstate);

	return;
}

static TupleTableSlot *
vec_sort_exec(CustomScanState *node)
{
	VecSortState *vsortstate = (VecSortState *) node;
	SortState  *sortstate = vsortstate->sortstate;
	EState	   *estate;
	ScanDirection dir;
	Tuplesortstate *tuplesortstate;
	TupleTableSlot *slot;
	TupleTableSlot *rowslot = vsortstate->rowslot;
	int i;

	CHECK_FOR_INTERRUPTS();

	estate = sortstate->ss.ps.state;
	dir = estate->es_direction;
	tuplesortstate = (Tuplesortstate *) sortstate->tuplesortstate;

	/*
	 * If first time through, read all tuples from outer plan and pass them to
	 * tuplesort.c. Subsequent calls just fetch tuples from tuplesort.
	 */

	if (!sortstate->sort_Done)
	{
		Sort	   *plannode = (Sort *) sortstate->ss.ps.plan;
		PlanState  *outerNode;
		TupleDesc	tupDesc;

		SO1_printf("ExecSort: %s\n",
				   "sorting subplan");

		/*
		 * Want to scan subplan in the forward direction while creating the
		 * sorted data.
		 */
		estate->es_direction = ForwardScanDirection;

		outerNode = outerPlanState(sortstate);
		tupDesc = rowslot->tts_tupleDescriptor;//ExecGetResultType(outerNode);

		tuplesortstate = tuplesort_begin_heap(tupDesc,
											  plannode->numCols,
											  plannode->sortColIdx,
											  plannode->sortOperators,
											  plannode->collations,
											  plannode->nullsFirst,
											  work_mem,
											  NULL,
											  sortstate->randomAccess);
		if (sortstate->bounded)
			tuplesort_set_bound(tuplesortstate, sortstate->bound);
		sortstate->tuplesortstate = (void *) tuplesortstate;

		/*
		 * Scan the subplan and feed all the tuples to tuplesort.
		 */

		for (;;)
		{
			VectorTupleSlot *vslot;
			slot = ExecProcNode(outerNode);

			if (TupIsNull(slot))
				break;

			vslot = (VectorTupleSlot *) slot;

			for (i = 0; i < vslot->dim; i++)
			{
				/* skip invalid tuples */
				if (vslot->skip[i])
					continue;

				ExecClearTuple(rowslot);
				tts_vector_slot_copy_one_row(rowslot, slot, i);
				tuplesort_puttupleslot(tuplesortstate, rowslot);
			}
		}

		/*
		 * Complete the sort.
		 */
		tuplesort_performsort(tuplesortstate);

		/*
		 * restore to user specified direction
		 */
		estate->es_direction = dir;

		/*
		 * finally set the sorted flag to true
		 */
		sortstate->sort_Done = true;
		sortstate->bounded_Done = sortstate->bounded;
		sortstate->bound_Done = sortstate->bound;
		if (sortstate->shared_info && sortstate->am_worker)
		{
			TuplesortInstrumentation *si;

			Assert(IsParallelWorker());
			Assert(ParallelWorkerNumber <= sortstate->shared_info->num_workers);
			si = &sortstate->shared_info->sinstrument[ParallelWorkerNumber];
			tuplesort_get_stats(tuplesortstate, si);
		}
		SO1_printf("ExecSort: %s\n", "sorting done");
	}

	SO1_printf("ExecSort: %s\n",
			   "retrieving tuple from tuplesort");

	/*
	 * Get the first or next tuple from tuplesort. Returns NULL if no more
	 * tuples.  Note that we only rely on slot tuple remaining valid until the
	 * next fetch from the tuplesort.
	 */
	slot = sortstate->ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(slot);

	i = 0;
	while (i < VECTOR_SIZE)
	{
		(void) tuplesort_gettupleslot(tuplesortstate,
									  ScanDirectionIsForward(dir),
									  false, rowslot, NULL);

		if (TupIsNull(rowslot))
			break;

		slot_getallattrs(rowslot);
		tts_vector_slot_fill_vector(slot, rowslot, i);
		i++;
	}

	if (i != 0)
	{
		ExecStoreVirtualTuple(slot);
		memset(((VectorTupleSlot *)slot)->skip, false, sizeof(bool) * i);
		VSlotSetNonSkip(((VectorTupleSlot *)slot));
	}

	return slot;
}

static void
vec_sort_end(CustomScanState *node)
{
	VecSortState *vsortstate = (VecSortState *) node;
	ExecEndSort(vsortstate->sortstate);

	return;
}
