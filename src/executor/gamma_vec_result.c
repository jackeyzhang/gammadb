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
#include "executor/nodeResult.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

#include "executor/gamma_vec_result.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

/* CustomScanMethods */
static Node *create_vec_result_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void vec_result_begin(CustomScanState *node,
							 EState *estate, int eflags);
static void vec_result_rescan(CustomScanState *node);
static TupleTableSlot* vec_result_exec(CustomScanState *node);
static void vec_result_end(CustomScanState *node);

static Plan * vec_plan_result(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

/*
 * VecResultScanState - state object of vectorized resultregate on executor.
 */
typedef struct VecResultState
{
	CustomScanState	css;
	ResultState *resultstate;
} VecResultState;

static CustomPathMethods vec_result_path_methods = {
	"gamma_vec_result",
	vec_plan_result,
};

static CustomScanMethods vec_result_scan_methods = {
	"gamma_vec_result",
	create_vec_result_state,
};

static CustomExecMethods vec_result_exec_methods = {
	"gamma_vec_result",
	vec_result_begin,
	vec_result_exec,
	vec_result_end,
	vec_result_rescan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

void
gamma_vec_result_init(void)
{
	RegisterCustomScanMethods(&vec_result_scan_methods);
}

const CustomPathMethods*
gamma_vec_result_path_methods(void)
{
	return &vec_result_path_methods;
}


static Plan *
vec_plan_result(PlannerInfo *root,
		RelOptInfo *rel,
		CustomPath *best_path,
		List *tlist,
		List *clauses,
		List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan *resultplan = NULL;

	Assert(list_length(custom_plans) == 1);
	Assert(tlist != NULL);

	/* The Result node may be eliminated during the create plan phase*/
	resultplan = (Plan *) linitial(custom_plans);
	if (!IsA(resultplan, Result))
	{
		Plan *subplan = NULL;

		Assert(IsA(resultplan, CustomScan));
		
		cscan = (CustomScan *) resultplan;
		Assert(list_length(cscan->custom_plans) == 1);
		subplan = (Plan *) linitial(cscan->custom_plans);

		//cscan->scan.plan.targetlist = 
		//	(List *)gamma_vec_convert_node((Node *)cscan->scan.plan.targetlist);
		cscan->custom_scan_tlist = (List *)copyObject(cscan->scan.plan.targetlist);
		subplan->targetlist = (List *) copyObject(cscan->custom_scan_tlist);
		//subplan->targetlist = (List *) gamma_vec_convert_node((Node *) subplan->targetlist);

		return resultplan;
	}

	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.targetlist = (List *) copyObject(tlist);
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = (List *)copyObject(tlist);

	cscan->custom_plans = custom_plans;//(List *)gamma_vec_convert_plan((Node*)custom_plans);

	cscan->methods = &vec_result_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_vec_result_state(CustomScan *custom_plan)
{
	VecResultState *vstate = 
		MemoryContextAllocZero(CurTransactionContext, sizeof(VecResultState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &vec_result_exec_methods;

	return (Node *) vstate;
}

static void
vec_result_begin(CustomScanState *node, EState *estate, int eflags)
{
	VecResultState *vresultstate = (VecResultState *) node;
	ResultState *resstate;

	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	Result *plan = (Result *)linitial(cscan->custom_plans);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) ||
		   outerPlan(plan) != NULL);

	/*
	 * create state structure
	 */
	resstate = makeNode(ResultState);
	resstate->ps.plan = (Plan *) plan;
	resstate->ps.state = estate;
	resstate->ps.ExecProcNode =  (ExecProcNodeMtd)vec_result_exec;

	resstate->rs_done = false;
	resstate->rs_checkqual = (plan->resconstantqual != NULL);
	Assert(!restate->rs_checkqual);

	ExecAssignExprContext(estate, &resstate->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(resstate) = ExecInitNode(outerPlan(plan), estate, eflags);

	/*
	 * we don't use inner plan
	 */
	Assert(innerPlan(plan) == NULL);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&resstate->ps, &TTSOpsVector);
	ExecAssignProjectionInfo(&resstate->ps, NULL);

	/*
	 * initialize child expressions
	 */
#if 0
	resstate->ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) resstate);
	resstate->resconstantqual =
		ExecInitQual((List *) node->resconstantqual, (PlanState *) resstate);
#endif

	vresultstate->resultstate = resstate;

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, vresultstate->resultstate);

	return;
}


static void
vec_result_rescan(CustomScanState *node)
{
	VecResultState *vresultstate = (VecResultState *) node;
	ExecReScanResult(vresultstate->resultstate);

	return;
}

static TupleTableSlot *
vec_result_exec(CustomScanState *node)
{
	VecResultState *vresultstate = (VecResultState *) node;
	ResultState *resultstate = vresultstate->resultstate;
	TupleTableSlot *outerTupleSlot;
	PlanState  *outerPlan;
	ExprContext *econtext;

	CHECK_FOR_INTERRUPTS();

	econtext = resultstate->ps.ps_ExprContext;

	/*
	 * check constant qualifications like (2 > 1), if not already done
	 */
	Assert(resultstate->rs_checkqual == NULL);
#if 0
	if (node->rs_checkqual)
	{
		bool		qualResult = ExecQual(node->resconstantqual, econtext);

		node->rs_checkqual = false;
		if (!qualResult)
		{
			node->rs_done = true;
			return NULL;
		}
	}
#endif

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * if rs_done is true then it means that we were asked to return a
	 * constant tuple and we already did the last time ExecResult() was
	 * called, OR that we failed the constant qual check. Either way, now we
	 * are through.
	 */
	if (!resultstate->rs_done)
	{
		outerPlan = outerPlanState(resultstate);

		if (outerPlan != NULL)
		{
			/*
			 * retrieve tuples from the outer plan until there are no more.
			 */
			outerTupleSlot = ExecProcNode(outerPlan);

			if (TupIsNull(outerTupleSlot))
				return NULL;

			/*
			 * prepare to compute projection expressions, which will expect to
			 * access the input tuples as varno OUTER.
			 */
			econtext->ecxt_outertuple = outerTupleSlot;

			/* form the result tuple using ExecProject(), and return it */
			if (resultstate->ps.ps_ProjInfo)
			{
				TupleTableSlot *resultSlot;
				//econtext->ecxt_scantuple = slot;

				resultSlot = ExecProject(resultstate->ps.ps_ProjInfo);
				memcpy(((VectorTupleSlot*)resultSlot)->skip,
						((VectorTupleSlot*)outerTupleSlot)->skip,
						sizeof(bool) * VECTOR_SIZE);
				if (VSlotHasNonSkip(((VectorTupleSlot *) outerTupleSlot)))
					VSlotSetNonSkip(((VectorTupleSlot *) resultSlot));

				((VectorTupleSlot *)resultSlot)->dim =
					((VectorTupleSlot *)outerTupleSlot)->dim;

				return resultSlot;
			}
		}
		else
		{
			/*
			 * if we don't have an outer plan, then we are just generating the
			 * results from a constant target list.  Do it only once.
			 */
			resultstate->rs_done = true;
		}

	}

	return NULL;
}

static void
vec_result_end(CustomScanState *node)
{
	VecResultState *vresultstate = (VecResultState *) node;
	ExecEndResult(vresultstate->resultstate);

	return;
}
