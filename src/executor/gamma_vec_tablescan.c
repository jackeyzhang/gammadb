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
#include "storage/bufmgr.h"
#include "utils/rel.h"

#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"

#include "executor/gamma_vec_ctablescan.h"
#include "executor/gamma_vec_tablescan.h"
#include "executor/vec_exec_scan.h"
#include "executor/gamma_expr.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "storage/ctable_am.h"
#include "utils/gamma_cache.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

/* CustomScanMethods */
static Node *create_vec_tablescan_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void vec_tablescan_begin(CustomScanState *node,
								EState *estate, int eflags);
static void vec_tablescan_rescan(CustomScanState *node);
static TupleTableSlot* vec_tablescan_exec(CustomScanState *node);
static void vec_tablescan_end(CustomScanState *node);

/* hook functions */
static SeqScanState* vec_tablescan_execinit(SeqScan *node, EState *estate,
												int eflags);
static TupleTableSlot* vec_tablescan_access_seqnext(ScanState *node);
static bool vec_tablescan_access_recheck(ScanState *node,
										 TupleTableSlot *slot);
static Plan * vec_plan_tablescan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

/*
 * VecTableScanState - state object of vectorscan on executor.
 */
typedef struct VecTableScanState
{
	CustomScanState	css;
	VecSeqScanState	*seqstate;
	TupleTableSlot *backup_css_result_slot;
} VecTableScanState;

static CustomPathMethods vec_tablescan_path_methods = {
	"gamma_vec_tablescan",			/* CustomName */
	vec_plan_tablescan,
};

static CustomScanMethods vec_tablescan_scan_methods = {
	"gamma_vec_tablescan",			/* CustomName */
	create_vec_tablescan_state,		/* CreateCustomScanState */
};

static CustomExecMethods vec_tablescan_exec_methods = {
	"gamma_vec_tablescan",		/* CustomName */
	vec_tablescan_begin,		/* BeginCustomScan */
	vec_tablescan_exec,			/* ExecCustomScan */
	vec_tablescan_end,			/* EndCustomScan */
	vec_tablescan_rescan,		/* ReScanCustomScan */
	NULL,						/* MarkPosCustomScan */
	NULL,						/* RestrPosCustomScan */
	NULL,						/* EstimateDSMCustomScan */
	NULL,						/* InitializeDSMCustomScan */
	NULL,						/* InitializeWorkerCustomScan */
	NULL,						/* ExplainCustomScan */
};

void
gamma_vec_tablescan_init(void)
{
	/* Register a vscan type of custom scan node */
	RegisterCustomScanMethods(&vec_tablescan_scan_methods);
}

const CustomPathMethods*
gamma_vec_tablescan_path_methods(void)
{
	return &vec_tablescan_path_methods;
}

const CustomExecMethods*
gamma_vec_tablescan_exec_methods(void)
{
	return &vec_tablescan_exec_methods;
}

static Plan *
vec_plan_tablescan(PlannerInfo *root,
		RelOptInfo *rel,
		CustomPath *best_path,
		List *tlist,
		List *clauses,
		List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List *scan_tlist = NULL;
	ListCell *lc = NULL;
	
	Assert(list_length(custom_plans) == 1);


	if (tlist == NULL)
	{
		foreach (lc, custom_plans)
		{
			Plan *temp = (Plan *) lfirst(lc);
			scan_tlist = temp->targetlist;
			if (scan_tlist == NULL)
			{
				scan_tlist = temp->targetlist = build_physical_tlist(root, rel);
			}
		}

		tlist = scan_tlist;
	}
	else
	{
		foreach (lc, custom_plans)
		{
			Plan *temp = (Plan *) lfirst(lc);
			scan_tlist = temp->targetlist;
			if (scan_tlist == NULL)
			{
				scan_tlist = temp->targetlist = build_physical_tlist(root, rel);
			}
		}

		//tlist = scan_tlist;
	}

	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.targetlist = (List *) copyObject(tlist);
	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.lefttree = NULL;//linitial(custom_plans);
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = (List *)copyObject(scan_tlist); //TODO:tlist;

	cscan->custom_plans = custom_plans;//(List *)gamma_vec_convert_plan((Node*)custom_plans);

	cscan->methods = &vec_tablescan_scan_methods;

	return &cscan->scan.plan;
}

/*
 * CreateVecTableScanState - A method of CustomScan; that populate a custom
 * object being delivered from CustomScanState type, according to the
 * supplied CustomPath object.
 *
 */
static Node *
create_vec_tablescan_state(CustomScan *custom_plan)
{
	VecTableScanState *vstate = 
		MemoryContextAllocZero(CurTransactionContext,
								sizeof(VecTableScanState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &vec_tablescan_exec_methods;

	return (Node *) vstate;
}

static void
vec_tablescan_begin(CustomScanState *node, EState *estate, int eflags)
{
	VecTableScanState *vstate = (VecTableScanState*)node;
	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	SeqScan	*plan = (SeqScan *)linitial(cscan->custom_plans);
	
	/* custom scan op need en vector TODO */
#if 0
	if (!IsParallelWorker())
	{
		cscan->scan.plan.targetlist =
			(List *) gamma_vec_convert_node((Node *) cscan->scan.plan.targetlist);
	}
#endif

	vstate->seqstate = (VecSeqScanState *) vec_tablescan_execinit(plan,
																	estate,
																	eflags);
	
	vstate->seqstate->scan_over = false;

	/* ExecEndCustomScan need it */
	vstate->backup_css_result_slot = vstate->css.ss.ps.ps_ResultTupleSlot;

	/*
	 * Fix the variables in Seq Scan State to Custom Plan State, which will
	 * be used by the upper level operators
	 */
	if (vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot != NULL)
	{
		TupleDesc result_desc = vstate->css.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
		en_vec_tupledesc(result_desc);
		vstate->css.ss.ps.ps_ResultTupleSlot = NULL;
		VecExecConditionalAssignProjectionInfo(&vstate->css.ss.ps,
				vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
				((Scan *) plan)->scanrelid);

		if (vstate->css.ss.ps.ps_ProjInfo == NULL)
			vstate->css.ss.ps.ps_ResultTupleSlot =
						  vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot;
	}
	else
	{
		if (vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot == NULL)
		{
			vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot =
				vstate->seqstate->sss.ss.ss_ScanTupleSlot;
		}

		if (vstate->css.ss.ps.ps_ProjInfo != NULL)
		{

			TupleDesc result_desc = vstate->css.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
			en_vec_tupledesc(result_desc);
			vstate->css.ss.ps.ps_ResultTupleSlot = NULL;

			VecExecConditionalAssignProjectionInfo(&vstate->css.ss.ps,
					vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
					((Scan *) plan)->scanrelid);
		}
		else
		{
			//node->ss.ps.ps_ResultTupleDesc = vstate->seqstate->sss.ss.ps.ps_ResultTupleDesc;
			node->ss.ps.scanops = vstate->seqstate->sss.ss.ps.scanops;
			node->ss.ps.resultops = vstate->seqstate->sss.ss.ps.resultops;
			vstate->css.ss.ps.ps_ResultTupleSlot =
							vstate->seqstate->sss.ss.ps.ps_ResultTupleSlot;
		}
	}

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, vstate->seqstate);

	return;
}


static SeqScanState *
vec_tablescan_execinit(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;
	int i;
	Relation rel;
	TupleDesc vdesc;
	Expr *qual = NULL;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create vector state structure
	 */
	scanstate = (SeqScanState *) palloc0(sizeof(VecSeqScanState));

	NodeSetTag(scanstate, T_SeqScanState);

	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = (ExecProcNodeMtd)vec_tablescan_exec;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 ((Scan *) node)->scanrelid,
							 eflags);

	rel = scanstate->ss.ss_currentRelation;
	vdesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
	for (i = 0; i < vdesc->natts; i++)
	{
		Form_pg_attribute	attr = &(vdesc->attrs[i]);
		Oid					vtypid = en_vec_type(attr->atttypid);
		if (vtypid != InvalidOid)
			attr->atttypid = vtypid;
		else
			elog(ERROR, "cannot find vectorized type for type %d",
					attr->atttypid);
	}

	ExecInitScanTupleSlot(estate, &scanstate->ss, vdesc, &TTSOpsVector);

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	VecExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	if (((Plan *) node)->qual != NULL && IsA(((Plan *) node)->qual, List))
	{
		FuncExpr *newexpr = makeNode(FuncExpr);
		newexpr->funcid = gamma_get_boolexpr_and_oid();
		newexpr->funcresulttype = en_vec_type(BOOLOID);
		newexpr->funcretset = false;
		newexpr->funcvariadic = true;
		newexpr->funcformat = COERCE_EXPLICIT_CALL; //TODO:
		newexpr->funccollid = InvalidOid;
		newexpr->inputcollid = InvalidOid;
		newexpr->args = ((Plan *) node)->qual;
		newexpr->location = -1;
		qual = (Expr *)newexpr;
		//qual = makeBoolExpr(AND_EXPR, ((Plan *) node)->qual, -1); 
	}
	else
	{
		qual = (Expr *) ((Plan *)node)->qual;
	}

	scanstate->ss.ps.qual = gamma_exec_init_expr(qual, (PlanState *) scanstate);

	return scanstate;
}

static void
vec_tablescan_rescan(CustomScanState *node)
{
	VecTableScanState *vstate = (VecTableScanState*)node;
	ExecReScanSeqScan((SeqScanState *)vstate->seqstate);
	return;
}

static TupleTableSlot *
vec_tablescan_exec(CustomScanState *node)
{
	ExprContext *econtext;
	VecTableScanState *vstate = (VecTableScanState*)node;
	SeqScanState *scanstate = (SeqScanState *) vstate->seqstate;
	Relation rel = scanstate->ss.ss_currentRelation;
	ProjectionInfo *projInfo;
	TupleTableSlot *slot;

	projInfo = node->ss.ps.ps_ProjInfo;
	econtext = node->ss.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	if (rel->rd_tableam == ctable_tableam_routine())
	{
		/* for columnar store, return vector tuples */
		slot = vec_tablescan_execscan((ScanState *)vstate->seqstate,
				vec_ctablescan_access_seqnext,
				vec_ctablescan_access_recheck);
	}
	else
	{
		/* for heap(row) store, return vector tuples */
		slot = vec_tablescan_execscan((ScanState *)vstate->seqstate,
				vec_tablescan_access_seqnext,
				vec_tablescan_access_recheck);
	}

	if (TupIsNull(slot))
	{
		if (projInfo)
			return ExecClearTuple(projInfo->pi_state.resultslot);
		else
			return slot;
	}

	if (projInfo)
	{
		TupleTableSlot *resultSlot;
		econtext->ecxt_scantuple = slot;

		resultSlot = ExecProject(projInfo);
		memcpy(((VectorTupleSlot*)resultSlot)->skip,
				((VectorTupleSlot*)slot)->skip, sizeof(bool) * VECTOR_SIZE);

		((VectorTupleSlot *)resultSlot)->dim = ((VectorTupleSlot *)slot)->dim;

		return resultSlot;
	}

	return slot;
}

static void
vec_tablescan_end(CustomScanState *node)
{
	VecTableScanState *vstate = (VecTableScanState*)node;
	ExecEndSeqScan((SeqScanState *)vstate->seqstate);
	if (vstate->backup_css_result_slot != NULL)
		vstate->css.ss.ps.ps_ResultTupleSlot = vstate->backup_css_result_slot;
}

static TupleTableSlot *
vec_tablescan_access_seqnext(ScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	SeqScanState	*state = (SeqScanState *)node;
	VecSeqScanState *vstate = (VecSeqScanState *)node;
	
	/*
	 * get information from the estate and scan state
	 */
	scandesc = state->ss.ss_currentScanDesc;
	estate = state->ss.ps.state;
	direction = estate->es_direction;
	slot = state->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(state->ss.ss_currentRelation,
								  estate->es_snapshot,
								  0, NULL);
		state->ss.ss_currentScanDesc = scandesc;
	}


	/* return the last batch. */
	if (vstate->scan_over)
	{
		ExecClearTuple(slot);
		return slot;
	}

	vstate->scan_over = tts_vector_slot_fill_tuple(scandesc, direction, slot);

	return slot;
}

static bool
vec_tablescan_access_recheck(ScanState *node, TupleTableSlot *slot)
{
	return true;
}
