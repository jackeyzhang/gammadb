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

#include "fmgr.h"
#include "optimizer/planner.h"
#include "nodes/extensible.h"
#include "executor/nodeCustom.h"

#include "executor/gamma_devectorize.h"
#include "executor/vector_tuple_slot.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"


/*
 * VecDevectorState - state object of vectorscan on executor.
 */
typedef struct VecDevectorState
{
	CustomScanState	css;

	TupleTableSlot *ps_ResultVTupleSlot; /* slot for my result tuples */
	int				iter;

	/* Attributes for vectorization */
} VecDevectorState;

static Node *create_vec_devector_state(CustomScan *custom_plan);
static TupleTableSlot *vec_devector_fetch_row(VecDevectorState *ubs);
static bool vec_devector_next(VecDevectorState *ubs);
static void vec_devector_rescan(CustomScanState *node);

/* CustomScanExecMethods */
static void vec_devector_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *vec_devector_exec(CustomScanState *node);
static void vec_devector_end(CustomScanState *node);

static Plan * vec_plan_devector(PlannerInfo *root, RelOptInfo *rel,
								CustomPath *best_path, List *tlist,
								List *clauses, List *custom_plans);

static CustomPathMethods vec_devector_path_methods = {
	"gamma_vec_devector",			/* CustomName */
	vec_plan_devector,
};

static CustomScanMethods vec_devector_scan_methods = {
	"gamma_vec_devector",			/* CustomName */
	create_vec_devector_state,	/* CreateCustomScanState */
};


static CustomExecMethods vec_devector_exec_methods = {
	"gamma_vec_devector",	/* CustomName */
	vec_devector_begin,		/* BeginCustomScan */
	vec_devector_exec,		/* ExecCustomScan */
	vec_devector_end,		/* EndCustomScan */
	vec_devector_rescan,	/* ReScanCustomScan */
	NULL,					/* MarkPosCustomScan */
	NULL,					/* RestrPosCustomScan */
	NULL,					/* EstimateDSMCustomScan */
	NULL,					/* InitializeDSMCustomScan */
	NULL,					/* InitializeWorkerCustomScan */
	NULL,					/* ExplainCustomScan */
};

void
gamma_vec_devector_init(void)
{
	RegisterCustomScanMethods(&vec_devector_scan_methods);
}

const CustomPathMethods*
gamma_vec_devector_path_methods(void)
{
	return &vec_devector_path_methods;
}

static Plan *
vec_plan_devector(PlannerInfo *root,
		RelOptInfo *rel,
		CustomPath *best_path,
		List *tlist,
		List *clauses,
		List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	
	Assert(list_length(custom_plans) == 1);
	
	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.targetlist = (List *) copyObject(tlist);
	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.lefttree = linitial(custom_plans);
	cscan->scan.plan.righttree = NULL;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = (List *) copyObject(tlist);//TODO:tlist;

	cscan->methods = &vec_devector_scan_methods;

	return &cscan->scan.plan;
}

Plan *
gamma_add_devector(CustomScan *cscan, Plan *subplan)
{
	CustomScan *devec_scan = makeNode(CustomScan);

	Assert(list_length(custom_plans) == 1);

	devec_scan->scan.plan.parallel_aware = subplan->parallel_aware;
	devec_scan->scan.plan.targetlist =
		(List *) copyObject(((Plan *)cscan)->targetlist);
	devec_scan->scan.plan.qual = NIL;
	devec_scan->scan.plan.lefttree = (Plan *) cscan;
	devec_scan->scan.plan.righttree = NULL;
	devec_scan->scan.scanrelid = 0;
	//TODO:tlist
	devec_scan->custom_scan_tlist = (List *) copyObject(subplan->targetlist);

	devec_scan->methods = &vec_devector_scan_methods;

	return (Plan *) devec_scan;
}

static void
vec_devector_begin(CustomScanState *node, EState *estate, int eflags)
{
	VecDevectorState *vcs = (VecDevectorState*) node;
	CustomScan	 *cscan = (CustomScan *) node->ss.ps.plan;
	TupleDesc   tupdesc;
	TupleDesc vtupdesc;

	outerPlanState(vcs) = ExecInitNode(outerPlan(cscan), estate, eflags);

	/* Convert Vtype in tupdesc to Ntype in unbatch Node */
	tupdesc = outerPlanState(vcs)->ps_ResultTupleDesc;
	vtupdesc = CreateTupleDescCopy(tupdesc);
	tupdesc = CreateTupleDescCopy(tupdesc);

	vcs->ps_ResultVTupleSlot = ExecInitExtraTupleSlot(estate, vtupdesc,
													  &TTSOpsVector);
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = &(tupdesc->attrs[i]);
		Oid typid = de_vec_type(attr->atttypid);
		if (typid != InvalidOid)
			attr->atttypid = typid;
	}

	PinTupleDesc(tupdesc);
	ReleaseTupleDesc(node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor);
	node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor = tupdesc;
	node->ss.ps.ps_ResultTupleDesc = tupdesc;

}

static TupleTableSlot*
vec_devector_fetch_row(VecDevectorState *ubs)
{
	VectorTupleSlot	*vslot;
	TupleTableSlot	   *slot;
	TupleTableSlot		*baseslot;
	int					iter;
	int					natts;
	int					i;

	
	slot = ubs->css.ss.ps.ps_ResultTupleSlot;
	vslot = (VectorTupleSlot *)ubs->ps_ResultVTupleSlot;
	baseslot = (TupleTableSlot *)vslot;
	
	iter = ubs->iter;

	while(iter < vslot->dim && vslot->skip[iter])
		iter++;
	
	/* we have checked that natts is greater than zero */
	if (iter == vslot->dim)
		return NULL;

	ExecClearTuple(slot);
	natts = slot->tts_tupleDescriptor->natts;
	for(i = 0; i < natts; i++)
	{
		Datum value = baseslot->tts_values[i];

		if (VDATUM_ISNULL((vdatum *)value, iter))
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
		else
		{
			slot->tts_values[i] = VDATUM_DATUM((vdatum*)value, iter);
			slot->tts_isnull[i] = false;
		}
	}

	ubs->iter = ++iter;
	return ExecStoreVirtualTuple(slot);
}

/*
 *
 */
static TupleTableSlot *
vec_devector_exec(CustomScanState *node)
{
	VecDevectorState		*ubs;
	TupleTableSlot	   *slot;

	ubs = (VecDevectorState*) node;
	/* find a non skip tuple and return to client */
	while(true)
	{
		/* 
		 * iter = 0 indicate we finish unbatching the vector slot
		 * and need to read next vector slot
		 */
		slot = vec_devector_fetch_row(ubs);
		if(slot)
			break;
		
		/* finish current batch, read next batch */
		if (!vec_devector_next(ubs))
			return NULL;
	}

	return slot;
}

static bool
vec_devector_next(VecDevectorState *ubs)
{
	TupleTableSlot		*slot;

	slot = ExecProcNode(ubs->css.ss.ps.lefttree);
	if(TupIsNull(slot))
		return false;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	ubs->ps_ResultVTupleSlot = slot;
	ubs->iter = 0;
	return true;
}
/*
 *
 */
static void
vec_devector_end(CustomScanState *node)
{
	PlanState  *outerPlan;
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}

static void
vec_devector_rescan(CustomScanState *node)
{
	PlanState  *outerPlan;
	VecDevectorState		*ubs;

	ubs = (VecDevectorState*) node;

	ExecClearTuple(ubs->ps_ResultVTupleSlot);

	outerPlan = outerPlanState(node);
	ExecReScan(outerPlan);
	return;
}

static Node *
create_vec_devector_state(CustomScan *custom_plan)
{
	VecDevectorState *vss = palloc0(sizeof(VecDevectorState));

	NodeSetTag(vss, T_CustomScanState);
	vss->css.methods = &vec_devector_exec_methods;

	return (Node *) &vss->css;
}


