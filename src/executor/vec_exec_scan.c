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

/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "executor/gamma_expr.h"
#include "executor/gamma_vec_tablescan.h"
#include "executor/vec_exec_scan.h"
#include "executor/vector_tuple_slot.h"

static inline TupleTableSlot* vec_tablescan_execscanfetch(ScanState *node,
			  ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);
static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno,
		TupleDesc tupdesc);
static bool vec_exec_qual(ExprState *qual, ExprContext *econtext);

/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
vec_tablescan_execscanfetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;

		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			/*
			 * Return empty slot, as we already performed an EPQ substitution
			 * for this relation.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Return empty slot, as we already returned a tuple */
			return ExecClearTuple(slot);
		}
		else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
		{
			/*
			 * Return replacement tuple provided by the EPQ caller.
			 */

			TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

			Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
		else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/*
			 * Fetch and return replacement tuple using a non-locking rowmark.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
				return NULL;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

static bool
vec_exec_qual(ExprState *qual, ExprContext *econtext)
{
	int row;
	VectorTupleSlot *vslot;
	Datum qual_result;
	vbool *qual_bools;
	bool isnull;

	if (qual == NULL)
		return true;

	Assert(qual->flags & EEO_FLAG_IS_QUAL);
	qual_result = ExecEvalExprSwitchContext(qual, econtext, &isnull);

	/* EEOP_QUAL should never return NULL */
	Assert(!isnull);

	vslot = (VectorTupleSlot *) econtext->ecxt_scantuple;
	qual_bools = (vbool *)DatumGetPointer(qual_result);

	/* using skip array to indicated row which didn't pass the qual */
	if (VDATUM_IS_REF(qual_bools) && VDATUM_REF_ISNULL(qual_bools) != NULL)
		memcpy(vslot->skip,VDATUM_REF_ISNULL(qual_bools),sizeof(bool) * VECTOR_SIZE);
	else if (VDATUM_IS_REF(qual_bools) && VDATUM_REF_ISNULL(qual_bools) != NULL)
	{
		/* do nothing */
		//memset(vslot->skip, false, sizeof(bool) * VECTOR_SIZE);
	}
	else
		memcpy(vslot->skip,VDATUM_ARR_ISNULL(qual_bools),sizeof(bool) * VECTOR_SIZE);


	if (VDATUM_IS_REF(qual_bools))
	{
		for (row = 0; row < VECTOR_SIZE; row++)
		{
			if(!DatumGetBool(qual_bools->ref_values[row]))
			{
				vslot->skip[row] = true;
			}
		}
	}
	else
	{
		for (row = 0; row < VECTOR_SIZE; row++)
		{
			if(!DatumGetBool(qual_bools->values[row]))
			{
				vslot->skip[row] = true;
			}
		}
	}

	for (row = 0; row < VECTOR_SIZE; row++)
	{
		if (vslot->skip[row])
		{
			VSlotClearNonSkip(vslot);
			break;
		}
	}

	for (row = 0; row < VECTOR_SIZE; row++)
	{
		if (!vslot->skip[row])
		{
			return true;
		}
	}

	return false;
}

TupleTableSlot *
vec_tablescan_execscan(ScanState *node,
		 ExecScanAccessMtd accessMtd,	/* function returning a tuple */
		 ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	ExprState  *qual;
	ProjectionInfo *projInfo;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;

	/* interrupt checks are in ExecScanFetch */

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo)
	{
		ResetExprContext(econtext);
		return vec_tablescan_execscanfetch(node, accessMtd, recheckMtd);
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.  Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;

		slot = vec_tablescan_execscanfetch(node, accessMtd, recheckMtd);

		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		if (qual == NULL || vec_exec_qual(qual, econtext))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				TupleTableSlot *resultSlot = ExecProject(projInfo);
				memcpy(((VectorTupleSlot*)resultSlot)->skip,
						((VectorTupleSlot*)slot)->skip, sizeof(bool) * VECTOR_SIZE);

			if (VSlotHasNonSkip(((VectorTupleSlot *) slot)))
					VSlotSetNonSkip(((VectorTupleSlot *) resultSlot));

				((VectorTupleSlot *)resultSlot)->dim = ((VectorTupleSlot *)slot)->dim;

				return resultSlot;
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, VECTOR_SIZE);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
void
VecExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	VecExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
}

/* ----------------
 *		ExecConditionalAssignProjectionInfo
 *
 * as ExecAssignProjectionInfo, but store NULL rather than building projection
 * info if no projection is required
 * ----------------
 */
void
VecExecConditionalAssignProjectionInfo(PlanState *planstate,
		TupleDesc inputDesc,
									Index varno)
{
	if (tlist_matches_tupdesc(planstate,
							  planstate->plan->targetlist,
							  varno,
							  inputDesc))
	{
		planstate->ps_ProjInfo = NULL;
		planstate->resultopsset = planstate->scanopsset;
		planstate->resultopsfixed = planstate->scanopsfixed;
		planstate->resultops = planstate->scanops;
	}
	else
	{
		if (!planstate->ps_ResultTupleSlot)
		{
			ExecInitResultSlot(planstate, &TTSOpsVector);
			planstate->resultops = &TTSOpsVector;
			planstate->resultopsfixed = true;
			planstate->resultopsset = true;
		}

		planstate->ps_ProjInfo =
			gamma_exec_build_proj_info(planstate->plan->targetlist,
					planstate->ps_ExprContext,
					planstate->ps_ResultTupleSlot,
					planstate,
					inputDesc);
	}
}

static bool
tlist_matches_tupdesc(PlanState *ps, List *tlist, Index varno, TupleDesc tupdesc)
{
	int			numattrs = tupdesc->natts;
	int			attrno;
	ListCell   *tlist_item = list_head(tlist);

	/* Check the tlist attributes */
	for (attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(tupdesc, attrno - 1);
		Var		   *var;

		if (tlist_item == NULL)
			return false;		/* tlist too short */
		var = (Var *) ((TargetEntry *) lfirst(tlist_item))->expr;
		if (!var || !IsA(var, Var))
			return false;		/* tlist item not a Var */
		/* if these Asserts fail, planner messed up */
		Assert(var->varno == varno);
		Assert(var->varlevelsup == 0);
		if (var->varattno != attrno)
			return false;		/* out of order */
		if (att_tup->attisdropped)
			return false;		/* table contains dropped columns */
		if (att_tup->atthasmissing)
			return false;		/* table contains cols with missing values */

		/*
		 * Note: usually the Var's type should match the tupdesc exactly, but
		 * in situations involving unions of columns that have different
		 * typmods, the Var may have come from above the union and hence have
		 * typmod -1.  This is a legitimate situation since the Var still
		 * describes the column, just not as exactly as the tupdesc does. We
		 * could change the planner to prevent it, but it'd then insert
		 * projection steps just to convert from specific typmod to typmod -1,
		 * which is pretty silly.
		 */
		if (var->vartype != att_tup->atttypid ||
			(var->vartypmod != att_tup->atttypmod &&
			 var->vartypmod != -1))
			return false;		/* type mismatch */

		tlist_item = lnext(tlist, tlist_item);
	}

	if (tlist_item)
		return false;			/* tlist too long */

	return true;
}
