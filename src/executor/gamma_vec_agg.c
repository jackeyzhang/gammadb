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
#include "executor/nodeAgg.h"
#include "executor/nodeCustom.h"
#include "optimizer/plancat.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#include "executor/gamma_vec_agg.h"
#include "executor/gamma_vec_exec_grouping.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"
#include "utils/vdatum/vvarlena.h"
#include "utils/vdatum/vnumeric.h"

#if PG_VERSION_NUM >= 170000
#include "../src/postgres/executor/nodeAgg_17.c"
#elif PG_VERSION_NUM >= 160000
#include "../src/postgres/executor/nodeAgg_16.c"
#else
#include "../src/postgres/executor/nodeAgg_15.c"
#endif

/*
 * VecAggScanState - state object of vectorized aggregate on executor.
 */
typedef struct VecAggState
{
	CustomScanState	css;
	AggState *aggstate;

	/* for sort agg */
	TupleTableSlot *grp_firstSlot;
	int first_row;
	int cur_row;

	/* collect rows for one group */
	int grouping_one_idx;
	short grouping_one[VECTOR_SIZE];
	FmgrInfo *eqfunctions;

	/* for agg(distinct) in row mode */
	TupleDesc *sortdesc;
	TupleTableSlot **sortslot;

	/* slot for row mode */
	TupleTableSlot *outer_tuple_slot;

	int entries_dim;
	VecTupleHashEntry entries[VECTOR_SIZE];

	int spill_dim;
	short spill_indexarr[VECTOR_SIZE];
} VecAggState;

/* CustomScanMethods */
static Node *create_vec_agg_state(CustomScan *custom_plan);

/* CustomScanExecMethods */
static void vec_agg_begin(CustomScanState *node,
		EState *estate, int eflags);
static void vec_agg_rescan(CustomScanState *node);
static TupleTableSlot* vec_agg_exec(CustomScanState *node);
static void vec_agg_end(CustomScanState *node);


static TupleTableSlot * vec_agg_exec_proc(PlanState *pstate);
static TupleTableSlot * vec_agg_retrieve_direct(PlanState *state);
static void vec_agg_fill_hash_table(VecAggState *vaggstate);
static TupleTableSlot *vec_agg_retrieve_hash_table(VecAggState *vaggstate);
static TupleTableSlot *vec_agg_retrieve_hash_table_in_memory(VecAggState *vaggstate);

static Plan * vec_plan_agg(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

static inline void gamma_vec_hashed_aggreggates_set(VecAggState *vaggstate,
															int setno);
static void gamma_vec_lookup_hash_entries(VecAggState *vaggstate);
static void gamma_vec_plain_advance_aggregates(VecAggState *vaggstate);
static void gamma_vec_hashed_advance_aggregates(VecAggState *vaggstate);
static void gamma_vec_reset_entry_batch(VecAggState *aggstate,
												VecTupleHashEntry entry, int32 row);
static void gamma_vec_initialize_hashentry(VecAggState *vaggstate,
		VecTupleHashEntry entry, TupleTableSlot *slot, int32 row);
static void gamma_vec_calc_hash_value(VecAggState *vaggstate, int setno, int32 *hashkeys);

static void vec_build_sort_grouping_match(AggState *aggstate);
static void gamma_init_distinct_sort_cols(VecAggState *vaggstate);

static void gamma_vec_reset_phase(AggState *aggstate);
static inline void gamma_vec_vslot_set_rows(TupleTableSlot *slot,
												short *indexarr);

static void vec_build_hash_tables(AggState *aggstate);
static void vec_build_hash_table(AggState *aggstate, int setno, long nbuckets);

static CustomPathMethods vec_agg_path_methods = {
	"gamma_vec_agg",
	vec_plan_agg,
};

static CustomScanMethods vec_agg_scan_methods = {
	"gamma_vec_agg",
	create_vec_agg_state,
};

static CustomExecMethods vec_agg_exec_methods = {
	"gamma_vec_agg",
	vec_agg_begin,
	vec_agg_exec,
	vec_agg_end,
	vec_agg_rescan,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

void
gamma_vec_agg_init(void)
{
	RegisterCustomScanMethods(&vec_agg_scan_methods);
}

const CustomPathMethods*
gamma_vec_agg_path_methods(void)
{
	return &vec_agg_path_methods;
}

static Plan *
vec_plan_agg(PlannerInfo *root,
		RelOptInfo *rel,
		CustomPath *best_path,
		List *tlist,
		List *clauses,
		List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List *scan_tlist = NULL;
	ListCell *lc = NULL;
	Plan *temp;

	Assert(list_length(custom_plans) == 1);
	Assert(tlist != NULL);

	foreach (lc, custom_plans)
	{
		temp = (Plan *) lfirst(lc);
		scan_tlist = temp->targetlist;
		break;
	}


	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.targetlist = (List *) copyObject(tlist);
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = (List *)copyObject(scan_tlist);

	cscan->custom_plans = custom_plans;//(List *)gamma_vec_convert_plan((Node*)custom_plans);

	cscan->methods = &vec_agg_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_vec_agg_state(CustomScan *custom_plan)
{
	VecAggState *vstate = 
		MemoryContextAllocZero(CurTransactionContext, sizeof(VecAggState));

	/* Set tag and executor callbacks */
	NodeSetTag(vstate, T_CustomScanState);
	vstate->css.methods = &vec_agg_exec_methods;

	return (Node *) vstate;
}

static void
vec_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	VecAggState *vaggstate = (VecAggState *) node;
	AggState *aggstate;
	TupleDesc scandesc;
	TupleDesc rowscandesc;

	CustomScan *cscan = (CustomScan *)node->ss.ps.plan;
	Agg *plan = (Agg *)linitial(cscan->custom_plans);

	vaggstate->aggstate = aggstate = ExecInitAgg(plan, estate, eflags);

	scandesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;

	vaggstate->grp_firstSlot = MakeTupleTableSlot(scandesc, &TTSOpsVector);

	if (aggstate->hash_spill_wslot)
	{
		de_vec_tupledesc(
				CreateTupleDescCopy(aggstate->hash_spill_wslot->tts_tupleDescriptor));
	}

	if (aggstate->hash_spill_rslot)
	{
		de_vec_tupledesc(
				CreateTupleDescCopy(aggstate->hash_spill_rslot->tts_tupleDescriptor));
	}

	rowscandesc = CreateTupleDescCopy(scandesc);
	de_vec_tupledesc(rowscandesc);

	vaggstate->outer_tuple_slot = MakeTupleTableSlot(rowscandesc, &TTSOpsVirtual);

	//if (aggstate->aggstrategy == AGG_PLAIN ||
	//	aggstate->aggstrategy == AGG_MIXED ||
	//	aggstate->aggstrategy == AGG_HASHED)
	
	/* for vec grouping */
	{
		clear_nonvec_hash_tables(aggstate);
		vec_build_hash_tables(aggstate);
		VecExecBuildAggTrans(aggstate, true);
		gamma_vec_reset_phase(aggstate);
		gamma_init_distinct_sort_cols(vaggstate);
	}

	if (aggstate->ss.ps.ps_ResultTupleDesc != NULL)
		de_vec_tupledesc(aggstate->ss.ps.ps_ResultTupleDesc);

	/* for sort grouping */
	vec_build_sort_grouping_match(aggstate);
	//vaggstate->eqfunctions = vec_exec_tuples_match_prepare();
	

	/* set child planstate */
	node->custom_ps = lappend(node->custom_ps, vaggstate->aggstate);

	return;
}


static void
vec_agg_rescan(CustomScanState *node)
{
	VecAggState *vaggstate = (VecAggState *) node;
	ExecReScanAgg(vaggstate->aggstate);

	return;
}

static TupleTableSlot *
vec_agg_exec(CustomScanState *node)
{
	VecAggState *vaggstate = (VecAggState *) node;
	return vec_agg_exec_proc((PlanState *) vaggstate);
}

static void
vec_agg_end(CustomScanState *node)
{
	VecAggState *vaggstate = (VecAggState *) node;
	ExecEndAgg(vaggstate->aggstate);

	return;
}

static TupleTableSlot *
vec_agg_exec_proc(PlanState *pstate)
{
	VecAggState *vaggstate = (VecAggState *) pstate;
	AggState   *node = vaggstate->aggstate;
	TupleTableSlot *result = NULL;

	CHECK_FOR_INTERRUPTS();

	if (!node->agg_done)
	{
		/* Dispatch based on strategy */
		switch (node->phase->aggstrategy)
		{
			case AGG_HASHED:
				if (!node->table_filled)
					vec_agg_fill_hash_table(vaggstate);
				/* FALLTHROUGH */
			case AGG_MIXED:
				result = vec_agg_retrieve_hash_table(vaggstate);
				break;
			case AGG_PLAIN:
			case AGG_SORTED:
				result = vec_agg_retrieve_direct(pstate);
				break;
		}

		if (!TupIsNull(result))
			return result;
	}

	return NULL;
}

static void
gamma_init_distinct_sort_cols(VecAggState *vaggstate)
{
	int i;
	AggState *aggstate = vaggstate->aggstate;

	vaggstate->sortdesc = (TupleDesc *) palloc(aggstate->numtrans * sizeof(TupleDesc));
	vaggstate->sortslot =
		(TupleTableSlot **) palloc(aggstate->numtrans * sizeof(TupleTableSlot *));

	for(i = 0; i < aggstate->numtrans; i++)
	{
		//Aggref	   *aggref = lfirst(l);
		AggStatePerTrans pertrans = &aggstate->pertrans[i];
		if (pertrans->sortdesc == NULL || pertrans->sortslot == NULL)
			continue;

		vaggstate->sortdesc[i] = de_vec_tupledesc(CreateTupleDescCopy(pertrans->sortdesc));
		vaggstate->sortslot[i] = MakeTupleTableSlot(vaggstate->sortdesc[i],
													&TTSOpsMinimalTuple);
	}

	return;
}

static TupleTableSlot *
vec_fetch_input_tuple(VecAggState *vaggstate)
{
	int i;
	TupleTableSlot *slot;
	AggState *aggstate = vaggstate->aggstate;
	TupleTableSlot *rowslot = aggstate->sort_slot;

	if (aggstate->sort_in)
	{
		slot = vaggstate->grp_firstSlot;

		ExecClearTuple(slot);

		/* make sure we check for interrupts in either path through here */
		CHECK_FOR_INTERRUPTS();
		i = 0;
		while (i < VECTOR_SIZE)
		{
			(void) tuplesort_gettupleslot(aggstate->sort_in,
					true, false, rowslot, NULL);

			if (TupIsNull(rowslot))
				break;

			slot_getallattrs(rowslot);
			tts_vector_slot_fill_vector(slot, rowslot, i);
			i++;

			if (!TupIsNull(slot) && aggstate->sort_out)
			{
				tuplesort_puttupleslot(aggstate->sort_out, rowslot);
				ExecClearTuple(rowslot);
			}
		}

		if (i != 0)
			ExecStoreVirtualTuple(slot);
	}
	else
	{
		VectorTupleSlot *vslot;
		slot = ExecProcNode(outerPlanState(aggstate));

		/* vector to row */
		if (!TupIsNull(slot) && aggstate->sort_out)
		{
			vslot = (VectorTupleSlot *) slot;
			for (i = 0; i < vslot->dim; i++)
			{
				ExecClearTuple(rowslot);
				tts_vector_slot_copy_one_row(rowslot, slot, i);
				tuplesort_puttupleslot(aggstate->sort_out, rowslot);
			}
		}
	}

	return slot;
}

static void
gamma_hashagg_recompile_expressions(AggState *aggstate, bool minslot, bool nullcheck)
{
	AggStatePerPhase phase;
	int			i = minslot ? 1 : 0;
	int			j = nullcheck ? 1 : 0;

	Assert(aggstate->aggstrategy == AGG_HASHED ||
		   aggstate->aggstrategy == AGG_MIXED);

	if (aggstate->aggstrategy == AGG_HASHED)
		phase = &aggstate->phases[0];
	else						/* AGG_MIXED */
		phase = &aggstate->phases[1];

	if (phase->evaltrans_cache[0][0] == NULL)
	{
		const TupleTableSlotOps *outerops = aggstate->ss.ps.outerops;
		bool		outerfixed = aggstate->ss.ps.outeropsfixed;
		bool		dohash = true;
		bool		dosort = false;

		/*
		 * If minslot is true, that means we are processing a spilled batch
		 * (inside agg_refill_hash_table()), and we must not advance the
		 * sorted grouping sets.
		 */
		if (aggstate->aggstrategy == AGG_MIXED && !minslot)
			dosort = true;

		/* temporarily change the outerops while compiling the expression */
		if (minslot)
		{
			aggstate->ss.ps.outerops = &TTSOpsMinimalTuple;
			aggstate->ss.ps.outeropsfixed = true;
		}

		phase->evaltrans_cache[0][0] = VecExecBuildAggTransPerPhase(aggstate, phase,
														 dosort, dohash,
														 nullcheck);

		/* change back */
		aggstate->ss.ps.outerops = outerops;
		aggstate->ss.ps.outeropsfixed = outerfixed;
	}

	phase->evaltrans = phase->evaltrans_cache[0][0];
}


static void
gamma_hash_agg_enter_spill_mode(AggState *aggstate)
{
	aggstate->hash_spill_mode = true;
	gamma_hashagg_recompile_expressions(aggstate, aggstate->table_filled, true);

	if (!aggstate->hash_ever_spilled)
	{
		Assert(aggstate->hash_tapeset == NULL);
		Assert(aggstate->hash_spills == NULL);

		aggstate->hash_ever_spilled = true;

		aggstate->hash_tapeset = LogicalTapeSetCreate(true, NULL, -1);

		aggstate->hash_spills = palloc(sizeof(HashAggSpill) * aggstate->num_hashes);

		for (int setno = 0; setno < aggstate->num_hashes; setno++)
		{
			AggStatePerHash perhash = &aggstate->perhash[setno];
			HashAggSpill *spill = &aggstate->hash_spills[setno];

			hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
							   perhash->aggnode->numGroups,
							   aggstate->hashentrysize);
		}
	}
}


static void
gamma_hash_agg_check_limits(AggState *aggstate)
{
	uint64		ngroups = aggstate->hash_ngroups_current;
	Size		meta_mem = MemoryContextMemAllocated(aggstate->hash_metacxt,
													 true);
	Size		hashkey_mem = MemoryContextMemAllocated(aggstate->hashcontext->ecxt_per_tuple_memory,
														true);

	/*
	 * Don't spill unless there's at least one group in the hash table so we
	 * can be sure to make progress even in edge cases.
	 */
	if (aggstate->hash_ngroups_current > 0 &&
		(meta_mem + hashkey_mem > aggstate->hash_mem_limit ||
		 ngroups > aggstate->hash_ngroups_limit))
	{
		gamma_hash_agg_enter_spill_mode(aggstate);
	}
}

static void
gamma_initialize_hash_entry(AggState *aggstate, TupleHashTable hashtable,
					  TupleHashEntry entry)
{
	AggStatePerGroup pergroup;
	int			transno;

	aggstate->hash_ngroups_current++;
	gamma_hash_agg_check_limits(aggstate);

	/* no need to allocate or initialize per-group state */
	if (aggstate->numtrans == 0)
		return;

	pergroup = (AggStatePerGroup)
		MemoryContextAlloc(hashtable->tablecxt,
						   sizeof(AggStatePerGroupData) * aggstate->numtrans);

	entry->additional = pergroup;

	/*
	 * Initialize aggregates for new tuple group, lookup_hash_entries()
	 * already has selected the relevant grouping set.
	 */
	for (transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		AggStatePerGroup pergroupstate = &pergroup[transno];

		initialize_aggregate(aggstate, pertrans, pergroupstate);
	}
}


static TupleTableSlot *
vec_agg_retrieve_direct(PlanState *state)
{
	VecAggState *vaggstate = (VecAggState *) state;
	AggState *aggstate = vaggstate->aggstate;
	Agg		   *node = aggstate->phase->aggnode;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	AggStatePerAgg peragg;
	AggStatePerGroup *pergroups;
	TupleTableSlot *outerslot;
	//VectorTupleSlot *vfirstSlot;
	TupleTableSlot *result;
	bool		hasGroupingSets = aggstate->phase->numsets > 0;
	int			numGroupingSets = Max(aggstate->phase->numsets, 1);
	int			currentSet;
	int			nextSetSize;
	int			numReset;
	int			i;

	/*
	 * get state info from node
	 *
	 * econtext is the per-output-tuple expression context
	 *
	 * tmpcontext is the per-input-tuple expression context
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	tmpcontext = aggstate->tmpcontext;

	peragg = aggstate->peragg;
	pergroups = aggstate->pergroups;
	//firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one matching
	 * aggstate->ss.ps.qual
	 *
	 * For grouping sets, we have the invariant that aggstate->projected_set
	 * is either -1 (initial call) or the index (starting from 0) in
	 * gset_lengths for the group we just completed (either by projecting a
	 * row or by discarding it in the qual).
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Clear the per-output-tuple context for each group, as well as
		 * aggcontext (which contains any pass-by-ref transvalues of the old
		 * group).  Some aggregate functions store working state in child
		 * contexts; those now get reset automatically without us needing to
		 * do anything special.
		 *
		 * We use ReScanExprContext not just ResetExprContext because we want
		 * any registered shutdown callbacks to be called.  That allows
		 * aggregate functions to ensure they've cleaned up any non-memory
		 * resources.
		 */
		ReScanExprContext(econtext);

		/*
		 * Determine how many grouping sets need to be reset at this boundary.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < numGroupingSets)
			numReset = aggstate->projected_set + 1;
		else
			numReset = numGroupingSets;

		/*
		 * numReset can change on a phase boundary, but that's OK; we want to
		 * reset the contexts used in _this_ phase, and later, after possibly
		 * changing phase, initialize the right number of aggregates for the
		 * _new_ phase.
		 */

		for (i = 0; i < numReset; i++)
		{
			ReScanExprContext(aggstate->aggcontexts[i]);
		}

		/*
		 * Check if input is complete and there are no more groups to project
		 * in this phase; move to next phase or mark as done.
		 */
		if (aggstate->input_done == true &&
			aggstate->projected_set >= (numGroupingSets - 1))
		{
			if (aggstate->current_phase < aggstate->numphases - 1)
			{
				initialize_phase(aggstate, aggstate->current_phase + 1);
				aggstate->input_done = false;
				aggstate->projected_set = -1;
				numGroupingSets = Max(aggstate->phase->numsets, 1);
				node = aggstate->phase->aggnode;
				numReset = numGroupingSets;
				ExecClearTuple(vaggstate->grp_firstSlot);
			}
			else if (aggstate->aggstrategy == AGG_MIXED)
			{
				/*
				 * Mixed mode; we've output all the grouped stuff and have
				 * full hashtables, so switch to outputting those.
				 */
				initialize_phase(aggstate, 0);
				aggstate->table_filled = true;
				VecResetTupleHashIterator(aggstate->perhash[0].hashtable,
									   (vec_tuplehash_iterator *)&aggstate->perhash[0].hashiter);
				select_current_set(aggstate, 0, true);
				return vec_agg_retrieve_hash_table(vaggstate);
			}
			else
			{
				aggstate->agg_done = true;
				break;
			}
		}

		/*
		 * Get the number of columns in the next grouping set after the last
		 * projected one (if any). This is the number of columns to compare to
		 * see if we reached the boundary of that set too.
		 */
		if (aggstate->projected_set >= 0 &&
			aggstate->projected_set < (numGroupingSets - 1))
			nextSetSize = aggstate->phase->gset_lengths[aggstate->projected_set + 1];
		else
			nextSetSize = 0;

		/*----------
		 * If a subgroup for the current grouping set is present, project it.
		 *
		 * We have a new group if:
		 *	- we're out of input but haven't projected all grouping sets
		 *	  (checked above)
		 * OR
		 *	  - we already projected a row that wasn't from the last grouping
		 *		set
		 *	  AND
		 *	  - the next grouping set has at least one grouping column (since
		 *		empty grouping sets project only once input is exhausted)
		 *	  AND
		 *	  - the previous and pending rows differ on the grouping columns
		 *		of the next grouping set
		 *----------
		 */
		tmpcontext->ecxt_innertuple = econtext->ecxt_outertuple;
		if (aggstate->input_done ||
			(node->aggstrategy != AGG_PLAIN &&
			 aggstate->projected_set != -1 &&
			 aggstate->projected_set < (numGroupingSets - 1) &&
			 nextSetSize > 0 &&
			 !gamma_vec_grouping_match(aggstate, nextSetSize,
									   econtext->ecxt_outertuple,
									   vaggstate->first_row, vaggstate->cur_row)))
		{
			aggstate->projected_set += 1;

			Assert(aggstate->projected_set < numGroupingSets);
			Assert(nextSetSize > 0 || aggstate->input_done);
		}
		else
		{
			/*
			 * We no longer care what group we just projected, the next
			 * projection will always be the first (or only) grouping set
			 * (unless the input proves to be empty).
			 */
			aggstate->projected_set = 0;

			/*
			 * If we don't already have the first tuple of the new group,
			 * fetch it from the outer plan.
			 */
			if (TupIsNull(vaggstate->grp_firstSlot))
			{
				outerslot = vec_fetch_input_tuple(vaggstate);
				vaggstate->first_row = 0;
				vaggstate->cur_row = 0;

				if (!TupIsNull(outerslot))
				{
					/*
					 * Make a copy of the first input tuple; we will use this
					 * for comparisons (in group mode) and for projection.
					 */
					if (node->aggstrategy != AGG_PLAIN && outerslot != vaggstate->grp_firstSlot)
						ExecCopySlot(vaggstate->grp_firstSlot, outerslot);
					else
						vaggstate->grp_firstSlot = outerslot;
					
					/* skip the invalid tuples */
					for (i = 0; i < VECTOR_SIZE; i++)
					{
						break;
						//TODO: get the first valid tuple
					}
				}
				else
				{
					/* outer plan produced no tuples at all */
					if (hasGroupingSets)
					{
						/*
						 * If there was no input at all, we need to project
						 * rows only if there are grouping sets of size 0.
						 * Note that this implies that there can't be any
						 * references to ungrouped Vars, which would otherwise
						 * cause issues with the empty output slot.
						 *
						 * XXX: This is no longer true, we currently deal with
						 * this in finalize_aggregates().
						 */
						aggstate->input_done = true;

						while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0)
						{
							aggstate->projected_set += 1;
							if (aggstate->projected_set >= numGroupingSets)
							{
								/*
								 * We can't set agg_done here because we might
								 * have more phases to do, even though the
								 * input is empty. So we need to restart the
								 * whole outer loop.
								 */
								break;
							}
						}

						if (aggstate->projected_set >= numGroupingSets)
							continue;
					}
					else
					{
						aggstate->agg_done = true;
						/* If we are grouping, we should produce no tuples too */
						if (node->aggstrategy != AGG_PLAIN)
							return NULL;
					}
				}
			}

			/*
			 * Initialize working state for a new input tuple group.
			 */
			initialize_aggregates(aggstate, pergroups, numReset);

			if (!TupIsNull(vaggstate->grp_firstSlot))
			{
				/* set up for first advance_aggregates call */
				tmpcontext->ecxt_outertuple = vaggstate->grp_firstSlot;
				vaggstate->first_row = vaggstate->cur_row;

				for (;;)
				{
					int setno;
					int allset = 0;
					int dim = tts_vector_get_dim(vaggstate->grp_firstSlot);

					if (node->aggstrategy != AGG_PLAIN)
					{
						/* begin to collect one group tuples */
						vaggstate->grouping_one_idx = 0;
						vaggstate->grouping_one[0] = vaggstate->first_row;
						vaggstate->grouping_one[1] = -1;

						tts_vector_slot_copy_one_row(vaggstate->outer_tuple_slot,
								vaggstate->grp_firstSlot,
								vaggstate->first_row);

						/* find the bound of group */
						for (vaggstate->cur_row = vaggstate->first_row;
							 vaggstate->cur_row < dim;
							 vaggstate->cur_row++)
						{
							//TODO: process skip, continue
							if (!gamma_vec_grouping_match(aggstate,
										node->numCols,
										vaggstate->grp_firstSlot,
										vaggstate->first_row,
										vaggstate->cur_row))
								break;
							vaggstate->grouping_one[vaggstate->grouping_one_idx++] = vaggstate->cur_row;
							if (vaggstate->grouping_one_idx < VECTOR_SIZE)
								vaggstate->grouping_one[vaggstate->grouping_one_idx] = -1;
						}

						/* process one group */
						gamma_vec_vslot_set_rows(vaggstate->grp_firstSlot, vaggstate->grouping_one);
					}
					else
					{
						/* have no group */
						vaggstate->cur_row = dim;
						gamma_vec_vslot_set_rows(vaggstate->grp_firstSlot, NULL);
					}

					allset = Max(aggstate->phase->numsets, 1);
					for (setno = 0; setno < allset; setno++)
					{
						select_current_set(aggstate, setno, true);
						gamma_vec_plain_advance_aggregates(vaggstate);
					}

					if (aggstate->aggstrategy == AGG_MIXED &&
							aggstate->current_phase == 1)
					{
						for (setno = 0; setno < aggstate->num_hashes; setno++)
						{
							gamma_vec_hashed_aggreggates_set(vaggstate, setno);
						}
					}

					/* Reset per-input-tuple context after each tuple */
					ResetExprContext(tmpcontext);

					if (vaggstate->cur_row < dim)
					{
						break;
					}
					else
					{
						outerslot = vec_fetch_input_tuple(vaggstate);
						if (TupIsNull(outerslot))
						{
							/* no more outer-plan tuples available */
							/* if we built hash tables, finalize any spills */
							if (aggstate->aggstrategy == AGG_MIXED &&
									aggstate->current_phase == 1)
								hashagg_finish_initial_spills(aggstate);

							if (hasGroupingSets)
							{
								aggstate->input_done = true;
								break;
							}
							else
							{
								aggstate->agg_done = true;
								break;
							}
						}

						/* set up for next advance_aggregates call */
						if (node->aggstrategy != AGG_PLAIN &&
							outerslot != vaggstate->grp_firstSlot)
						{
							ExecClearTuple(vaggstate->grp_firstSlot);
							ExecCopySlot(vaggstate->grp_firstSlot, outerslot);
						}
						else
						{
							vaggstate->grp_firstSlot = outerslot;
						}

						tmpcontext->ecxt_outertuple = vaggstate->grp_firstSlot;
						vaggstate->first_row = 0;
						vaggstate->cur_row = 0;
						//TODO: process skip

					}

					/* The last row in the vector is the last row in the group */
					if (node->aggstrategy != AGG_PLAIN)
					{
						if (!gamma_vec_grouping_row_match(aggstate,
									node->numCols,
									vaggstate->outer_tuple_slot,
									vaggstate->grp_firstSlot,
									vaggstate->first_row))
							break;
					}
				}
			}

			/*
			 * Use the representative input tuple for any references to
			 * non-aggregated input columns in aggregate direct args, the node
			 * qual, and the tlist.  (If we are not grouping, and there are no
			 * input rows at all, we will come here with an empty firstSlot
			 * ... but if not grouping, there can't be any references to
			 * non-aggregated input columns, so no problem.)
			 */
			econtext->ecxt_outertuple = vaggstate->grp_firstSlot;
		}

		econtext->ecxt_outertuple = vaggstate->outer_tuple_slot;

		Assert(aggstate->projected_set >= 0);

		currentSet = aggstate->projected_set;

		prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);

		select_current_set(aggstate, currentSet, false);

		finalize_aggregates(aggstate,
							peragg,
							pergroups[currentSet]);

		/*
		 * If there's no row to project right now, we must continue rather
		 * than returning a null since there might be more groups.
		 */
		result = project_aggregates(aggstate);

		econtext->ecxt_outertuple = vaggstate->grp_firstSlot;

		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

static TupleTableSlot *
vec_agg_retrieve_hash_table(VecAggState *vaggstate)
{
	TupleTableSlot *result = NULL;
	AggState *aggstate = vaggstate->aggstate;

	while (result == NULL)
	{
		result = vec_agg_retrieve_hash_table_in_memory(vaggstate);
		if (result == NULL)
		{
			if (!agg_refill_hash_table(aggstate))
			{
				aggstate->agg_done = true;
				break;
			}
		}
	}

	return result;
}

static TupleTableSlot *
vec_agg_retrieve_hash_table_in_memory(VecAggState *vaggstate)
{
	ExprContext *econtext;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleHashEntryData *entry;
	TupleTableSlot *firstSlot;
	TupleTableSlot *result;
	AggStatePerHash perhash;
	VecTupleHashEntry ventry;

	AggState *aggstate = vaggstate->aggstate;

	/*
	 * get state info from node.
	 *
	 * econtext is the per-output-tuple expression context.
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * Note that perhash (and therefore anything accessed through it) can
	 * change inside the loop, as we change between grouping sets.
	 */
	perhash = &aggstate->perhash[aggstate->current_set];

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	for (;;)
	{
		TupleTableSlot *hashslot = perhash->hashslot;
		int			i;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Find the next entry in the hash table
		 */
		ventry = VecScanTupleHashTable(perhash->hashtable,
								(vec_tuplehash_iterator *)&perhash->hashiter);
		entry = (TupleHashEntryData *) ventry;

		if (entry == NULL)
		{
			int			nextset = aggstate->current_set + 1;

			if (nextset < aggstate->num_hashes)
			{
				/*
				 * Switch to next grouping set, reinitialize, and restart the
				 * loop.
				 */
				select_current_set(aggstate, nextset, true);

				perhash = &aggstate->perhash[aggstate->current_set];

				VecResetTupleHashIterator(perhash->hashtable,
								(vec_tuplehash_iterator *)&perhash->hashiter);

				continue;
			}
			else
			{
				return NULL;
			}
		}

		/*
		 * Clear the per-output-tuple context for each group
		 *
		 * We intentionally don't use ReScanExprContext here; if any aggs have
		 * registered shutdown callbacks, they mustn't be called yet, since we
		 * might not be done with that agg.
		 */
		ResetExprContext(econtext);

		/*
		 * Transform representative tuple back into one with the right
		 * columns.
		 */
		//ExecStoreMinimalTuple(entry->firstTuple, hashslot, false);
		//slot_getallattrs(hashslot);
		hashslot = ventry->first_slot;

		ExecClearTuple(firstSlot);
		memset(firstSlot->tts_isnull, true,
			   firstSlot->tts_tupleDescriptor->natts * sizeof(bool));

		for (i = 0; i < perhash->numhashGrpCols; i++)
		{
			int			varNumber = perhash->hashGrpColIdxInput[i] - 1;

			firstSlot->tts_values[varNumber] = hashslot->tts_values[i];
			firstSlot->tts_isnull[varNumber] = hashslot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(firstSlot);

		pergroup = (AggStatePerGroup) entry->additional;

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_outertuple = firstSlot;

		prepare_projection_slot(aggstate,
								econtext->ecxt_outertuple,
								aggstate->current_set);

		finalize_aggregates(aggstate, peragg, pergroup);

		result = project_aggregates(aggstate);
		if (result)
			return result;
	}

	/* No more groups */
	return NULL;
}

static void
vec_agg_fill_hash_table(VecAggState *vaggstate)
{
	TupleTableSlot *outerslot;
	AggState *aggstate = vaggstate->aggstate;
	ExprContext *tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until we
	 * exhaust the outer plan.
	 */
	for (;;)
	{
		int setno;
		outerslot = vec_fetch_input_tuple(vaggstate);
		if (TupIsNull(outerslot))
			break;

		/* set up for lookup_hash_entries and advance_aggregates */
		tmpcontext->ecxt_outertuple = outerslot;

		for (setno = 0; setno < aggstate->num_hashes; setno++)
		{
			gamma_vec_hashed_aggreggates_set(vaggstate, setno);
		}

		/*
		 * Reset per-input-tuple context after each tuple, but note that the
		 * hash lookups do this too
		 */
		//TODO: check if need to reset each time
		ResetExprContext(aggstate->tmpcontext);
	}

	/* finalize spills, if any */
	hashagg_finish_initial_spills(aggstate);

	aggstate->table_filled = true;

	/* Initialize to walk the first hash table */
	select_current_set(aggstate, 0, true);
	VecResetTupleHashIterator(aggstate->perhash[0].hashtable,
						   (vec_tuplehash_iterator *)&aggstate->perhash[0].hashiter);
}

static void
gamma_vec_calc_hash_value(VecAggState *vaggstate, int setno, int32 *hashkeys)
{
	AggState *aggstate = vaggstate->aggstate;
	AggStatePerHash perhash = &aggstate->perhash[setno];
	TupleHashTable hashtable = perhash->hashtable;
	uint32 hashkey_iv = hashtable->hash_iv;
	int i,j;

	TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
	VectorTupleSlot *vouterslot = (VectorTupleSlot *) outerslot;

	int numCols = hashtable->numCols;
	FmgrInfo   *hashfunctions = hashtable->tab_hash_funcs;
	int32 dim = vouterslot->dim;
	TupleDesc tupdesc = outerslot->tts_tupleDescriptor;

	for (j = 0; j < dim; j++)
	{
		hashkeys[j] = hashkey_iv;
	}

	for (i = 0; i < numCols; i++)
	{
		AttrNumber att;
		vdatum *vec_value;

		/* map to outer tuple slot */
		att = perhash->hashGrpColIdxInput[i] - 1;
		if (!is_vec_type(tupdesc->attrs[att].atttypid))
		{
			Datum datum = outerslot->tts_values[att];
			if (!outerslot->tts_isnull[att])
			{
				for (j = 0; j < dim; j++)
				{
					uint32 hkey = gamma_hash_datum(&hashfunctions[i],
							datum, hashtable->tab_collations[i]);
					hashkeys[j] =
						(hashkeys[j] << 1) | ((hashkeys[j] & 0x80000000) ? 1 : 0);
					hashkeys[j] ^= hkey;
				}
			}
			continue;
		}

		vec_value = (vdatum *) outerslot->tts_values[att];

		for (j = 0; j < dim; j++)
		{
			if (vec_value->skipref != NULL && vec_value->skipref[j])
				continue;

			/* rotate hashkey left 1 bit at each step */
			hashkeys[j] = (hashkeys[j] << 1) | ((hashkeys[j] & 0x80000000) ? 1 : 0);

			/* treat nulls as having hash key 0 */
			if (!VDATUM_ISNULL(vec_value, j))
			{
				uint32 hkey = gamma_hash_datum(&hashfunctions[i],
						VDATUM_DATUM(vec_value, j), hashtable->tab_collations[i]);
				hashkeys[j] ^= hkey;
			}

			/* do hash if the last column is processed */
			if (i == numCols - 1)
				hashkeys[j] = murmurhash32(hashkeys[j]);
		}
	}

	return;
}

static short indexarr_cache[VECTOR_SIZE][VECTOR_SIZE];

static void
gamma_vec_reset_entry_batch(VecAggState *vaggstate, VecTupleHashEntry entry, int row)
{
	entry->indexarr_dim = 0;
	entry->indexarr = (short *) &indexarr_cache[row][0];

	return;
}

static void
gamma_vec_initialize_hashentry(VecAggState *vaggstate, VecTupleHashEntry entry,
		TupleTableSlot *slot, int32 row)
{
	MemoryContext oldctx;
	int col;
	AggState *aggstate = vaggstate->aggstate;
	AggStatePerHash perhash = &aggstate->perhash[aggstate->current_set];
	TupleHashTable hashtable = perhash->hashtable;
	TupleDesc tupdesc = slot->tts_tupleDescriptor;

	/* Copy the first tuple in the group and use it when projecting */
	oldctx = MemoryContextSwitchTo(hashtable->tablecxt);

	/* init first slot */
	entry->first_slot = MakeTupleTableSlot(tupdesc, &TTSOpsVirtual);

	for (col = 0; col < tupdesc->natts; col++)
	{
		vdatum *column = (vdatum *)DatumGetPointer(slot->tts_values[col]);
		if (!is_vec_type(tupdesc->attrs[col].atttypid))
		{
			entry->first_slot->tts_values[col] = datumCopy(slot->tts_values[col],
													tupdesc->attrs[col].attbyval,
													tupdesc->attrs[col].attlen);
			entry->first_slot->tts_isnull[col] = slot->tts_isnull[col];
			continue;
		}
		else if (tupdesc->attrs[col].attlen > 0)
		{
			entry->first_slot->tts_values[col] = VDATUM_DATUM(column, row);
		}
		else
		{
			entry->first_slot->tts_values[col] = datumCopy(VDATUM_DATUM(column, row),
					tupdesc->attrs[col].attbyval,
					tupdesc->attrs[col].attlen);
		}
		entry->first_slot->tts_isnull[col] = VDATUM_ISNULL(column, row);
	}

	ExecStoreVirtualTuple(entry->first_slot);

	MemoryContextSwitchTo(oldctx);

	return;
}

static void
gamma_vec_lookup_hash_entries(VecAggState *vaggstate)
{
	int32 i;
	AggState *aggstate = vaggstate->aggstate;
	AggStatePerGroup *pergroup = aggstate->hash_pergroup;
	TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
	VectorTupleSlot *vouterslot = (VectorTupleSlot *) outerslot;
	int32 setno = aggstate->current_set;
	int32 hashkeys[VECTOR_SIZE];
	VecTupleHashEntry *entries = (VecTupleHashEntry *)vaggstate->entries;

	short row_indexarr[VECTOR_SIZE];
	
	AggStatePerHash perhash = &aggstate->perhash[setno];
	TupleHashTable hashtable = perhash->hashtable;
	VecTupleHashTable vhashtable = (VecTupleHashTable) hashtable;

	vaggstate->entries_dim = vouterslot->dim;
	vaggstate->spill_dim = 0;

	row_indexarr[0] = 0;
	row_indexarr[1] = -1;
	vouterslot->row_indexarr = (short *) row_indexarr;

	gamma_vec_hashtable_grow(aggstate, setno, VECTOR_SIZE); 
	gamma_vec_calc_hash_value(vaggstate, setno, hashkeys);

	for (i = 0; i < vouterslot->dim; i++)
	{
		bool isnew = false;
		bool *p_isnew;

		if (vouterslot->skip[i])
		{
			entries[i] = NULL;
			continue;
		}

		row_indexarr[0] = i;

		/* if hash table already spilled, don't create new entries */
		p_isnew = aggstate->hash_spill_mode ? NULL : &isnew;
		entries[i] = VecLookupTupleHashEntryHash(vhashtable, outerslot,
				p_isnew, hashkeys[i]);

		if (entries[i] != NULL)
		{
			if (isnew)
			{
				gamma_initialize_hash_entry(aggstate, hashtable,
											(TupleHashEntry) entries[i]);
				gamma_vec_initialize_hashentry(vaggstate, entries[i], outerslot, i);
			}

			gamma_vec_reset_entry_batch(vaggstate, entries[i], i);
		}
	}

	/* batch process spill tuples */
	for (i = 0; i < vouterslot->dim; i++)
	{
		int col;
		HashAggSpill *spill = &aggstate->hash_spills[setno];
		TupleTableSlot *slot = aggstate->tmpcontext->ecxt_outertuple;
		TupleTableSlot *rowslot = vaggstate->outer_tuple_slot;

		if (vouterslot->skip[i])
			continue;

		if (entries[i] != NULL)
			continue;

		//TODO:batch write ? move to same level with advance_agg?

		for (col = 0; col < slot->tts_tupleDescriptor->natts; col++)
		{
			vdatum *vec_value = (vdatum *)DatumGetPointer(slot->tts_values[col]);
			rowslot->tts_values[col] = VDATUM_DATUM(vec_value, i);
			rowslot->tts_isnull[col] = VDATUM_ISNULL(vec_value, i);
		}

		ExecStoreVirtualTuple(rowslot);

		if (spill->partitions == NULL)
			hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
					perhash->aggnode->numGroups,
					aggstate->hashentrysize);

		hashagg_spill_tuple(aggstate, spill, rowslot, hashkeys[i]);
		pergroup[setno] = NULL;
	}

	/* batch tuples belonging to the same entry together*/
	for (i = 0; i < vouterslot->dim; i++)
	{
		if (entries[i] != NULL)
		{
			VecTupleHashEntry entry = entries[i];
			int indexarr_dim;
			short *indexarr = entry->indexarr;

			indexarr[entry->indexarr_dim++] = i;
			indexarr_dim = entry->indexarr_dim;
			if (indexarr_dim < VECTOR_SIZE)
				indexarr[indexarr_dim] = -1; /* TODO: new loop? */

			if (indexarr_dim > 1)
				entries[i] = NULL;
		}
	}
}

static void
gamma_vec_plain_advance_aggregates(VecAggState *vaggstate)
{
	bool dummynull;
	int transno;
	AggState *aggstate = vaggstate->aggstate;
	int setno = aggstate->current_set;
	ExprState **expr_state = (ExprState **) aggstate->phase->evaltrans;
	AggStatePerTrans transstates = aggstate->pertrans;
	int numTrans = aggstate->numtrans;

	//gamma_vec_vslot_set_rows(outerslot, NULL);

	ExecEvalExprSwitchContext(expr_state[setno],
			aggstate->tmpcontext,
			&dummynull);

	for (transno = 0; transno < numTrans; transno++)
	{
		AggStatePerTrans pertrans = &transstates[transno];

#if PG_VERSION_NUM < 160000
		if (pertrans->numInputs > 0 && pertrans->numSortCols > 0)
#else
		if (pertrans->aggsortrequired)
#endif
		{
			int i;
			int k;
			vdatum *vec_value = NULL;
			short *indexarr = NULL;
			ExecClearTuple(pertrans->sortslot);
			pertrans->sortslot->tts_nvalid = pertrans->numInputs;
			ExecStoreVirtualTuple(pertrans->sortslot);

			vec_value = (vdatum *) pertrans->sortslot->tts_values[0];
			indexarr = vec_value->indexarr;

			for (i = 0; i < vec_value->dim; i++)
			{
				if (indexarr != NULL)
				{
					if (indexarr[i] < 0)
						break;

					k = indexarr[i];
				}
				else
					k = i;

				if (pertrans->numInputs == 1)
				{
					tuplesort_putdatum(pertrans->sortstates[setno],
							VDATUM_DATUM(vec_value, k),
							VDATUM_ISNULL(vec_value, k));
					continue;
				}

				ExecClearTuple(vaggstate->sortslot[transno]);
				tts_vector_slot_copy_one_row(vaggstate->sortslot[transno],
												   pertrans->sortslot, k);
				tuplesort_puttupleslot(pertrans->sortstates[setno],
									   vaggstate->sortslot[transno]);
			}
		}
#if PG_VERSION_NUM >= 160000
		/* Handle DISTINCT aggregates which have pre-sorted input */
		else if (pertrans->numDistinctCols ==1 && !pertrans->aggsortrequired) 
		{
			int i;
			VectorTupleSlot *vslot = (VectorTupleSlot *)aggstate->tmpcontext->ecxt_outertuple;
			short *indexarr = vslot->row_indexarr;
			AggStatePerGroup pergroup = &aggstate->all_pergroups[setno][transno];
			vdatum *vec_value = (vdatum *)pertrans->transfn_fcinfo->args[1].value;

			Datum value;
			bool isnull;

			for (i = 0; i < vec_value->dim; i++)
			{
				if (indexarr != NULL)
				{
					if (indexarr[i] == -1)
						break;

					value = vec_value->values[indexarr[i]];
					isnull = vec_value->isnull[indexarr[i]];
				}
				else
				{
					value = vec_value->values[i];
					isnull = vec_value->isnull[i];
				}

				if (!pertrans->haslast ||
					pertrans->lastisnull != isnull ||
					(!isnull && !DatumGetBool(FunctionCall2Coll(&pertrans->equalfnOne,
															pertrans->aggCollation,
															pertrans->lastdatum, value))))
				{
					if (pertrans->haslast && !pertrans->inputtypeByVal &&
							!pertrans->lastisnull)
						pfree(DatumGetPointer(pertrans->lastdatum));

					pertrans->haslast = true;
					if (!isnull)
					{
						MemoryContext oldContext;
						oldContext =
							MemoryContextSwitchTo(aggstate->aggcontexts[setno]->ecxt_per_tuple_memory);

						pertrans->lastdatum = datumCopy(value, pertrans->inputtypeByVal,
								pertrans->inputtypeLen);

						MemoryContextSwitchTo(oldContext);
					}
					else
						pertrans->lastdatum = (Datum) 0;
					pertrans->lastisnull = isnull;

					pergroup->transValue =
						(Datum) ((unsigned long)pergroup->transValue + 1);
				}
			}

			continue;
		}
		else if (pertrans->numDistinctCols > 0 && !pertrans->aggsortrequired)
		{
			//TODO:
			elog(ERROR, "PG 16 NOT SUPPORT MULTI-COLS DISTINCT");
		}
#endif
		else if (pertrans->aggref->aggstar)
		{
			int i;
			unsigned long k = 0;
            AggStatePerGroup pergroup = &aggstate->all_pergroups[setno][transno];
			VectorTupleSlot *vslot = (VectorTupleSlot *)aggstate->tmpcontext->ecxt_outertuple;
			short *indexarr = vslot->row_indexarr;

			if (indexarr == NULL && VSlotHasNonSkip(vslot))
			{
				k = vslot->dim;
			}
			else
			{
				for (i = 0; i < vslot->dim; i++)
				{
					if (indexarr != NULL)
					{
						if (indexarr[i] < 0)
							break;

						k++;
					}
					else
					{
						if (vslot->skip[i])
							continue;

						k++;
					}
				}
			}

			pergroup->transValue = (Datum) ((unsigned long)pergroup->transValue + k);
		}
	}
}

static inline void
gamma_vec_vslot_set_rows(TupleTableSlot *slot, short *indexarr)
{
	TupleDesc tupdesc = slot->tts_tupleDescriptor;
	int natts = tupdesc->natts;
	int i;

	for (i = 0; i < natts; i++)
	{
		vdatum *vec_value;
		if (!is_vec_type(tupdesc->attrs[i].atttypid))
			continue;

		vec_value = (vdatum *)slot->tts_values[i];
		vec_value->indexarr = indexarr;
	}

	((VectorTupleSlot *)slot)->row_indexarr = indexarr;

	return;
}

static inline void
gamma_vec_hashed_aggreggates_set(VecAggState *vaggstate, int setno)
{
	AggState *aggstate = vaggstate->aggstate;
	
	Assert(aggstate != NULL);

	select_current_set(aggstate, setno, true);
	gamma_vec_lookup_hash_entries(vaggstate);
	gamma_vec_hashed_advance_aggregates(vaggstate);
}

static void
gamma_vec_hashed_advance_aggregates(VecAggState *vaggstate)
{
	int i;
	bool dummynull;
	int transno;
	AggState *aggstate = vaggstate->aggstate;
	AggStatePerGroup *pergroup = aggstate->hash_pergroup;
	TupleTableSlot *outerslot = aggstate->tmpcontext->ecxt_outertuple;
	int setno = aggstate->current_set;
	int setoff = setno;
	ExprState **expr_state = (ExprState **) aggstate->phase->evaltrans;
	AggStatePerTrans transstates = aggstate->pertrans;
	int numTrans = aggstate->numtrans;

	if (aggstate->aggstrategy == AGG_MIXED && aggstate->current_phase == 1)
	{
		setoff = setoff + aggstate->maxsets;
	}

	for (i = 0; i < vaggstate->entries_dim; i++)
	{
		if (vaggstate->entries[i] == NULL)
			continue;

		pergroup[setno] = vaggstate->entries[i]->additional;
		//vouterslot->row_indexarr = vaggstate->entries[i]->indexarr;
		gamma_vec_vslot_set_rows(outerslot, vaggstate->entries[i]->indexarr);
		ExecEvalExprSwitchContext(expr_state[setoff],
				aggstate->tmpcontext,
				&dummynull);

		for (transno = 0; transno < numTrans; transno++)
		{
			AggStatePerTrans pertrans = &transstates[transno];

			if (pertrans->aggref->aggstar)
			{
				int j;
				unsigned long k = 0;
				AggStatePerGroup pergroup_set = &pergroup[setno][transno];
				VectorTupleSlot *vslot = (VectorTupleSlot *)outerslot;
				short *indexarr = vslot->row_indexarr;

				for (j = 0; j < VECTOR_SIZE; j++)
				{
					if (indexarr != NULL)
					{
						if (indexarr[j] < 0)
							break;

						k++;
					}
					else
					{
						if (vslot->skip[j])
							continue;

						k++;
					}
				}

				pergroup_set->transValue = (Datum) ((unsigned long)pergroup_set->transValue + k);
			}
		}
	}
} 

static void
vec_build_hash_tables(AggState *aggstate)
{
	int			setno;

	for (setno = 0; setno < aggstate->num_hashes; ++setno)
	{
		AggStatePerHash perhash = &aggstate->perhash[setno];
		VecTupleHashTable vhashtable = (VecTupleHashTable) perhash->hashtable;
		long		nbuckets;
		Size		memory;

		if (perhash->hashtable != NULL)
		{
			VecResetTupleHashTable(vhashtable);
			continue;
		}

		Assert(perhash->aggnode->numGroups > 0);

		memory = aggstate->hash_mem_limit / aggstate->num_hashes;

		nbuckets = hash_choose_num_buckets(aggstate->hashentrysize,
										   perhash->aggnode->numGroups,
										   memory);

		vec_build_hash_table(aggstate, setno, nbuckets);
	}

	aggstate->hash_ngroups_current = 0;
}

/*
 * Build a single hashtable for this grouping set.
 */
static void
vec_build_hash_table(AggState *aggstate, int setno, long nbuckets)
{
	AggStatePerHash perhash = &aggstate->perhash[setno];
	MemoryContext metacxt = aggstate->hash_metacxt;
	MemoryContext hashcxt = aggstate->hashcontext->ecxt_per_tuple_memory;
	MemoryContext tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size		additionalsize;

	Assert(aggstate->aggstrategy == AGG_HASHED ||
		   aggstate->aggstrategy == AGG_MIXED);

	/*
	 * Used to make sure initial hash table allocation does not exceed
	 * hash_mem. Note that the estimate does not include space for
	 * pass-by-reference transition data values, nor for the representative
	 * tuple of each group.
	 */
	additionalsize = aggstate->numtrans * sizeof(AggStatePerGroupData);

	perhash->hashtable = (TupleHashTable) VecBuildTupleHashTableExt(&aggstate->ss.ps,
												perhash->hashslot->tts_tupleDescriptor,
												perhash->numCols,
												perhash->hashGrpColIdxHash,
												perhash->eqfuncoids,
												perhash->hashfunctions,
												perhash->aggnode->grpCollations,
												nbuckets,
												additionalsize,
												metacxt,
												hashcxt,
												tmpcxt,
												DO_AGGSPLIT_SKIPFINAL(aggstate->aggsplit));
}

static void
gamma_vec_reset_phase(AggState *aggstate)
{
	/*
	 * Initialize current phase-dependent values to initial phase. The initial
	 * phase is 1 (first sort pass) for all strategies that use sorting (if
	 * hashing is being done too, then phase 0 is processed last); but if only
	 * hashing is being done, then phase 0 is all there is.
	 */ 
	if (aggstate->aggstrategy == AGG_HASHED)
	{
		aggstate->current_phase = 0;
		initialize_phase(aggstate, 0);
		select_current_set(aggstate, 0, true);
	}
	else
	{
		aggstate->current_phase = 1;
		initialize_phase(aggstate, 1);
		select_current_set(aggstate, 0, false);
	}

	return;
}

static void
vec_build_sort_grouping_match(AggState *aggstate)
{
	int i = 0;

	for (i = 0; i < aggstate->numphases; i++)
	{
		AggStatePerPhase phase = &aggstate->phases[i];

		if (phase->aggstrategy != AGG_SORTED)
			continue;

		//TODO: free prev one
		phase->eqfunctions = (ExprState **)vec_exec_grouping_match_prepare(
											phase->aggnode->numCols,
											phase->aggnode->grpOperators);

	}
}
