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

#include "access/parallel.h"
#include "catalog/pg_collation.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "executor/nodeSubplan.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "executor/gamma_expr.h"
#include "executor/gamma_vec_exec_grouping.h"
#include "executor/vector_tuple_slot.h"
#include "utils/gamma_cache.h"
#include "utils/utils.h"

//#include "../src/postgres/executor/execExpr.c"

static ExprState *
VecExecBuildAggTransPerSet(AggState *aggstate, AggStatePerPhase phase,
				  int setno, int setoff, bool ishash, bool nullcheck);

static bool VecTupleHashTableMatch(struct vec_tuplehash_hash *tb,
					const TupleTableSlot* slot1,
					const TupleTableSlot* slot2);
static inline uint32 VecTupleHashTableHash_internal(struct vec_tuplehash_hash *tb,
												 const TupleTableSlot *slot);

#define SH_GROW_MAX_MOVE VECTOR_SIZE
#define SH_GROW_MAX_DIB VECTOR_SIZE 
#define SH_GROW_MIN_FILLFACTOR 0.5

#define SH_PREFIX vec_tuplehash
#define SH_ELEMENT_TYPE VecTupleHashEntryData
#define SH_KEY_TYPE TupleTableSlot *
#define SH_KEY first_slot
#define SH_HASH_KEY(tb, key) VecTupleHashTableHash_internal(tb, key)
#define SH_EQUAL(tb, a, b) VecTupleHashTableMatch(tb, a, b)
#define SH_SCOPE extern
#define SH_STORE_HASH
#define SH_MANUAL_GROW
#define SH_GET_HASH(tb, a) a->hash
#define SH_DEFINE
#include "utils/gamma_hash.h"

static inline VecTupleHashEntry VecLookupTupleHashEntry_internal(VecTupleHashTable vec_hashtable,
														   TupleTableSlot *slot,
														   bool *isnew, uint32 hash);
#if 0
#define SH_STORE_HASH
static VecTupleHashEntry
gamma_vec_tuplehash_insert_hash(struct vec_tuplehash_hash *tb,
								TupleTableSlot *slot,
								uint32 hash, bool *found)
{
	uint32		startelem;
	uint32		curelem;
	VecTupleHashEntry data;

	if (unlikely(tb->members >= tb->grow_threshold))
	{
        if (unlikely(tb->size == PG_UINT32_MAX))
			elog(ERROR, "hash table size exceeded");

		/*
		 * The gamma_vec_hashtable_grow function is called once
		 * for every VECTOR_SIZE.
		 */
	}

	/* perform insert, start bucket search at optimal location */
	data = tb->data;
	startelem = vec_tuplehash_initial_bucket(tb, hash);
	curelem = startelem;
	while (true)
	{
		VecTupleHashEntry entry = &data[curelem];

		/* any empty bucket can directly be used */
		if (entry->status == 0/*TODO: EMPTY*/)
		{
			tb->members++;
			entry->first_slot = slot;
#ifdef SH_STORE_HASH
			entry->hash = hash;
#endif
			entry->status = 1;/*TODO:SH_STATUS_IN_USE*/;
			*found = false;
			return entry;
		}

		/*
		 * If the bucket is not empty, we either found a match (in which case
		 * we're done), or we have to decide whether to skip over or move the
		 * colliding entry. When the colliding element's distance to its
		 * optimal position is smaller than the to-be-inserted entry's, we
		 * shift the colliding entry (and its followers) forward by one.
		 */

		if (entry->hash == hash && VecTupleHashTableMatch(tb, entry->first_slot, slot))
		{
			Assert(entry->status == 1/*TODO:SH_STATUS_IN_USE*/);
			*found = true;
			return entry;
		}

		curelem = vec_tuplehash_next(tb, curelem, startelem);
	}
}

#undef SH_STORE_HASH
#undef SH_MANUAL_GROW
#endif

/*****************************************************************************
 *		Utility routines for grouping tuples together
 *****************************************************************************/

FmgrInfo *
vec_exec_tuples_match_prepare(int numCols, const Oid *eqOperators)
{
	FmgrInfo* eqFunctions = (FmgrInfo*)palloc(numCols * sizeof(FmgrInfo));
	int i;

	for (i = 0; i < numCols; i++) {
		Oid eq_opr = eqOperators[i];
		//Oid eq_function;

		//eq_function = get_opcode(eq_opr);
		fmgr_info(eq_opr, &eqFunctions[i]);
	}

	return eqFunctions;
}

FmgrInfo *
vec_exec_grouping_match_prepare(int numCols,
					  const Oid *eqOperators)
{
	int i;
	FmgrInfo* eqFunctions = (FmgrInfo*)palloc(numCols * sizeof(FmgrInfo));

	for (i = 0; i < numCols; i++)
	{
		Oid			eq_opr = eqOperators[i];
		Oid			eq_function;

		eq_function = get_opcode(eq_opr);
		fmgr_info(eq_function, &eqFunctions[i]);
	}

	return eqFunctions;
}

/*****************************************************************************
 *		Utility routines for all-in-memory hash tables
 *
 * These routines build hash tables for grouping tuples together (eg, for
 * hash aggregation).  There is one entry for each not-distinct set of tuples
 * presented.
 *****************************************************************************/

/*
 * Construct an empty TupleHashTable
 *
 *	numCols, keyColIdx: identify the tuple fields to use as lookup key
 *	eqfunctions: equality comparison functions to use
 *	hashfunctions: datatype-specific hashing functions to use
 *	nbuckets: initial estimate of hashtable size
 *	additionalsize: size of data stored in ->additional
 *	metacxt: memory context for long-lived allocation, but not per-entry data
 *	tablecxt: memory context in which to store table entries
 *	tempcxt: short-lived context for evaluation hash and comparison functions
 *
 * The function arrays may be made with execTuplesHashPrepare().  Note they
 * are not cross-type functions, but expect to see the table datatype(s)
 * on both sides.
 *
 * Note that keyColIdx, eqfunctions, and hashfunctions must be allocated in
 * storage that will live as long as the hashtable does.
 */
VecTupleHashTable
VecBuildTupleHashTableExt(PlanState *parent,
					   TupleDesc inputDesc,
					   int numCols, AttrNumber *keyColIdx,
					   const Oid *eqfuncoids,
					   FmgrInfo *hashfunctions,
					   Oid *collations,
					   long nbuckets, Size additionalsize,
					   MemoryContext metacxt,
					   MemoryContext tablecxt,
					   MemoryContext tempcxt,
					   bool use_variable_hash_iv)
{
	//TupleHashTable hashtable;
	VecTupleHashTable hashtable;
	Size		entrysize = sizeof(VecTupleHashEntryData) + additionalsize;
	Size		hash_mem_limit;
	MemoryContext oldcontext;
	bool		allow_jit;

	Assert(nbuckets > 0);

	/* Limit initial table size request to not more than hash_mem */
	hash_mem_limit = get_hash_memory_limit() / entrysize;
	if (nbuckets > hash_mem_limit)
		nbuckets = hash_mem_limit;

	oldcontext = MemoryContextSwitchTo(metacxt);

	hashtable = (VecTupleHashTable) palloc(sizeof(VecTupleHashTableData));
	//hashtable = (TupleHashTable) vhashtable;

	hashtable->numCols = numCols;
	hashtable->keyColIdx = keyColIdx;
	hashtable->tab_hash_funcs = hashfunctions;
	hashtable->tab_collations = collations;
	hashtable->tablecxt = tablecxt;
	hashtable->tempcxt = tempcxt;
	hashtable->entrysize = entrysize;
	hashtable->tableslot = NULL;	/* will be made on first lookup */
	hashtable->inputslot = NULL;
	hashtable->in_hash_funcs = NULL;
	hashtable->cur_eq_func = NULL;

	hashtable->planstate = parent;

	/*
	 * If parallelism is in use, even if the leader backend is performing the
	 * scan itself, we don't want to create the hashtable exactly the same way
	 * in all workers. As hashtables are iterated over in keyspace-order,
	 * doing so in all processes in the same way is likely to lead to
	 * "unbalanced" hashtables when the table size initially is
	 * underestimated.
	 */
	if (use_variable_hash_iv)
		hashtable->hash_iv = murmurhash32(ParallelWorkerNumber);
	else
		hashtable->hash_iv = 0;

	hashtable->hashtab = vec_tuplehash_create(metacxt, nbuckets, hashtable);

	/*
	 * We copy the input tuple descriptor just for safety --- we assume all
	 * input tuples will have equivalent descriptors.
	 */
	hashtable->tableslot = MakeSingleTupleTableSlot(CreateTupleDescCopy(inputDesc),
													&TTSOpsMinimalTuple);

	/*
	 * If the old reset interface is used (i.e. BuildTupleHashTable, rather
	 * than BuildTupleHashTableExt), allowing JIT would lead to the generated
	 * functions to a) live longer than the query b) be re-generated each time
	 * the table is being reset. Therefore prevent JIT from being used in that
	 * case, by not providing a parent node (which prevents accessing the
	 * JitContext in the EState).
	 */
	allow_jit = metacxt != tablecxt;

	/* build comparator for all columns */
	/* XXX: should we support non-minimal tuples for the inputslot? */
	hashtable->tab_eq_func = ExecBuildGroupingEqual(inputDesc, inputDesc,
													&TTSOpsMinimalTuple, &TTSOpsMinimalTuple,
													numCols,
													keyColIdx, eqfuncoids, collations,
													allow_jit ? parent : NULL);

	hashtable->eqfunctions = vec_exec_tuples_match_prepare(numCols, eqfuncoids);

	/*
	 * While not pretty, it's ok to not shut down this context, but instead
	 * rely on the containing memory context being reset, as
	 * ExecBuildGroupingEqual() only builds a very simple expression calling
	 * functions (i.e. nothing that'd employ RegisterExprContextCallback()).
	 */
	hashtable->exprcontext = CreateStandaloneExprContext();

	MemoryContextSwitchTo(oldcontext);

	return hashtable;
}

/*
 * BuildTupleHashTable is a backwards-compatibilty wrapper for
 * BuildTupleHashTableExt(), that allocates the hashtable's metadata in
 * tablecxt. Note that hashtables created this way cannot be reset leak-free
 * with ResetTupleHashTable().
 */
VecTupleHashTable
VecBuildTupleHashTable(PlanState *parent,
					TupleDesc inputDesc,
					int numCols, AttrNumber *keyColIdx,
					const Oid *eqfuncoids,
					FmgrInfo *hashfunctions,
					Oid *collations,
					long nbuckets, Size additionalsize,
					MemoryContext tablecxt,
					MemoryContext tempcxt,
					bool use_variable_hash_iv)
{
	return VecBuildTupleHashTableExt(parent,
								  inputDesc,
								  numCols, keyColIdx,
								  eqfuncoids,
								  hashfunctions,
								  collations,
								  nbuckets, additionalsize,
								  tablecxt,
								  tablecxt,
								  tempcxt,
								  use_variable_hash_iv);
}

/*
 * Reset contents of the hashtable to be empty, preserving all the non-content
 * state. Note that the tablecxt passed to BuildTupleHashTableExt() should
 * also be reset, otherwise there will be leaks.
 */
void
VecResetTupleHashTable(VecTupleHashTable hashtable)
{
	vec_tuplehash_reset(hashtable->hashtab);
}

/*
 * A variant of LookupTupleHashEntry for callers that have already computed
 * the hash value.
 */
VecTupleHashEntry
VecLookupTupleHashEntryHash(VecTupleHashTable hashtable, TupleTableSlot *slot,
						 bool *isnew, uint32 hash)
{
	VecTupleHashEntry entry;
	MemoryContext oldContext;

	/* Need to run the hash functions in short-lived context */
	oldContext = MemoryContextSwitchTo(hashtable->tempcxt);

	/* set up data needed by hash and match functions */
#if 0
	hashtable->inputslot = slot;
	hashtable->in_hash_funcs = hashtable->tab_hash_funcs;
	hashtable->cur_eq_func = hashtable->tab_eq_func;
#endif

	entry = VecLookupTupleHashEntry_internal(hashtable, slot, isnew, hash);
	Assert(entry == NULL || entry->hash == hash);

	MemoryContextSwitchTo(oldContext);

	return entry;
}

/*
 * If tuple is NULL, use the input slot instead. This convention avoids the
 * need to materialize virtual input tuples unless they actually need to get
 * copied into the table.
 *
 * Also, the caller must select an appropriate memory context for running
 * the hash functions. (dynahash.c doesn't change CurrentMemoryContext.)
 */
static uint32
VecTupleHashTableHash_internal(struct vec_tuplehash_hash *tb,
							const TupleTableSlot* slot)
{
	VecTupleHashTable hashtable = (VecTupleHashTable) tb->private_data;
	AggState *aggstate = (AggState *) hashtable->planstate;
	AggStatePerHash perhash = &aggstate->perhash[aggstate->current_set];
	uint32		hashkey = hashtable->hash_iv;
	int			i;
	VectorTupleSlot *vslot = (VectorTupleSlot *) slot;
	int row;

	Assert(slot != NULL);

	row = vslot->row_indexarr[0];

	for (i = 0; i < perhash->hashtable->numCols; i++)
	{
		Datum attr;
		bool isNull;

		int varno = perhash->hashGrpColIdxInput[i] - 1;
		vdatum *vec_value = (vdatum *) DatumGetPointer(slot->tts_values[varno]);

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		attr = VDATUM_DATUM(vec_value, row);
		isNull = VDATUM_ISNULL(vec_value, row);

		if (!isNull)			/* treat nulls as having hash key 0 */
		{
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1Coll(&perhash->hashfunctions[i],
													hashtable->tab_collations[i],
													attr));
			hashkey ^= hkey;
		}
	}

	/*
	 * The way hashes are combined above, among each other and with the IV,
	 * doesn't lead to good bit perturbation. As the IV's goal is to lead to
	 * achieve that, perform a round of hashing of the combined hash -
	 * resulting in near perfect perturbation.
	 */
	return murmurhash32(hashkey);
}



/*
 * Does the work of LookupTupleHashEntry and LookupTupleHashEntryHash. Useful
 * so that we can avoid switching the memory context multiple times for
 * LookupTupleHashEntry.
 *
 * NB: This function may or may not change the memory context. Caller is
 * expected to change it back.
 */
static inline VecTupleHashEntry
VecLookupTupleHashEntry_internal(VecTupleHashTable hashtable, TupleTableSlot *slot,
							  bool *isnew, uint32 hash)
{
	VecTupleHashEntryData *entry;
	bool		found;

	if (isnew)
	{
		entry = vec_tuplehash_insert_hash(hashtable->hashtab, slot, hash, &found);

		if (found)
		{
			/* found pre-existing entry */
			*isnew = false;
		}
		else
		{
			/* created new entry */
			*isnew = true;
			/* zero caller data */
			entry->additional = NULL;
		}
	}
	else
	{
		entry = vec_tuplehash_lookup_hash(hashtable->hashtab, slot, hash);
	}

	return entry;
}

static bool
VecTupleHashTableMatch(struct vec_tuplehash_hash *tb,
						const TupleTableSlot *slot1,
						const TupleTableSlot *slot2)
{
	VecTupleHashTable vhashtable = (VecTupleHashTable) tb->private_data;
	AggState *aggstate = (AggState *) vhashtable->planstate;
	AggStatePerHash perhash = &aggstate->perhash[aggstate->current_set];
	int i;
	MemoryContext oldctx;
	TupleTableSlot *first_slot = (TupleTableSlot *) slot1;
	VectorTupleSlot *vslot = (VectorTupleSlot *) slot2;
	int row = vslot->row_indexarr[0];

	oldctx = MemoryContextSwitchTo(vhashtable->tempcxt);

	for (i = 0; i < vhashtable->numCols; i++)
	{
		AttrNumber varno;
		vdatum *vec_value;
		Datum value2;
		FmgrInfo *fcinfo;
		Datum result;
		Oid colloid;
		bool is_vec = is_vec_type(slot2->tts_tupleDescriptor->attrs[i].atttypid);

		bool isnull1,isnull2;

		fcinfo = &vhashtable->eqfunctions[i];

		if (perhash->hashtable->tab_collations != NULL)
			colloid = perhash->hashtable->tab_collations[i];
		else
			colloid = DEFAULT_COLLATION_OID;
	
		varno = perhash->hashGrpColIdxInput[i] - 1;
		if (is_vec)
		{
			vec_value = (vdatum *) DatumGetPointer(slot2->tts_values[varno]);
			isnull2 = VDATUM_ISNULL(vec_value, row);
			value2 = VDATUM_DATUM(vec_value, row);
		}
		else
		{
			isnull2 = slot2->tts_isnull[varno];
			value2 = slot2->tts_values[varno];
		}

		isnull1 = first_slot->tts_isnull[i];
		if (unlikely(isnull1 != isnull2))
		{
			MemoryContextSwitchTo(oldctx);
			return false;
		}
		else if (isnull1)
		{
			continue;
		}

		result = FunctionCall2Coll(fcinfo, colloid,
						value2, first_slot->tts_values[varno]);

		if (!DatumGetBool(result))
		{
			MemoryContextSwitchTo(oldctx);
			return false;
		}
	}

	MemoryContextSwitchTo(oldctx);
	return true;
}

bool
gamma_vec_grouping_match(AggState *aggstate, int numCols,
						 const TupleTableSlot *slot,
						 int first_row, int cur_row)
{
	AggStatePerPhase phase = aggstate->phase;
	Agg *aggnode = phase->aggnode;
	int i;

	for (i = 0; i < numCols; i++)
	{
		vdatum *vec_value;
		FmgrInfo *fcinfo;
		Datum result;
		int varno;
		Oid colloid;

		Datum val1, val2;
		bool isnull1,isnull2;
		FmgrInfo *eqfunctions = (FmgrInfo *)phase->eqfunctions;
		bool is_vec = is_vec_type(slot->tts_tupleDescriptor->attrs[i].atttypid);

		/* */
		if (!is_vec)
			continue;

		fcinfo = (FmgrInfo *)&eqfunctions[i];
	
		if (aggnode != NULL && aggnode->grpCollations != NULL)
		{
			colloid = aggnode->grpCollations[i];
		}
		else
		{
			colloid = DEFAULT_COLLATION_OID;
		}

		varno = phase->aggnode->grpColIdx[i] - 1;
		vec_value = (vdatum *) DatumGetPointer(slot->tts_values[varno]);

		isnull1 = VDATUM_ISNULL(vec_value, first_row);
		isnull2 = VDATUM_ISNULL(vec_value, cur_row);
		if (unlikely(isnull1 != isnull2))
		{
			return false;
		}
		else if (isnull1)
		{
			continue;
		}

		val1 = VDATUM_DATUM(vec_value, first_row);
		val2 = VDATUM_DATUM(vec_value, cur_row);
		result = FunctionCall2Coll(fcinfo, colloid, val1, val2);

		if (!DatumGetBool(result))
		{
			return false;
		}
	}

	return true;
}

bool
gamma_vec_grouping_row_match(AggState *aggstate, int numCols,
						 const TupleTableSlot *rowslot,
						 const TupleTableSlot *vecslot, int cur_row)
{
	AggStatePerPhase phase = aggstate->phase;
	Agg *aggnode = phase->aggnode;
	int i;

	for (i = 0; i < numCols; i++)
	{
		vdatum *vec_value;
		FmgrInfo *fcinfo;
		Datum result;
		int varno;
		Oid colloid;

		Datum val1, val2;
		bool isnull1,isnull2;
		FmgrInfo *eqfunctions = (FmgrInfo *)phase->eqfunctions;
		bool is_vec = is_vec_type(vecslot->tts_tupleDescriptor->attrs[i].atttypid);

		fcinfo = (FmgrInfo *)&eqfunctions[i];
	
		if (aggnode != NULL && aggnode->grpCollations != NULL)
		{
			colloid = aggnode->grpCollations[i];
		}
		else
		{
			colloid = DEFAULT_COLLATION_OID;
		}

		varno = phase->aggnode->grpColIdx[i] - 1;
		vec_value = (vdatum *) DatumGetPointer(vecslot->tts_values[varno]);

		isnull1 = rowslot->tts_isnull[varno];
		isnull2 = is_vec ?
				VDATUM_ISNULL(vec_value, cur_row) : vecslot->tts_isnull[varno];
		if (unlikely(isnull1 != isnull2))
		{
			return false;
		}
		else if (isnull1)
		{
			continue;
		}

		val1 = rowslot->tts_values[varno];
		val2 = is_vec ?
				VDATUM_DATUM(vec_value, cur_row) : vecslot->tts_values[varno];

		result = FunctionCall2Coll(fcinfo, colloid, val1, val2);

		if (!DatumGetBool(result))
		{
			return false;
		}
	}

	return true;
}


/********************************* agg expr *********************************/

/*
 * Build transition/combine function invocation for a single transition
 * value. This is separated from ExecBuildAggTrans() because there are
 * multiple callsites (hash and sort in some grouping set cases).
 */
static void
VecExecBuildAggTransCall(ExprState *state, AggState *aggstate,
					  ExprEvalStep *scratch,
					  FunctionCallInfo fcinfo, AggStatePerTrans pertrans,
					  int transno, int setno, int setoff, bool ishash,
					  bool nullcheck)
{
	ExprContext *aggcontext;
	int			adjust_jumpnull = -1;

	if (ishash)
		aggcontext = aggstate->hashcontext;
	else
		aggcontext = aggstate->aggcontexts[setno];

	/* add check for NULL pointer? */
	if (nullcheck)
	{
		scratch->opcode = EEOP_AGG_PLAIN_PERGROUP_NULLCHECK;
		scratch->d.agg_plain_pergroup_nullcheck.setoff = setoff;
		/* adjust later */
		scratch->d.agg_plain_pergroup_nullcheck.jumpnull = -1;
		gamma_expr_eval_push_step(state, scratch);
		adjust_jumpnull = state->steps_len - 1;
	}

	/*
	 * For non-ordered aggregates: same as row engine;
	 * For ordered aggregates: do nothing for vectorized engine;
	 */
#if PG_VERSION_NUM < 160000
	if (pertrans->numSortCols == 0)
#else
	if (!pertrans->aggsortrequired)
#endif
	{
		if (pertrans->transtypeByVal)
		{
			if (fcinfo->flinfo->fn_strict &&
				pertrans->initValueIsNull)
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL;
			else if (fcinfo->flinfo->fn_strict)
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL;
			else
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_BYVAL;
		}
		else
		{
			if (fcinfo->flinfo->fn_strict &&
				pertrans->initValueIsNull)
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF;
			else if (fcinfo->flinfo->fn_strict)
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_STRICT_BYREF;
			else
				scratch->opcode = EEOP_AGG_PLAIN_TRANS_BYREF;
		}

		scratch->d.agg_trans.pertrans = pertrans;
		scratch->d.agg_trans.setno = setno;
		scratch->d.agg_trans.setoff = setoff;
		scratch->d.agg_trans.transno = transno;
		scratch->d.agg_trans.aggcontext = aggcontext;
		gamma_expr_eval_push_step(state, scratch);
	}

	/* fix up jumpnull */
	if (adjust_jumpnull != -1)
	{
		ExprEvalStep *as = &state->steps[adjust_jumpnull];

		Assert(as->opcode == EEOP_AGG_PLAIN_PERGROUP_NULLCHECK);
		Assert(as->d.agg_plain_pergroup_nullcheck.jumpnull == -1);
		as->d.agg_plain_pergroup_nullcheck.jumpnull = state->steps_len;
	}
}

/*
 * Build transition/combine function invocations for all aggregate transition
 * / combination function invocations in a grouping sets phase. This has to
 * invoke all sort based transitions in a phase (if doSort is true), all hash
 * based transitions (if doHash is true), or both (both true).
 *
 * The resulting expression will, for each set of transition values, first
 * check for filters, evaluate aggregate input, check that that input is not
 * NULL for a strict transition function, and then finally invoke the
 * transition for each of the concurrently computed grouping sets.
 *
 * If nullcheck is true, the generated code will check for a NULL pointer to
 * the array of AggStatePerGroup, and skip evaluation if so.
 */
void
VecExecBuildAggTrans(AggState *aggstate, bool nullcheck)
{
	int phaseidx;

	/*
	 * Build expressions doing all the transition work at once. We build a
	 * different one for each phase, as the number of transition function
	 * invocation can differ between phases. Note this'll work both for
	 * transition and combination functions (although there'll only be one
	 * phase in the latter case).
	 */
	for (phaseidx = 0; phaseidx < aggstate->numphases; phaseidx++)
	{
		AggStatePerPhase phase = &aggstate->phases[phaseidx];
		bool		dohash = false;
		bool		dosort = false;

		/* phase 0 doesn't necessarily exist */
		if (!phase->aggnode)
			continue;

		if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 1)
		{
			/*
			 * Phase one, and only phase one, in a mixed agg performs both
			 * sorting and aggregation.
			 */
			dohash = true;
			dosort = true;
		}
		else if (aggstate->aggstrategy == AGG_MIXED && phaseidx == 0)
		{
			/*
			 * No need to compute a transition function for an AGG_MIXED phase
			 * 0 - the contents of the hashtables will have been computed
			 * during phase 1.
			 */
			continue;
		}
		else if (phase->aggstrategy == AGG_PLAIN ||
				 phase->aggstrategy == AGG_SORTED)
		{
			dohash = false;
			dosort = true;
		}
		else if (phase->aggstrategy == AGG_HASHED)
		{
			dohash = true;
			dosort = false;
		}
		else
			Assert(false);

		phase->evaltrans = VecExecBuildAggTransPerPhase(aggstate, phase,
											 dosort, dohash,
											 false);

		/* cache compiled expression for outer slot without NULL check */
		phase->evaltrans_cache[0][0] = phase->evaltrans;
	}

}

ExprState *
VecExecBuildAggTransPerPhase(AggState *aggstate, AggStatePerPhase phase,
				  bool doSort, bool doHash, bool nullcheck)
{
	ExprState **result = NULL;
	int expr_count = 0;

	if (doSort)
		expr_count += aggstate->maxsets;

	if (doHash)
		expr_count += aggstate->num_hashes;
	
	result = (ExprState **) palloc0(sizeof(ExprState *) * expr_count);

	if (doSort)
	{
		int			processGroupingSets = Max(phase->numsets, 1);
		int			setoff = 0;

		for (int setno = 0; setno < processGroupingSets; setno++)
		{
			result[setoff] = VecExecBuildAggTransPerSet(aggstate, phase,
													setno, setoff, false, nullcheck);
			setoff++;
		}
	}

	if (doHash)
	{
		int			numHashes = aggstate->num_hashes;
		int			setoff;

		/* in MIXED mode, there'll be preceding transition values */
		if (aggstate->aggstrategy != AGG_HASHED)
			setoff = aggstate->maxsets;
		else
			setoff = 0;

		for (int setno = 0; setno < numHashes; setno++)
		{
			result[setoff] = VecExecBuildAggTransPerSet(aggstate, phase,
													setno, setoff, true, nullcheck);
			setoff++;
		}
	}

	return (ExprState *) result;
}

static ExprState *
VecExecBuildAggTransPerSet(AggState *aggstate, AggStatePerPhase phase,
				  int setno, int setoff, bool ishash, bool nullcheck)
{
	ExprState  *state = makeNode(ExprState);
	PlanState  *parent = &aggstate->ss.ps;
	ExprEvalStep scratch = {0};
	bool		isCombine = DO_AGGSPLIT_COMBINE(aggstate->aggsplit);
	ExprSetupInfo deform = {0, 0, 0, NIL};

	state->expr = (Expr *) aggstate;
	state->parent = parent;

	scratch.resvalue = &state->resvalue;
	scratch.resnull = &state->resnull;

	/*
	 * First figure out which slots, and how many columns from each, we're
	 * going to need.
	 */
	for (int transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];

		gamma_expr_setup_walker((Node *) pertrans->aggref->aggdirectargs,
						  &deform);
		gamma_expr_setup_walker((Node *) pertrans->aggref->args,
						  &deform);
		gamma_expr_setup_walker((Node *) pertrans->aggref->aggorder,
						  &deform);
		gamma_expr_setup_walker((Node *) pertrans->aggref->aggdistinct,
						  &deform);
		gamma_expr_setup_walker((Node *) pertrans->aggref->aggfilter,
						  &deform);
	}
	gamma_exec_push_expr_setup_steps(state, &deform);

	/*
	 * Emit instructions for each transition value / grouping set combination.
	 */
	for (int transno = 0; transno < aggstate->numtrans; transno++)
	{
		AggStatePerTrans pertrans = &aggstate->pertrans[transno];
		FunctionCallInfo trans_fcinfo = pertrans->transfn_fcinfo;
		List	   *adjust_bailout = NIL;
		NullableDatum *strictargs = NULL;
		bool	   *strictnulls = NULL;
		int			argno;
		ListCell   *bail;

		/*
		 * If filter present, emit. Do so before evaluating the input, to
		 * avoid potentially unneeded computations, or even worse, unintended
		 * side-effects.  When combining, all the necessary filtering has
		 * already been done.
		 */
		if (pertrans->aggref->aggfilter && !isCombine)
		{
			/* evaluate filter expression */
			gamma_exec_init_expr_rec(pertrans->aggref->aggfilter, state,
							&state->resvalue, &state->resnull);
			/* and jump out if false */
			scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
			scratch.d.jump.jumpdone = -1;	/* adjust later */
			gamma_expr_eval_push_step(state, &scratch);
			adjust_bailout = lappend_int(adjust_bailout,
										 state->steps_len - 1);
		}

		/*
		 * Evaluate arguments to aggregate/combine function.
		 */
		argno = 0;
		if (isCombine)
		{
			/*
			 * Combining two aggregate transition values. Instead of directly
			 * coming from a tuple the input is a, potentially deserialized,
			 * transition value.
			 */
			TargetEntry *source_tle;

			Assert(pertrans->numSortCols == 0);
			Assert(list_length(pertrans->aggref->args) == 1);

			strictargs = trans_fcinfo->args + 1;
			source_tle = (TargetEntry *) linitial(pertrans->aggref->args);

			/*
			 * deserialfn_oid will be set if we must deserialize the input
			 * state before calling the combine function.
			 */
			if (!OidIsValid(pertrans->deserialfn_oid))
			{
				/*
				 * Start from 1, since the 0th arg will be the transition
				 * value
				 */
				gamma_exec_init_expr_rec(source_tle->expr, state,
								&trans_fcinfo->args[argno + 1].value,
								&trans_fcinfo->args[argno + 1].isnull);
			}
			else
			{
				FunctionCallInfo ds_fcinfo = pertrans->deserialfn_fcinfo;

				/* evaluate argument */
				gamma_exec_init_expr_rec(source_tle->expr, state,
								&ds_fcinfo->args[0].value,
								&ds_fcinfo->args[0].isnull);

				/* Dummy second argument for type-safety reasons */
				ds_fcinfo->args[1].value = PointerGetDatum(NULL);
				ds_fcinfo->args[1].isnull = false;

				/*
				 * Don't call a strict deserialization function with NULL
				 * input
				 */
				if (pertrans->deserialfn.fn_strict)
					scratch.opcode = EEOP_AGG_STRICT_DESERIALIZE;
				else
					scratch.opcode = EEOP_AGG_DESERIALIZE;

				scratch.d.agg_deserialize.fcinfo_data = ds_fcinfo;
				scratch.d.agg_deserialize.jumpnull = -1;	/* adjust later */
				scratch.resvalue = &trans_fcinfo->args[argno + 1].value;
				scratch.resnull = &trans_fcinfo->args[argno + 1].isnull;

				gamma_expr_eval_push_step(state, &scratch);
				/* don't add an adjustment unless the function is strict */
				if (pertrans->deserialfn.fn_strict)
					adjust_bailout = lappend_int(adjust_bailout,
												 state->steps_len - 1);

				/* restore normal settings of scratch fields */
				scratch.resvalue = &state->resvalue;
				scratch.resnull = &state->resnull;
			}
			argno++;
		}
#if PG_VERSION_NUM < 160000
		else if (pertrans->numSortCols == 0)
#else
		else if (!pertrans->aggsortrequired)
#endif
		{
			ListCell   *arg;

			/*
			 * Normal transition function without ORDER BY / DISTINCT.
			 */
			strictargs = trans_fcinfo->args + 1;

			foreach(arg, pertrans->aggref->args)
			{
				TargetEntry *source_tle = (TargetEntry *) lfirst(arg);

				/*
				 * Start from 1, since the 0th arg will be the transition
				 * value
				 */
				gamma_exec_init_expr_rec(source_tle->expr, state,
								&trans_fcinfo->args[argno + 1].value,
								&trans_fcinfo->args[argno + 1].isnull);
				argno++;
			}
		}
#if 0
		else if (pertrans->numInputs == 1)
		{
			/*
			 * DISTINCT and/or ORDER BY case, with a single column sorted on.
			 */
			TargetEntry *source_tle =
			(TargetEntry *) linitial(pertrans->aggref->args);

			Assert(list_length(pertrans->aggref->args) == 1);

			ExecInitExprWrapper(parent, state, source_tle->expr,
					&state->resvalue,
					&state->resnull);
			//gamma_exec_init_expr_rec(source_tle->expr, state,
			//				&state->resvalue,
			//				&state->resnull);
			strictnulls = &state->resnull;
			argno++;
		}
#endif
		else
		{
			/*
			 * DISTINCT and/or ORDER BY case, with multiple columns sorted on.
			 */
			Datum	   *values = pertrans->sortslot->tts_values;
			bool	   *nulls = pertrans->sortslot->tts_isnull;
			ListCell   *arg;

			strictnulls = nulls;

			foreach(arg, pertrans->aggref->args)
			{
				TargetEntry *source_tle = (TargetEntry *) lfirst(arg);

				gamma_exec_init_expr_rec(source_tle->expr, state,
								&values[argno], &nulls[argno]);
				argno++;
			}
		}
		Assert(pertrans->numInputs == argno);

		/* for count(*) */
		if (pertrans->aggref->aggstar)
			continue;
		
#if PG_VERSION_NUM >= 160000
		if (!pertrans->aggsortrequired && pertrans->numDistinctCols > 0)
			continue;
#endif

		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue. This is true for both plain and
		 * sorted/distinct aggregates.
		 */
		if (trans_fcinfo->flinfo->fn_strict && pertrans->numTransInputs > 0)
		{
			if (strictnulls)
				scratch.opcode = EEOP_AGG_STRICT_INPUT_CHECK_NULLS;
			else
				scratch.opcode = EEOP_AGG_STRICT_INPUT_CHECK_ARGS;
			scratch.d.agg_strict_input_check.nulls = strictnulls;
			scratch.d.agg_strict_input_check.args = strictargs;
			scratch.d.agg_strict_input_check.jumpnull = -1; /* adjust later */
			scratch.d.agg_strict_input_check.nargs = pertrans->numTransInputs;
			gamma_expr_eval_push_step(state, &scratch);
			adjust_bailout = lappend_int(adjust_bailout,
										 state->steps_len - 1);
		}


		VecExecBuildAggTransCall(state, aggstate, &scratch, trans_fcinfo,
				pertrans, transno, setno, setoff, ishash,
				nullcheck);

		/* adjust early bail out jump target(s) */
		foreach(bail, adjust_bailout)
		{
			ExprEvalStep *as = &state->steps[lfirst_int(bail)];

			if (as->opcode == EEOP_JUMP_IF_NOT_TRUE)
			{
				Assert(as->d.jump.jumpdone == -1);
				as->d.jump.jumpdone = state->steps_len;
			}
			else if (as->opcode == EEOP_AGG_STRICT_INPUT_CHECK_ARGS ||
					 as->opcode == EEOP_AGG_STRICT_INPUT_CHECK_NULLS)
			{
				Assert(as->d.agg_strict_input_check.jumpnull == -1);
				as->d.agg_strict_input_check.jumpnull = state->steps_len;
			}
			else if (as->opcode == EEOP_AGG_STRICT_DESERIALIZE)
			{
				Assert(as->d.agg_deserialize.jumpnull == -1);
				as->d.agg_deserialize.jumpnull = state->steps_len;
			}
			else
				Assert(false);
		}
	}

	scratch.resvalue = NULL;
	scratch.resnull = NULL;
	scratch.opcode = EEOP_DONE;
	gamma_expr_eval_push_step(state, &scratch);

	gamma_exec_ready_expr(state);

	return state;
}

void
clear_nonvec_hash_tables(AggState *aggstate)
{
	int setno;

	for (setno = 0; setno < aggstate->num_hashes; ++setno)
	{
		AggStatePerHash perhash = &aggstate->perhash[setno];
		if (perhash->hashtable != NULL)
		{
			tuplehash_destroy(
				(tuplehash_hash *)((VecTupleHashTable)perhash->hashtable)->hashtab);
			perhash->hashtable = NULL;
		}
	}

	return;
}

void
gamma_vec_hashtable_grow(AggState *aggstate, int setno, int add_size)
{
	AggStatePerHash perhash = &aggstate->perhash[setno];
	VecTupleHashTable hashtable = (VecTupleHashTable)perhash->hashtable;
	vec_tuplehash_hash *tb = hashtable->hashtab;

	if (tb->members + add_size >= tb->grow_threshold)
	{
		//if (unlikely(tb->size + add_size > tb->size * 2))
		//	tb->size = tb->size + add_size;

		if (unlikely(tb->size == ( (((uint64) PG_UINT32_MAX) + 1))))
			sh_error("hash table size exceeded");

		vec_tuplehash_grow(tb, (tb->size + add_size) * 2);
	}

	return;
}

ExprState *
gamma_vec_init_interp_expr_proc()
{
	ExprState *state = makeNode(ExprState);
	ExprEvalStep scratch = {0};
	int i;

	for (i = 0; i < 5; i++)
	{
		scratch.resvalue = NULL;
		scratch.resnull = NULL;
		scratch.opcode = EEOP_DONE;
		gamma_expr_eval_push_step(state, &scratch);
	}

	gamma_exec_ready_expr(state);

	return state;
}
