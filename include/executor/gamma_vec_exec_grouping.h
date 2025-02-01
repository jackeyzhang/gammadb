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

#ifndef GAMMA_VEC_EXEC_GROUPING_H
#define GAMMA_VEC_EXEC_GROUPING_H

#include "postgres.h"

#include "utils/vdatum/vdatum.h"

typedef struct VecTupleHashEntryData
{
	/* TupleHashEntryData as a header */
	/*
	 * GAMMA NOTE: Replace first_slot with firstTuple, as both first_slot and
	 * firstTuple refer to pointers, so the memory layout of the Header
	 * remains unchanged.
	 */
	TupleTableSlot *first_slot; 
	void *additional;		/* user data */
	uint32 status;			/* hash status */
	uint32 hash;			/* hash value (cached) */

	/* Batch the tuple for advance_aggregates functions */
	short indexarr_dim;
	short *indexarr;
} VecTupleHashEntryData;

#define SH_PREFIX vec_tuplehash
#define SH_ELEMENT_TYPE VecTupleHashEntryData
#define SH_KEY_TYPE TupleTableSlot *
#define SH_SCOPE extern
#define SH_DECLARE

#define SH_GROW_MAX_MOVE VECTOR_SIZE
#define SH_GROW_MAX_DIB VECTOR_SIZE 
#define SH_GROW_MIN_FILLFACTOR 0.5

#include "utils/gamma_hash.h"

/*
 * Use InitTupleHashIterator/TermTupleHashIterator for a read/write scan.
 * Use ResetTupleHashIterator if the table can be frozen (in this case no
 * explicit scan termination is needed).
 */
#define VecInitTupleHashIterator(htable, iter) \
	vec_tuplehash_start_iterate((vec_tuplehash_hash *)htable->hashtab, iter)
#define VecTermTupleHashIterator(iter) \
	((void) 0)
#define VecResetTupleHashIterator(htable, iter) \
	VecInitTupleHashIterator(htable, iter)
#define VecScanTupleHashTable(htable, iter) \
	vec_tuplehash_iterate((vec_tuplehash_hash *)htable->hashtab, iter)

typedef struct VecTupleHashTableData
{
	vec_tuplehash_hash *hashtab;	/* underlying hash table */
	int			numCols;		/* number of columns in lookup key */
	AttrNumber *keyColIdx;		/* attr numbers of key columns */
	FmgrInfo   *tab_hash_funcs; /* hash functions for table datatype(s) */
	ExprState  *tab_eq_func;	/* comparator for table datatype(s) */
	Oid		   *tab_collations; /* collations for hash and comparison */
	MemoryContext tablecxt;		/* memory context containing table */
	MemoryContext tempcxt;		/* context for function evaluations */
	Size		entrysize;		/* actual size to make each hash entry */
	TupleTableSlot *tableslot;	/* slot for referencing table entries */
	/* The following fields are set transiently for each table search: */
	TupleTableSlot *inputslot;	/* current input tuple's slot */
	FmgrInfo   *in_hash_funcs;	/* hash functions for input datatype(s) */
	ExprState  *cur_eq_func;	/* comparator for input vs. table */
	uint32		hash_iv;		/* hash-function IV */
	ExprContext *exprcontext;	/* expression context */
	//VecTupleHashTableData header;

	PlanState *planstate;
	FmgrInfo   *eqfunctions;
} VecTupleHashTableData;

typedef struct VecTupleHashEntryData *VecTupleHashEntry;
typedef struct VecTupleHashTableData *VecTupleHashTable;

extern VecTupleHashTable VecBuildTupleHashTableExt(PlanState *parent,
					   TupleDesc inputDesc,
					   int numCols, AttrNumber *keyColIdx,
					   const Oid *eqfuncoids,
					   FmgrInfo *hashfunctions,
					   Oid *collations,
					   long nbuckets, Size additionalsize,
					   MemoryContext metacxt,
					   MemoryContext tablecxt,
					   MemoryContext tempcxt,
					   bool use_variable_hash_iv);
extern VecTupleHashTable VecBuildTupleHashTable(PlanState *parent,
					TupleDesc inputDesc,
					int numCols, AttrNumber *keyColIdx,
					const Oid *eqfuncoids,
					FmgrInfo *hashfunctions,
					Oid *collations,
					long nbuckets, Size additionalsize,
					MemoryContext tablecxt,
					MemoryContext tempcxt,
					bool use_variable_hash_iv);

extern void VecResetTupleHashTable(VecTupleHashTable hashtable);
extern void VecExecBuildAggTrans(AggState *aggstate, bool nullcheck);
extern ExprState * VecExecBuildAggTransPerPhase(AggState *aggstate, AggStatePerPhase phase,
				  bool doSort, bool doHash, bool nullcheck);
extern VecTupleHashEntry VecLookupTupleHashEntryHash(VecTupleHashTable hashtable,
							TupleTableSlot *slot, bool *isnew, uint32 hash);
extern void clear_nonvec_hash_tables(AggState *aggstate);
extern void gamma_vec_hashtable_grow(AggState *aggstate, int setno, int add_size);

extern FmgrInfo* vec_exec_tuples_match_prepare(int numCols, const Oid *eqOperators);
extern FmgrInfo* vec_exec_grouping_match_prepare(int numCols, const Oid *eqOperators);

extern bool gamma_vec_grouping_match(AggState *aggstate, int numCols,
						const TupleTableSlot *slot,
						int first_row, int cur_row);

extern bool gamma_vec_grouping_row_match(AggState *aggstate, int numCols,
						 const TupleTableSlot *rowslot,
						 const TupleTableSlot *vecslot, int cur_row);

#endif /* GAMMA_VEC_EXEC_GROUPING_H */
