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

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sdir.h"
#include "access/tupmacs.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/shmem.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "executor/gamma_merge.h"
#include "storage/gamma_meta.h"
#include "storage/gamma_rg.h"

bool gammadb_delta_table_merge_all = false;
static void gamma_merge_delete_tuple(Relation rel,
									 HeapTupleData *pin_tuples, int32 row);
static void gamma_merge_insert_index(Relation rel, HeapTupleData *pin_tuples,
									 uint32 rgid, int32 row);
static void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple);

void
gamma_merge_one_rowgroup(Relation rel, HeapTupleData *pin_tuples, uint32 rgid,
						bool *delbitmap, int rowcount)
{
	RowGroup *rg;

	rg = gamma_rg_build(rel);
	rg->rgid = rgid;
	gamma_fill_rowgroup(rel, pin_tuples, NULL, rg, rowcount);
	gamma_meta_insert_rowgroup(rel, rg);
	gamma_rg_free(rg);

	return;
}

void
gamma_merge(Relation rel)
{
	static HeapTupleData pin_tuples[GAMMA_COLUMN_VECTOR_SIZE];
	static Buffer pin_buffers[GAMMA_COLUMN_VECTOR_SIZE] = {0};

	TableScanDesc scan;
	HeapTuple	tup;
	Snapshot	snapshot;
	int32		row = 0;
	TupleTableSlot *slot;
	uint32 flags = SO_TYPE_SEQSCAN |SO_ALLOW_STRAT |
				   SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
	int i = 0;
	MemoryContext merge_context;
	MemoryContext old_context;

	merge_context = AllocSetContextCreate(CurrentMemoryContext,
										"Gamma Merge", ALLOCSET_DEFAULT_SIZES);

	LockRelation(rel, AccessExclusiveLock);
	slot = MakeTupleTableSlot(RelationGetDescr(rel), &TTSOpsBufferHeapTuple);

	/* transaction snapshot*/
	snapshot = RegisterSnapshot(GetTransactionSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL, NULL, flags);	

	/* backward scan */
	while (heap_getnextslot(scan, BackwardScanDirection, slot))
	{
		HeapScanDesc hscan = (HeapScanDesc) scan;
		Buffer buffer = hscan->rs_cbuf;
		BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *)slot;
		tup = bslot->base.tuple;

		memcpy(&pin_tuples[row], tup, sizeof(HeapTupleData));

		if (BufferIsValid(buffer) &&
				(pin_buffers[row] == 0 || pin_buffers[row - 1] != buffer))
		{
			if (BufferIsValid(pin_buffers[row]))
				ReleaseBuffer(pin_buffers[row]);
			pin_buffers[row] = buffer;
			IncrBufferRefCount(buffer);
		}

		row++;

		if (row >= GAMMA_COLUMN_VECTOR_SIZE)
		{
			uint32 rgid = gamma_meta_next_rgid(rel);
			old_context = MemoryContextSwitchTo(merge_context);
			gamma_merge_one_rowgroup(rel, pin_tuples, rgid, NULL, row);

			/* delete before inserting index */
			gamma_merge_delete_tuple(rel, pin_tuples, row);
			gamma_merge_insert_index(rel, pin_tuples, rgid, row);

			MemoryContextSwitchTo(old_context);
			MemoryContextReset(merge_context);

			/* reset the row */
			for(i = 0; i < row; i++)
			{
				if(BufferIsValid(pin_buffers[i]))
				{
					ReleaseBuffer(pin_buffers[i]);
					pin_buffers[i] = InvalidBuffer;
				}
			}

			row = 0;

		}
	}

	Assert(row < GAMMA_COLUMN_VECTOR_SIZE);

	if (gammadb_delta_table_merge_all && row > 0)
	{
		uint32 rgid = gamma_meta_next_rgid(rel);
		old_context = MemoryContextSwitchTo(merge_context);
		gamma_merge_one_rowgroup(rel, pin_tuples, rgid, NULL, row);

		/* delete before inserting index */
		gamma_merge_delete_tuple(rel, pin_tuples, row);
		gamma_merge_insert_index(rel, pin_tuples, rgid, row);

		MemoryContextSwitchTo(old_context);
		MemoryContextReset(merge_context);
	}

	/* reset the row */
	for(i = 0; i < row; i++)
	{
		if(BufferIsValid(pin_buffers[i]))
		{
			ReleaseBuffer(pin_buffers[i]);
			pin_buffers[i] = InvalidBuffer;
		}
	}

	heap_endscan(scan);
	UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	UnlockRelation(rel, AccessExclusiveLock);

	MemoryContextDelete(merge_context);
	return;
}


static void
gamma_merge_delete_tuple(Relation rel, HeapTupleData *pin_tuples, int32 row)
{
	int i;
	for (i = 0; i < row; i++)
		CatalogTupleDelete(rel, &pin_tuples[i].t_self);

	return;
}

static void
gamma_merge_insert_index(Relation rel, HeapTupleData *pin_tuples,
							uint32 rgid, int32 row)
{
	int i;
	CatalogIndexState indstate;

	indstate = CatalogOpenIndexes(rel);

	for (i = 0; i < row; i++)
	{
		gamma_meta_set_tid(&pin_tuples[i], rgid, i);
		CatalogIndexInsert(indstate, &pin_tuples[i]);
	}

	CatalogCloseIndexes(indstate);
}

/* GAMMA NOTE: these codes is copied from PostgreSQL
 *
 * CatalogIndexInsert - insert index entries for one catalog tuple
 *
 * This should be called for each inserted or updated catalog tuple.
 *
 * This is effectively a cut-down version of ExecInsertIndexTuples.
 */
static void
CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple)
{
	int			i;
	int			numIndexes;
	RelationPtr relationDescs;
	Relation	heapRelation;
	TupleTableSlot *slot;
	IndexInfo **indexInfoArray;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	/*
	 * HOT update does not require index inserts. But with asserts enabled we
	 * want to check that it'd be legal to currently insert into the
	 * table/index.
	 */
#ifndef USE_ASSERT_CHECKING
	if (HeapTupleIsHeapOnly(heapTuple))
		return;
#endif

	/*
	 * Get information from the state structure.  Fall out if nothing to do.
	 */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;
	relationDescs = indstate->ri_IndexRelationDescs;
	indexInfoArray = indstate->ri_IndexRelationInfo;
	heapRelation = indstate->ri_RelationDesc;

	/* Need a slot to hold the tuple being examined */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation),
									&TTSOpsHeapTuple);
	ExecStoreHeapTuple(heapTuple, slot, false);

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndexes; i++)
	{
		IndexInfo  *indexInfo;
		Relation	index;

		indexInfo = indexInfoArray[i];
		index = relationDescs[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/*
		 * Expressional and partial indexes on system catalogs are not
		 * supported, nor exclusion constraints, nor deferred uniqueness
		 */
		Assert(indexInfo->ii_Expressions == NIL);
		Assert(indexInfo->ii_Predicate == NIL);
		Assert(indexInfo->ii_ExclusionOps == NULL);
		Assert(index->rd_index->indimmediate);
		Assert(indexInfo->ii_NumIndexKeyAttrs != 0);

		/* see earlier check above */
#ifdef USE_ASSERT_CHECKING
		if (HeapTupleIsHeapOnly(heapTuple))
		{
			Assert(!ReindexIsProcessingIndex(RelationGetRelid(index)));
			continue;
		}
#endif							/* USE_ASSERT_CHECKING */

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   NULL,	/* no expression eval to do */
					   values,
					   isnull);

		/*
		 * The index AM does the rest.
		 */
		index_insert(index,		/* index relation */
					 values,	/* array of index Datums */
					 isnull,	/* is-null flags */
					 &(heapTuple->t_self),	/* tid of heap tuple */
					 heapRelation,
					 index->rd_index->indisunique ?
					 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 false,
					 indexInfo);
	}

	ExecDropSingleTupleTableSlot(slot);
}
