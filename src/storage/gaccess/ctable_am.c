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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "storage/lockdefs.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "executor/gamma_copy.h"
#include "executor/gamma_vec_tablescan.h"
#include "executor/vector_tuple_slot.h"
#include "storage/ctable_am.h"
#include "storage/ctable_dml.h"
#include "storage/ctable_vec_am.h"
#include "storage/gamma_buffer.h"
#include "storage/gamma_cvtable_am.h"
#include "storage/gamma_meta.h"
#include "storage/gamma_rg.h"

double gammadb_stats_analyze_tuple_factor = 0.01;

static const TupleTableSlotOps* ctable_slot_callbacks(Relation relation);
static TableScanDesc ctable_beginscan(Relation rel, Snapshot snapshot,
		int nkeys, struct ScanKeyData * key,
		ParallelTableScanDesc parallel_scan, uint32 flags);
static void ctable_endscan(TableScanDesc scan);
static void ctable_rescan(TableScanDesc scan, struct ScanKeyData * key,
		bool set_params, bool allow_strat, bool allow_sync,
		bool allow_pagemode);
static bool ctable_getnextslot(TableScanDesc scan, ScanDirection direction,
		TupleTableSlot * slot);
static Size ctable_parallelscan_estimate(Relation rel);
static Size ctable_parallelscan_initialize(Relation rel,
		ParallelTableScanDesc pscan);
static void ctable_parallelscan_reinitialize(Relation rel,
		ParallelTableScanDesc pscan);
static IndexFetchTableData* ctable_index_fetch_begin(Relation rel);
static void ctable_index_fetch_reset(IndexFetchTableData * sscan);
static void ctable_index_fetch_end(IndexFetchTableData * sscan);
static bool ctable_index_fetch_tuple(struct IndexFetchTableData * sscan,
		ItemPointer tid, Snapshot snapshot, TupleTableSlot * slot,
		bool * call_again, bool * all_dead);
static bool ctable_fetch_row_version(Relation relation, ItemPointer tid,
		Snapshot snapshot, TupleTableSlot *slot);
static void ctable_get_latest_tid(TableScanDesc sscan, ItemPointer tid);
static bool ctable_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static bool ctable_tuple_satisfies_snapshot(Relation rel, TupleTableSlot * slot,
		Snapshot snapshot);
static TransactionId ctable_index_delete_tuples(Relation rel,
		TM_IndexDeleteOp *delstate);
static void ctable_tuple_insert(Relation rel, TupleTableSlot * slot,
		CommandId cid, int options, struct BulkInsertStateData * bistate);
static void ctable_tuple_insert_speculative(Relation rel, TupleTableSlot * slot,
		CommandId cid, int options, struct BulkInsertStateData * bistate,
		uint32 specToken);
static void ctable_tuple_complete_speculative(Relation rel,
		TupleTableSlot * slot, uint32 specToken, bool succeeded);

static void ctable_multi_insert(Relation rel, TupleTableSlot ** slots,
		int ntuples, CommandId cid, int options,
		struct BulkInsertStateData * bistate);
static TM_Result ctable_tuple_delete(Relation rel, ItemPointer tip,
		CommandId cid, Snapshot snapshot, Snapshot crosscheck,
		bool wait, TM_FailureData *tmfd, bool changingPart);

#if PG_VERSION_NUM < 160000
static TM_Result ctable_tuple_update(Relation rel, ItemPointer otid,
		TupleTableSlot * slot, CommandId cid, Snapshot snapshot,
		Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
		LockTupleMode * lockmode, bool * update_indexes);
#else
static TM_Result ctable_tuple_update(Relation rel, ItemPointer otid,
		TupleTableSlot * slot, CommandId cid,
		Snapshot snapshot, Snapshot crosscheck,
		bool wait, TM_FailureData *tmfd,
		LockTupleMode * lockmode,
		TU_UpdateIndexes * update_indexes);
#endif

static TM_Result ctable_tuple_lock(Relation rel, ItemPointer tid,
		Snapshot snapshot, TupleTableSlot * slot, CommandId cid,
		LockTupleMode mode, LockWaitPolicy wait_policy,
		uint8 flags, TM_FailureData *tmfd);
static void ctable_finish_bulk_insert(Relation rel, int options);

#if PG_VERSION_NUM < 160000
static void ctable_set_new_filenode(Relation rel,
		const RelFileNode *newrnode, char persistence,
		TransactionId * freezeXid, MultiXactId * minmulti);
static void ctable_copy_data(Relation rel, const RelFileNode * newrnode);
#else
static void ctable_set_new_filelocator(Relation rel,
		const RelFileLocator *newrlocator, char persistence,
		TransactionId * freezeXid, MultiXactId * minmulti);
static void ctable_copy_data(Relation rel, const RelFileLocator * newrlocator);
#endif

static void ctable_nontransactional_truncate(Relation rel);
static void ctable_copy_for_cluster(Relation OldHeap, Relation NewHeap,
		Relation OldIndex, bool use_sort,
		TransactionId OldestXmin, TransactionId * xid_cutoff,
		MultiXactId * multi_cutoff,
		double * num_tuples, double * tups_vacuumed, double * tups_recently_dead);
static void ctable_vacuum_relation(Relation rel, VacuumParams * params,
		BufferAccessStrategy bstrategy);
#if PG_VERSION_NUM < 170000
static bool ctable_scan_analyze_next_block(TableScanDesc scan,
		BlockNumber blockno, BufferAccessStrategy bstrategy);
#else
static bool ctable_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream);
#endif
static bool ctable_scan_analyze_next_tuple(TableScanDesc scan,
		TransactionId OldestXmin, double * liverows, double * deadrows,
		TupleTableSlot * slot);
static double ctable_index_build_range_scan(Relation columnarRelation,
		Relation indexRelation, IndexInfo * indexInfo,
		bool allow_sync, bool anyvisible, bool progress,
		BlockNumber start_blockno, BlockNumber numblocks,
		IndexBuildCallback callback,
		void * callback_state, TableScanDesc scan);
static void ctable_index_validate_scan(Relation columnarRelation,
		Relation indexRelation, IndexInfo * indexInfo, Snapshot snapshot,
		ValidateIndexState * validateIndexState);
static bool ctable_relation_needs_toast_table(Relation rel);
static uint64 ctable_relation_size(Relation rel, ForkNumber forkNumber);
static void ctable_estimate_rel_size(Relation rel, int32 * attr_widths,
		BlockNumber * pages, double *tuples, double * allvisfrac);
static bool ctable_scan_sample_next_block(TableScanDesc scan,
		SampleScanState * scanstate);
static bool ctable_scan_sample_next_tuple(TableScanDesc scan,
		SampleScanState * scanstate, TupleTableSlot * slot);
static Oid ctable_relation_toast_am(Relation rel);
static void RelationTruncateIndexes(Relation heapRelation);

static const TableAmRoutine ctable_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = ctable_slot_callbacks,

	.scan_begin = ctable_beginscan,
	.scan_end = ctable_endscan,
	.scan_rescan = ctable_rescan,
	.scan_getnextslot = ctable_getnextslot,

	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,

	.parallelscan_estimate = ctable_parallelscan_estimate,
	.parallelscan_initialize = ctable_parallelscan_initialize,
	.parallelscan_reinitialize = ctable_parallelscan_reinitialize,

	.index_fetch_begin = ctable_index_fetch_begin,
	.index_fetch_reset = ctable_index_fetch_reset,
	.index_fetch_end = ctable_index_fetch_end,
	.index_fetch_tuple = ctable_index_fetch_tuple,

	.tuple_fetch_row_version = ctable_fetch_row_version,
	.tuple_tid_valid = ctable_tuple_tid_valid,
	.tuple_get_latest_tid = ctable_get_latest_tid,

	.tuple_satisfies_snapshot = ctable_tuple_satisfies_snapshot,
	.index_delete_tuples = ctable_index_delete_tuples,


	.tuple_insert = ctable_tuple_insert,
	.tuple_insert_speculative = ctable_tuple_insert_speculative,
	.tuple_complete_speculative = ctable_tuple_complete_speculative,
	.multi_insert = ctable_multi_insert,
	.tuple_delete = ctable_tuple_delete,
	.tuple_update = ctable_tuple_update,
	.tuple_lock = ctable_tuple_lock,
	.finish_bulk_insert = ctable_finish_bulk_insert,
#if PG_VERSION_NUM < 160000
	.relation_set_new_filenode = ctable_set_new_filenode,
#else
	.relation_set_new_filelocator = ctable_set_new_filelocator,

#endif
	.relation_nontransactional_truncate = ctable_nontransactional_truncate,
	.relation_copy_data = ctable_copy_data,
	.relation_copy_for_cluster = ctable_copy_for_cluster,
	.relation_vacuum = ctable_vacuum_relation,
	.scan_analyze_next_block = ctable_scan_analyze_next_block,
	.scan_analyze_next_tuple = ctable_scan_analyze_next_tuple,
	.index_build_range_scan = ctable_index_build_range_scan,
	.index_validate_scan = ctable_index_validate_scan,

	.relation_size = ctable_relation_size,
	.relation_needs_toast_table = ctable_relation_needs_toast_table,
	.relation_toast_am = ctable_relation_toast_am,
	.relation_fetch_toast_slice = NULL,

	.relation_estimate_size = ctable_estimate_rel_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = ctable_scan_sample_next_block,
	.scan_sample_next_tuple = ctable_scan_sample_next_tuple
};

const TableAmRoutine *
ctable_tableam_routine(void)
{
	return &ctable_am_methods;
}

PG_FUNCTION_INFO_V1(ctable_handler);
Datum
ctable_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&ctable_am_methods);
}

static const TupleTableSlotOps *
ctable_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static TableScanDesc 
ctable_beginscan(Relation rel, Snapshot snapshot, int nkeys,
		struct ScanKeyData * key,
		ParallelTableScanDesc parallel_scan,
		uint32 flags) 
{
	return vec_ctable_beginscan(rel, snapshot, nkeys, key, parallel_scan, flags);
}


static void
ctable_endscan(TableScanDesc scan)
{
	vec_ctable_end_scan(scan);
	return;
}

static void
ctable_rescan(TableScanDesc scan, struct ScanKeyData * key,
		bool set_params, bool allow_strat, bool allow_sync,
		bool allow_pagemode)
{
	vec_ctable_rescan(scan, key, set_params, allow_strat,
							allow_sync, allow_pagemode);
	return;
}

static bool
ctable_getnextslot(TableScanDesc scan, ScanDirection direction,
		TupleTableSlot * slot)
{
	CTableScanDesc cscan = (CTableScanDesc) scan;
	CVScanDesc cvscan = cscan->cvscan;
	HeapScanDesc hscan = cscan->hscan;

refetch:

	/* return the last batch. */
	if (cscan->scan_over)
	{
		ExecClearTuple(slot);
		return false;
	}

	if (!cscan->heap)
	{
		bool fetch = true;
		if (cvscan->offset >= cvscan->rg->dim)
		{
			cvscan->offset = 0;
			fetch = cvtable_loadnext_rg(cvscan, direction);
		}

		if (!fetch)
		{
			cscan->heap = true;
		}
		else
		{
			if (RGHasDelBitmap(cvscan->rg))
			{
				while (cvscan->rg->delbitmap[cvscan->offset])
				{
					cvscan->offset++;
					continue;
				}
			}

			if (cvscan->offset >= cvscan->rg->dim)
				goto refetch;

			cvscan->offset += tts_slot_from_rg(slot, cvscan->rg,
											cvscan->bms_proj, cvscan->offset);
			return true;
		}
	}

	/* begin scan delta table */
	Assert(cscan->heap);

	if (cscan->heap)
	{
		if (heap_getnextslot((TableScanDesc)hscan, direction, cscan->buf_slot))
		{
			slot_getallattrs(cscan->buf_slot);
			tts_slot_copy_values(slot, cscan->buf_slot);
			slot->tts_tid = cscan->buf_slot->tts_tid; /* keep the tid */
			return true;
		}
		else
		{
			cscan->scan_over = true;
			ExecClearTuple(slot);
			return false;
		}
	}

	return false;
}


static Size
ctable_parallelscan_estimate(Relation rel)
{
	return sizeof(VecParallelTableScanDescData);
}

static Size
ctable_rowgroup_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	RowGroupCtableScanDesc pdata = (RowGroupCtableScanDesc)((char *)pscan) +
										sizeof(ParallelBlockTableScanDescData);

	pg_atomic_init_u32(&pdata->cur_rg_id, 0);;
	pg_atomic_init_u32(&pdata->max_rg_id, gamma_meta_max_rgid(rel));

	return sizeof(VecParallelTableScanDescData);
}

static Size
ctable_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	table_block_parallelscan_initialize(rel, pscan);
	ctable_rowgroup_parallelscan_initialize(rel, pscan);

	return sizeof(VecParallelTableScanDescData);
}


static void
ctable_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	RowGroupCtableScanDesc pdata = (RowGroupCtableScanDesc)((char *)pscan) +
										sizeof(ParallelBlockTableScanDescData);

	table_block_parallelscan_reinitialize(rel, pscan);
	pg_atomic_init_u32(&pdata->cur_rg_id, 0);;
}

static IndexFetchTableData *
ctable_index_fetch_begin(Relation rel)
{
	Oid delta_oid = gamma_meta_get_delta_table_rel(rel);
	Relation delta_rel = table_open(delta_oid, AccessShareLock);
	CIndexFetchCTableData *scan = palloc0(sizeof(CIndexFetchCTableData));
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	scan->base.xs_base.rel = rel;
	scan->base.xs_cbuf = InvalidBuffer;

	scan->delta_scan = (IndexFetchHeapData *)heapam_routine->index_fetch_begin(delta_rel);

	scan->heapslot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
												&TTSOpsBufferHeapTuple);

	return (IndexFetchTableData *)scan;
}


static void
ctable_index_fetch_reset(IndexFetchTableData * sscan)
{
}


static void
ctable_index_fetch_end(IndexFetchTableData * sscan)
{
	CIndexFetchCTableData *scan = (CIndexFetchCTableData *)sscan;
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) sscan;
	IndexFetchTableData *delta_scan = (IndexFetchTableData *)scan->delta_scan;
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();

	if (BufferIsValid(hscan->xs_cbuf))
	{
		ReleaseBuffer(hscan->xs_cbuf);
		hscan->xs_cbuf = InvalidBuffer;
	}

	/* close the delta table */
	if (scan->delta_scan != NULL)
	{
		table_close(scan->delta_scan->xs_base.rel, NoLock);
		heapam_routine->index_fetch_end(delta_scan);
	}

	ExecDropSingleTupleTableSlot(scan->heapslot);

	pfree(sscan);
}


static bool
ctable_index_fetch_tuple(struct IndexFetchTableData * sscan,
		ItemPointer tid,
		Snapshot snapshot,
		TupleTableSlot * slot,
		bool * call_again, bool * all_dead)
{
	CIndexFetchCTableData *scan = (CIndexFetchCTableData *) sscan;
	Relation rel = scan->base.xs_base.rel;
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	bool found = false;

	ExecClearTuple(slot);

	/* the tuple is in delta table */
	if (!gamma_meta_tid_is_columnar(tid))
	{
		IndexFetchTableData *delta_scan = (IndexFetchTableData *)scan->delta_scan;
		ExecClearTuple(scan->heapslot);
		found = heapam_routine->index_fetch_tuple(delta_scan, tid, snapshot,
										scan->heapslot, call_again, all_dead);

		if (found)
		{
			slot_getallattrs(scan->heapslot);
			tts_slot_copy_values(slot, scan->heapslot);
			slot->tts_tid = scan->heapslot->tts_tid;
		}

		return found;
	}

	/* the tuple is in columnar part */

	*call_again = false; /* HOT is not supported by columnar tables */

	//TODO: check...
	if (all_dead)
	{
		*all_dead = false;
	}

	if (scan->indexonlyscan)
	{
		return gamma_rg_check_visible(rel, snapshot, tid);
	}

	return gamma_rg_fetch_slot(rel, snapshot, tid, slot, scan->bms_proj);

}

static bool
ctable_fetch_row_version(Relation relation,
		ItemPointer tid,
		Snapshot snapshot,
		TupleTableSlot *slot)
{
	/* the tuple is in delta table */
	if (!gamma_meta_tid_is_columnar(tid))
	{
		TupleTableSlot *tempslot;
		BufferHeapTupleTableSlot *btempslot;
		Buffer		buffer;
		Oid delta_oid;
		Relation delta_rel;

		delta_oid = gamma_meta_get_delta_table_rel(relation);
		delta_rel = table_open(delta_oid, AccessShareLock);

		ExecClearTuple(slot);
		tempslot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor,
											&TTSOpsBufferHeapTuple);
		btempslot = (BufferHeapTupleTableSlot *)tempslot;
		btempslot->base.tupdata.t_self = *tid;
		if (heap_fetch(relation, snapshot, &btempslot->base.tupdata, &buffer, false))
		{
			/* store in slot, transferring existing pin */
			ExecForceStoreHeapTuple(&btempslot->base.tupdata, slot, false);
			ExecStorePinnedBufferHeapTuple(&btempslot->base.tupdata, tempslot, buffer);
			slot->tts_tableOid = RelationGetRelid(relation);

			ExecDropSingleTupleTableSlot(tempslot);
			table_close(delta_rel, NoLock);
			return true;
		}

		table_close(delta_rel, NoLock);
		ExecDropSingleTupleTableSlot(tempslot);
		return false;
	}
	else
	{
		return gamma_rg_fetch_slot(relation, snapshot, tid, slot, NULL);
	}
}


static void
ctable_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
	elog(ERROR, "ctable_get_latest_tid not implemented");
}


static bool
ctable_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "ctable_tuple_tid_valid not implemented");
}


static bool
ctable_tuple_satisfies_snapshot(Relation rel, TupleTableSlot * slot,
		Snapshot snapshot)
{
	elog(ERROR, "ctable_tuple_satisfies_snapshot not implemented");
}

static TransactionId
ctable_index_delete_tuples(Relation rel,
		TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "ctable_index_delete_tuples not implemented");
}


static void
ctable_tuple_insert(Relation rel, TupleTableSlot * slot, CommandId cid,
		int options, struct BulkInsertStateData * bistate)
{
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	Oid delta_oid;
	Relation delta_rel;

	delta_oid = gamma_meta_get_delta_table_rel(rel);
	delta_rel = table_open(delta_oid, AccessShareLock);
	heapam_routine->tuple_insert(delta_rel, slot, cid, options, bistate);
	table_close(delta_rel, NoLock);
	return;
}

static void 
ctable_tuple_insert_speculative(Relation rel, TupleTableSlot * slot,
		CommandId cid, int options,
		struct BulkInsertStateData * bistate,
		uint32 specToken)
{
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	Oid delta_oid;
	Relation delta_rel;

	delta_oid = gamma_meta_get_delta_table_rel(rel);
	delta_rel = table_open(delta_oid, AccessShareLock);
	heapam_routine->tuple_insert_speculative(rel, slot, cid, options,
													bistate, specToken);
	table_close(delta_rel, NoLock);
	return;
}


static void
ctable_tuple_complete_speculative(Relation rel, TupleTableSlot * slot,
		uint32 specToken, bool succeeded)
{
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	Oid delta_oid;
	Relation delta_rel;

	delta_oid = gamma_meta_get_delta_table_rel(rel);
	delta_rel = table_open(delta_oid, AccessShareLock);
	heapam_routine->tuple_complete_speculative(rel, slot, specToken, succeeded);
	table_close(delta_rel, NoLock);
}

static void
ctable_multi_insert(Relation rel, TupleTableSlot ** slots, int ntuples,
		CommandId cid, int options,
		struct BulkInsertStateData * bistate)
{
	const TableAmRoutine *heapam_routine = GetHeapamTableAmRoutine();
	if (gammadb_copy_to_cvtable)
	{
		gamma_copy_collect_and_merge(rel, slots, ntuples, cid, options, bistate);
	}
	else
	{
		Oid delta_oid;
		Relation delta_rel;

		delta_oid = gamma_meta_get_delta_table_rel(rel);
		delta_rel = table_open(delta_oid, AccessShareLock);
		heapam_routine->multi_insert(rel, slots, ntuples, cid, options, bistate);
		table_close(delta_rel, NoLock);
	}
	return;
}


static TM_Result 
ctable_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
		Snapshot snapshot, Snapshot crosscheck,
		bool wait, TM_FailureData *tmfd,
		bool changingPart)
{
	return ctable_delete(rel, tid, cid, snapshot, crosscheck, wait,
			tmfd, changingPart);
}

#if PG_VERSION_NUM < 160000
static TM_Result
ctable_tuple_update(Relation rel, ItemPointer otid,
		TupleTableSlot * slot, CommandId cid,
		Snapshot snapshot, Snapshot crosscheck,
		bool wait, TM_FailureData *tmfd,
		LockTupleMode * lockmode,
		bool * update_indexes)
#else
static TM_Result
ctable_tuple_update(Relation rel, ItemPointer otid,
		TupleTableSlot * slot, CommandId cid,
		Snapshot snapshot, Snapshot crosscheck,
		bool wait, TM_FailureData *tmfd,
		LockTupleMode * lockmode,
		TU_UpdateIndexes * update_indexes)
#endif
{
	bool		shouldFree = true;
	HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	TM_Result	result;

	/* Update the tuple with table oid */
	slot->tts_tableOid = RelationGetRelid(rel);
	tuple->t_tableOid = slot->tts_tableOid;

	result = ctable_update(rel, otid, tuple, cid, snapshot, crosscheck, wait,
						 tmfd, lockmode);
	ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

	*update_indexes = result == TM_Ok && !HeapTupleIsHeapOnly(tuple);

	if (shouldFree)
		pfree(tuple);

	return result;
}


static TM_Result
ctable_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
		TupleTableSlot * slot, CommandId cid,
		LockTupleMode mode, LockWaitPolicy wait_policy,
		uint8 flags, TM_FailureData *tmfd)
{
	ereport(ERROR, (errmsg("ctable_tuple_lock is not implemented")));
}


static void
ctable_finish_bulk_insert(Relation rel, int options)
{
	if (gammadb_copy_to_cvtable)
		gamma_copy_finish_collect(rel, options);
	return;
}

#if PG_VERSION_NUM < 160000
static void
ctable_set_new_filenode(Relation rel,
		const RelFileNode *newrnode,
		char persistence,
		TransactionId * freezeXid,
		MultiXactId * minmulti)
{
	SMgrRelation srel;
	Oid cvrelid;

	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Unlogged columnar tables are not supported")));
	}

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	/* 1. create file node */
	srel = RelationCreateStorage(*newrnode, persistence, true);
	smgrclose(srel);

	/* 2. create the cv table to storage vector datas (NOTE:use heapam) */
	cvrelid = gamma_meta_get_cv_table_rel(rel);
	if (OidIsValid(cvrelid))
	{
		gamma_meta_truncate_cvtable(cvrelid);
		gamma_buffer_invalid_rel(RelationGetRelid(rel)); /* Oid of base rel*/
	}
	else
	{
		gamma_meta_delta_table(rel, (Datum)0);
		gamma_meta_cv_table(rel, (Datum)0);
	}
}

static void
ctable_copy_data(Relation rel, const RelFileNode * newrnode)
{
	elog(ERROR, "ctable_copy_data not implemented");
}

#else

static void
ctable_set_new_filelocator(Relation rel,
		const RelFileLocator *newrlocator,
		char persistence,
		TransactionId * freezeXid,
		MultiXactId * minmulti)
{
	SMgrRelation srel;
	Oid cvrelid;

	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Unlogged columnar tables are not supported")));
	}

	if (rel->rd_locator.relNumber != newrlocator->relNumber)
	{
		cvrelid = gamma_meta_get_cv_table_rel(rel);
		if (OidIsValid(cvrelid))
		{
			heap_drop_with_catalog(cvrelid);
			CommandCounterIncrement();
		}
	}

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	/* 1. create file node */
	srel = RelationCreateStorage(*newrlocator, persistence, true);
	smgrclose(srel);

	/* 2. create the cv table to storage vector datas (NOTE:use heapam) */
	cvrelid = gamma_meta_get_cv_table_rel(rel);
	if (OidIsValid(cvrelid))
	{
		gamma_meta_truncate_cvtable(cvrelid);
		gamma_buffer_invalid_rel(RelationGetRelid(rel)); /* Oid of base rel */
	}
	else
	{
		gamma_meta_delta_table(rel, (Datum)0);
		gamma_meta_cv_table(rel, (Datum)0);
	}
}

static void
ctable_copy_data(Relation rel, const RelFileLocator * newrlocator)
{
	elog(ERROR, "ctable_copy_data not implemented");
}

#endif


/* 
 * copy from Postgres
 *
 * RelationTruncateIndexes - truncate all indexes associated
 * with the heap relation to zero tuples.
 *
 * The routine will truncate and then reconstruct the indexes on
 * the specified relation.  Caller must hold exclusive lock on rel.
 */
static void
RelationTruncateIndexes(Relation heapRelation)
{
	ListCell   *indlist;

	/* Ask the relcache to produce a list of the indexes of the rel */
	foreach(indlist, RelationGetIndexList(heapRelation))
	{
		Oid			indexId = lfirst_oid(indlist);
		Relation	currentIndex;
		IndexInfo  *indexInfo;

		/* Open the index relation; use exclusive lock, just to be sure */
		currentIndex = index_open(indexId, AccessExclusiveLock);

		/*
		 * Fetch info needed for index_build.  Since we know there are no
		 * tuples that actually need indexing, we can use a dummy IndexInfo.
		 * This is slightly cheaper to build, but the real point is to avoid
		 * possibly running user-defined code in index expressions or
		 * predicates.  We might be getting invoked during ON COMMIT
		 * processing, and we don't want to run any such code then.
		 */
		indexInfo = BuildDummyIndexInfo(currentIndex);

		/*
		 * Now truncate the actual file (and discard buffers).
		 */
		RelationTruncate(currentIndex, 0);

		/* Initialize the index and rebuild */
		/* Note: we do not need to re-establish pkey setting */
		index_build(heapRelation, currentIndex, indexInfo, true, false);

		/* We're done with this index */
		index_close(currentIndex, NoLock);
	}
}

static void
ctable_nontransactional_truncate(Relation rel)
{
	Oid	toastrelid;
	Oid cvrelid;

	Oid delta_oid;
	Relation delta_rel;

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return;

	RelationTruncate(rel, 0);
	RelationTruncateIndexes(rel);

	/* If there is a toast table, truncate that too */
	toastrelid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		Relation toastrel = table_open(toastrelid, AccessExclusiveLock);

		table_relation_nontransactional_truncate(toastrel);
		RelationTruncateIndexes(toastrel);
		/* keep the lock... */
		table_close(toastrel, NoLock);
	}

	/* truncate cv table */
	cvrelid = gamma_meta_get_cv_table_rel(rel);
	if (OidIsValid(cvrelid))
	{
		gamma_meta_truncate_cvtable(cvrelid);
		gamma_buffer_invalid_rel(RelationGetRelid(rel)); /* Oid of base rel */
	}

	/* truncate delta table */
	delta_oid = gamma_meta_get_delta_table_rel(rel);
	delta_rel = table_open(delta_oid, AccessShareLock);
	table_relation_nontransactional_truncate(delta_rel);
	table_close(delta_rel, NoLock);
}

static void
ctable_copy_for_cluster(Relation OldHeap, Relation NewHeap,
		Relation OldIndex, bool use_sort,
		TransactionId OldestXmin,
		TransactionId * xid_cutoff,
		MultiXactId * multi_cutoff,
		double * num_tuples,
		double * tups_vacuumed,
		double * tups_recently_dead)
{

	elog(ERROR, "ctable_copy_for_cluster not implemented");
}


static void
ctable_vacuum_relation(Relation rel, VacuumParams * params,
		BufferAccessStrategy bstrategy)
{
	ctable_vacuum_rel(rel, params, bstrategy);
}

#if PG_VERSION_NUM < 170000
static bool
ctable_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
		BufferAccessStrategy bstrategy)
{
	/* The rowgroup part is not block-based, so there is nothing to do here. */
	return true;
}
#else

static bool gamma_block_end = false;
static bool
ctable_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	if (gamma_block_end)
	{
		gamma_block_end = false;
		return false;
	}
	return true;
}
#endif

static bool
ctable_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
		double * liverows, double * deadrows,
		TupleTableSlot * slot)
{
	/* sampler 5% */
	Snapshot snapshot = GetTransactionSnapshot();
	CTableScanDesc cscan = (CTableScanDesc) scan;
	CVScanDesc cvscan = cscan->cvscan;
	HeapScanDesc hscan = cscan->hscan;

	cscan->base.rs_snapshot = snapshot;
	cvscan->snapshot = snapshot;
	hscan->rs_base.rs_snapshot = snapshot;

	while (ctable_getnextslot(scan, ForwardScanDirection, slot))
	{
		double factor = 0.0;
		(*liverows)++;

		factor = ((double)(random() % 100000)) / (double) 100000;
		if (factor > gammadb_stats_analyze_tuple_factor)
		{
			slot = ExecClearTuple(slot);
			continue;
		}

		return true;
	}

#if PG_VERSION_NUM >= 170000
	gamma_block_end = true;
#endif

	return false;
}


static double
ctable_index_build_range_scan(Relation rel,
		Relation indexRelation,
		IndexInfo * indexInfo,
		bool allow_sync,
		bool anyvisible,
		bool progress,
		BlockNumber start_blockno,
		BlockNumber numblocks,
		IndexBuildCallback callback,
		void * callback_state,
		TableScanDesc scan)
{
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	TransactionId oldest_xmin = InvalidTransactionId;
	Snapshot snapshot;
	BlockNumber previous_blkno = InvalidBlockNumber;
	double reltuples = 0;
	bool need_unregister_snapshot = false;

	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Only btree index is supported")));
	}

	if (scan != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel scans are not supported for columnar index")));
	}

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(rel, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);


	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
	{
		/* ignore lazy VACUUM's */
		oldest_xmin = GetOldestNonRemovableTransactionId(rel);
	}

	if (!TransactionIdIsValid(oldest_xmin))
	{

		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		need_unregister_snapshot = true;
	}
	else
	{
		snapshot = SnapshotAny;
	}

	scan = table_beginscan_strat(rel, snapshot, 0, NULL, true, allow_sync);

	if (progress)
	{
		//TODO; blocks
	}

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		Datum indexValues[INDEX_MAX_KEYS];
		bool indexNulls[INDEX_MAX_KEYS];

		if (progress)
		{
			BlockNumber blocks_done = gamma_meta_ptid_get_rgid(&slot->tts_tid);

			if (blocks_done != previous_blkno)
			{
				//TODO:blocks
				previous_blkno = blocks_done;
			}
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (predicate != NULL && !ExecQual(predicate, econtext))
			continue;

		FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

		callback(indexRelation, &slot->tts_tid, indexValues, indexNulls,
				true/*TODO:check*/, callback_state);

		reltuples++;
	}

	table_endscan(scan);

	if (progress)
	{
		//TODO:
	}

	if (need_unregister_snapshot)
	{
		UnregisterSnapshot(snapshot);
	}

	ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
	FreeExecutorState(estate);

	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NULL;

	return reltuples;
}

static void
ctable_index_validate_scan(Relation columnarRelation,
		Relation indexRelation,
		IndexInfo * indexInfo,
		Snapshot snapshot,
		ValidateIndexState * validateIndexState)
{
	elog(ERROR, "ctable_index_validate_scan not implemented");
}

static bool
ctable_relation_needs_toast_table(Relation rel)
{ 
	/* deltatable part need toast table */
	return true;
}

static uint64 
ctable_relation_size(Relation rel, ForkNumber forkNumber)
{
	int all_width = 0;
	BlockNumber pages;

	Oid cv_rel_oid = gamma_meta_get_cv_table_rel(rel);
	Relation cv_rel = table_open(cv_rel_oid, AccessShareLock);

	Oid delta_oid = gamma_meta_get_delta_table_rel(rel);
	Relation delta_rel = table_open(delta_oid, AccessShareLock);

	uint64 rows = cvtable_get_rows(cv_rel);
	pages = table_relation_size(delta_rel, MAIN_FORKNUM);

	all_width = get_rel_data_width(delta_rel, NULL);

	/* treat Column Vector as a page */
	pages += (rows * (all_width + sizeof(HeapTupleHeaderData)) / BLCKSZ);

	table_close(cv_rel, AccessShareLock);
	table_close(delta_rel, AccessShareLock);
	return pages;
}

static void
ctable_estimate_rel_size(Relation rel, int32 * attr_widths,
		BlockNumber * pages, double *tuples,
		double * allvisfrac)
{
	Oid delta_oid;
	Relation delta_rel;
	int all_width = 0;
	uint64 rows;

	Oid cv_rel_oid = gamma_meta_get_cv_table_rel(rel);
	Relation cv_rel = table_open(cv_rel_oid, AccessShareLock);

	delta_oid = gamma_meta_get_delta_table_rel(rel);
	delta_rel = table_open(delta_oid, AccessShareLock);

	rows = cvtable_get_rows(cv_rel);
	table_block_relation_estimate_size(delta_rel, attr_widths, pages, tuples,
										allvisfrac, 0, 0);

	all_width = get_rel_data_width(rel, attr_widths);
	if (*pages > 0)
	{
		*tuples = (*pages / (all_width + sizeof(HeapTupleHeaderData)) * BLCKSZ);
	}

	/* treat Column Vector as a page */
	*pages += (rows * (all_width + sizeof(HeapTupleHeaderData)) / BLCKSZ);
	*tuples += rows;

	table_close(delta_rel, AccessShareLock);
	table_close(cv_rel, AccessShareLock);
}

static bool
ctable_scan_sample_next_block(TableScanDesc scan, SampleScanState * scanstate)
{
	elog(ERROR, "ctable_scan_sample_next_block not implemented");
}


static bool
ctable_scan_sample_next_tuple(TableScanDesc scan, SampleScanState * scanstate,
		TupleTableSlot * slot)
{
	elog(ERROR, "ctable_scan_sample_next_tuple not implemented");
}

static Oid
ctable_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;/* NOTE: heapam*/
}
