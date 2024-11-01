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

#ifndef GAMMA_CVTABLE_AM_H
#define GAMMA_CVTABLE_AM_H

#include "access/heapam.h"
#include "access/genam.h"
#include "access/relscan.h"

#include "executor/vector_tuple_slot.h"
#include "storage/gamma_rg.h"

typedef struct RowGroupCtableScanDescData
{
	pg_atomic_uint32 max_rg_id;
	pg_atomic_uint32 cur_rg_id;
} RowGroupCtableScanDescData;

typedef struct RowGroupCtableScanDescData *RowGroupCtableScanDesc;

typedef struct VecParallelTableScanDescData
{
	ParallelBlockTableScanDescData hbdata;
	RowGroupCtableScanDescData rgdata;

}VecParallelTableScanDescData;

typedef struct VecParallelTableScanDescData *VecParallelTableScanDesc;

typedef struct CVScanDescData {
	IndexScanDesc scan;
	Relation cv_rel;
	Relation base_rel;
	Relation cv_index_rel;
	TupleTableSlot *cv_slot;
	Snapshot snapshot;

	/* for parallel */
	ParallelBlockTableScanDesc p_b;
	RowGroupCtableScanDesc p_rg;
	
	/* row group data */
	RowGroup *rg;
	uint32 offset;		/* # rows have been processed */

	/* projection info*/
	Bitmapset *bms_proj;

	bool inited;
} CVScanDescData;

typedef struct CVScanDescData *CVScanDesc;


extern CVScanDesc cvtable_beginscan(Relation rel, Snapshot snapshot, int nkeys,
		struct ScanKeyData * key,
		ParallelTableScanDesc parallel_scan,
		uint32 flags);
extern bool cvtable_getnextslot(CVScanDesc cvscan, ScanDirection direction,
		TupleTableSlot * slot);
extern bool cvtable_loadnext_rg(CVScanDesc cvscan, ScanDirection direction);
extern bool cvtable_load_rg(CVScanDesc cvscan, uint32 rgid);
extern bool cvtable_load_rowslot(CVScanDesc cvscan, uint32 rgid,
									int32 rowid, TupleTableSlot *slot);
extern void cvtable_rescan(CVScanDesc scan, struct ScanKeyData * key,
		bool set_params, bool allow_strat, bool allow_sync,
		bool allow_pagemode);
extern void cvtable_endscan(CVScanDesc cvscan);

extern TM_Result cvtable_delete_tuple(Relation relation, ItemPointer tid,
			CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, bool changingPart);
extern HeapTuple cvtable_get_delbitmap_tuple(Relation cvrel, Oid indexoid,
											 Snapshot snapshot, Oid rgid);
extern void cvtable_load_delbitmap(CVScanDesc cvscan, uint32 rgid);

extern uint64 cvtable_get_rows(Relation cvrel);
extern void cvtable_update_delete_bitmap(Relation relation, Snapshot snapshot,
								uint32 rgid, bool *vacuum_delbitmap, int count);

#endif /* GAMMA_CVTABLE_AM_H */
