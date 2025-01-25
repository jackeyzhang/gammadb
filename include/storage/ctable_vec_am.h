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

#ifndef CTABLE_VEC_AM_H
#define CTABLE_VEC_AM_H

#include "storage/gamma_rg.h"
#include "storage/gamma_cvtable_am.h"

typedef struct CTableScanDescData {
	TableScanDescData base;
	HeapScanDesc hscan;			/* delta table scan desc */
	CVScanDesc cvscan;			/* cv table scan desc */
	TupleTableSlot *buf_slot;
	bool heap;					/* cv table first */
	bool scan_over;
} CTableScanDescData;

typedef struct CTableScanDescData *CTableScanDesc;

extern bool vec_ctable_getnextslot(TableScanDesc scan, ScanDirection direction,
		TupleTableSlot * slot);
extern TableScanDesc  vec_ctable_beginscan(Relation rel, Snapshot snapshot,
		int nkeys,
		struct ScanKeyData * key,
		ParallelTableScanDesc parallel_scan,
		uint32 flags);
extern void vec_ctable_end_scan(TableScanDesc scan);
extern void vec_ctable_rescan(TableScanDesc scan, struct ScanKeyData * key,
		bool set_params, bool allow_strat, bool allow_sync,
		bool allow_pagemode);

#endif /* CTABLE_VEC_AM_H */
