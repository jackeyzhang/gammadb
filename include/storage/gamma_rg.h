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

#ifndef GAMMA_RG_H
#define GAMMA_RG_H

#include "access/htup.h"
#include "executor/tuptable.h"
#include "utils/snapshot.h"

#include "storage/gamma_cv.h"

#define GAMMA_ROWGROUP_HAS_DELBITMAP		(1)

typedef struct RowGroup {
	Oid rgid;
	int dim;
	int flags;
	bool *delbitmap;
	ColumnVector cvs[FLEXIBLE_ARRAY_MEMBER];
} RowGroup;

#define RGSetDelBitmap(rg) (rg->flags |= GAMMA_ROWGROUP_HAS_DELBITMAP);
#define RGHasDelBitmap(rg) (rg->flags & GAMMA_ROWGROUP_HAS_DELBITMAP)

#define SizeOfRowGroup(cnt) \
				add_size(offsetof(RowGroup, cvs), \
						 mul_size(sizeof(ColumnVector), cnt))

extern RowGroup* gamma_rg_build(Relation rel);
extern RowGroup* gamma_rg_build_desc(TupleDesc tupdesc);
extern void gamma_rg_free(RowGroup *rg);

extern ColumnVector *gamma_rg_get_cv(RowGroup *rg, int idx);
extern bool gamma_rg_fetch_slot(Relation rel, Snapshot snapshot,
								ItemPointer tid, TupleTableSlot *slot,
								Bitmapset *bms_proj);
extern bool gamma_rg_check_visible(Relation rel, Snapshot snapshot, ItemPointer tid);

extern void gamma_fill_rowgroup(Relation rel, HeapTupleData *pin_tuples,
								bool *delbitmap, RowGroup *rg, int32 rowcount);

#endif /*GAMMA_RG_H*/
