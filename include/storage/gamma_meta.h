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

#ifndef GAMMA_META_H
#define GAMMA_META_H

#include "utils/relcache.h"
#include "catalog/objectaddress.h"

#include "storage/gamma_rg.h"

#define GAMMA_NAMESPACE "gammadb_namespace"
#define GAMMA_META_CV_TABLE_NAME "gammadb_cv_table_%u"

#define Natts_gamma_rowgroup			9
#define Anum_gamma_rowgroup_rgid		1
#define Anum_gamma_rowgroup_attno		2
#define Anum_gamma_rowgroup_min			3
#define Anum_gamma_rowgroup_max			4
#define Anum_gamma_rowgroup_count		5
#define Anum_gamma_rowgroup_mode		6
#define Anum_gamma_rowgroup_values		7
#define Anum_gamma_rowgroup_nulls		8
#define Anum_gamma_rowgroup_option		9

#define GammaDelBitmapAttributeNumber	-2
#define GammaTidAttributeNumber			-1

/* the size of delta table: 1T */
extern int gammadb_delta_table_nblocks;
#define GAMMA_DELTA_TABLE_NBLOCKS (gammadb_delta_table_nblocks)

extern ObjectAddress gamma_meta_create_sequence(Relation baserel);
extern bool gamma_meta_cv_table(Relation rel, Datum reloptions);
extern void gamma_meta_truncate_cvtable(Oid cvrelid);
extern Oid gamma_meta_get_cv_table_rel(Relation baserel);
extern Oid gamma_meta_get_cv_table_oid(Oid base_rel_oid);
extern uint32 gamma_meta_next_rgid(Relation rel);
extern uint32 gamma_meta_max_rgid(Relation rel);
extern Oid gamma_meta_rgid_sequence_oid(Relation rel);

extern void gamma_meta_insert_rowgroup(Relation rel, RowGroup *rg);
extern void gamma_meta_insert_delbitmap(Relation cvrel, uint32 rgid, 
											bool *delbitmap, int32 count);
extern void gamma_meta_insert_cv(Relation rel, Relation cvrel,
					 uint32 rgid, int32 attno, ColumnVector *cv);

extern ItemPointerData gamma_meta_cv_convert_tid(uint32 rgid, uint16 rowid);
extern uint32 gamma_meta_tid_get_rgid(ItemPointerData tid);
extern uint16 gamma_meta_tid_get_rowid(ItemPointerData tid);
extern uint32 gamma_meta_ptid_get_rgid(ItemPointer tid);
extern uint16 gamma_meta_ptid_get_rowid(ItemPointer tid);
extern bool gamma_meta_tid_is_columnar(ItemPointer tid);
extern void gamma_meta_set_tid(HeapTuple tuple, uint32 rgid, uint16 rowid);

extern bool gamma_meta_is_gamma_table(Oid relid);

#endif /*GAMMA_META_H*/
