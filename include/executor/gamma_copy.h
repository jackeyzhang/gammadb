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

#ifndef GAMMA_COPY_H
#define GAMMA_COPY_H

#include "access/hio.h"
#include "executor/tuptable.h"

#include "storage/gamma_cv.h"

extern bool gammadb_copy_to_cvtable;

typedef struct CopyCollectorState
{
	Relation rel;
	int32 rows;
	MemoryContext context;
	CommandId cid;
	int options;
	uint32 rgid;

	/* to be a mark for one COPY command */
	BulkInsertStateData *bi;
	HeapTupleData pin_tuples[GAMMA_COLUMN_VECTOR_SIZE];
}
CopyCollectorState;

extern void gamma_copy_finish_collect(Relation rel, int options);
extern void gamma_copy_collect_and_merge(Relation rel, TupleTableSlot ** slots,
		int ntuples, CommandId cid, int options,
		struct BulkInsertStateData * bistate);

#endif /* GAMMA_COPY_H */
