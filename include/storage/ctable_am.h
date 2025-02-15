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

#ifndef CTABLE_AM_H
#define CTABLE_AM_H

#include "access/heapam.h"
#include "access/tableam.h"

#define GAMMA_COLTABLE_AM_NAME  "gamma"

typedef struct CIndexFetchCTableData
{
	IndexFetchHeapData base;
	IndexFetchHeapData *delta_scan;
	TupleTableSlot *heapslot; /* for heap fetch slot */
	Bitmapset *bms_proj;
	bool indexonlyscan;
} CIndexFetchCTableData;

const TableAmRoutine * ctable_tableam_routine(void);

#endif /* CTABLE_AM_H */
