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

#ifndef VEC_EXEC_SCAN_H
#define VEC_EXEC_SCAN_H

#include "utils/relcache.h"
#include "executor/executor.h"
#include "executor/tuptable.h"

extern void VecExecAssignScanProjectionInfo(ScanState *node);
extern void VecExecConditionalAssignProjectionInfo(PlanState *planstate,
									TupleDesc inputDesc,
									Index varno);
extern TupleTableSlot* vec_tablescan_execscan(ScanState *node,
		 ExecScanAccessMtd accessMtd, ExecScanRecheckMtd recheckMtd);

#endif /* VEC_EXEC_SCAN_H */
