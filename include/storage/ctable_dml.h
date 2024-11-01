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

#ifndef CTABLE_DML_H
#define CTABLE_DML_H


extern void ctable_insert(Relation relation, HeapTuple tup, CommandId cid,
			int options, BulkInsertState bistate);
extern TM_Result ctable_delete(Relation relation, ItemPointer tid,
			CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, bool changingPart);
extern TM_Result ctable_update(Relation relation, ItemPointer otid,
			HeapTuple newtup, CommandId cid, Snapshot snapshot,
			Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, LockTupleMode *lockmode);
extern void ctable_vacuum_rel(Relation rel, VacuumParams * params,
			BufferAccessStrategy bstrategy);

#endif /* CTABLE_DML_H */
