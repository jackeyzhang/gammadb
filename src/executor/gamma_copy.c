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
#include "utils/memutils.h"

#include "executor/gamma_copy.h"
#include "executor/gamma_merge.h"
#include "storage/gamma_meta.h"

bool gammadb_copy_to_cvtable = true;

static CopyCollectorState cstate = {0};

static void gamma_copy_slot_set_tid(TupleTableSlot *slot, uint32 rgid, uint16 row);

static void
gamma_copy_init_state(Relation rel, CommandId cid, int options,
		struct BulkInsertStateData *bistate)
{
	cstate.rel = rel;
	cstate.rows = 0;
	cstate.cid = cid;
	cstate.options = options;
	cstate.bi = bistate;
	cstate.rgid = gamma_meta_next_rgid(rel);

	if (cstate.context != NULL)
	{
		MemoryContextReset(cstate.context);
	}
	else
	{
		cstate.context = AllocSetContextCreate(TopMemoryContext,
				"Gamma Copy Collector",
				ALLOCSET_DEFAULT_SIZES);
	}

	return;
}

static void
gamma_copy_reset_state(Relation rel)
{
	cstate.rows = 0;
	cstate.rgid = gamma_meta_next_rgid(rel);

	if (cstate.context != NULL)
	{
		MemoryContextReset(cstate.context);
	}
	else
	{
		cstate.context = AllocSetContextCreate(TopMemoryContext,
				"Gamma Copy Collector",
				ALLOCSET_DEFAULT_SIZES);
	}

	return;
}

static void
gamma_copy_release_state(void)
{
	cstate.rel = NULL;
	cstate.rows = 0;
	cstate.cid = 0;
	cstate.options = 0;
	cstate.bi = NULL;

	if (cstate.context != NULL)
	{
		/*TODO: destroy and set NULL? */
		MemoryContextReset(cstate.context);
	}

	return;
}

void
gamma_copy_collect_and_merge(Relation rel, TupleTableSlot ** slots, int ntuples,
		CommandId cid, int options,
		struct BulkInsertStateData * bistate)
{
	int i;
	MemoryContext old_context;
	HeapTuple tup;

	/* check if it is first time */
	if (cstate.bi == NULL || cstate.bi != bistate)
	{
		gamma_copy_init_state(rel, cid, options, bistate);
	}

	/* begin to collect */
	for (i = 0; i < ntuples; i++)
	{
		old_context = MemoryContextSwitchTo(cstate.context);
		gamma_copy_slot_set_tid(slots[i], cstate.rgid, cstate.rows);
		tup = ExecFetchSlotHeapTuple(slots[i], false, NULL);
		tup = heap_copytuple(tup);
		memcpy(&cstate.pin_tuples[cstate.rows++], tup, sizeof(HeapTupleData));
		MemoryContextSwitchTo(old_context);

		if (cstate.rows >= GAMMA_COLUMN_VECTOR_SIZE)
		{
			uint32 rgid = cstate.rgid;
			old_context = MemoryContextSwitchTo(cstate.context);
			gamma_merge_one_rowgroup(rel, cstate.pin_tuples, rgid, NULL, cstate.rows);
			MemoryContextSwitchTo(old_context);

			/* reset the collector */
			gamma_copy_reset_state(rel);
		}
	}

	return;
}

void
gamma_copy_finish_collect(Relation rel, int options)
{
	MemoryContext old_context;

	if (cstate.rows > 0)
	{
		uint32 rgid = cstate.rgid;
		old_context = MemoryContextSwitchTo(cstate.context);
		gamma_merge_one_rowgroup(rel, cstate.pin_tuples, rgid, NULL, cstate.rows);
		MemoryContextSwitchTo(old_context);
	}

	gamma_copy_release_state();
	return;
}

static void
gamma_copy_slot_set_tid(TupleTableSlot *slot, uint32 rgid, uint16 row)
{
	Assert(slot != NULL);
	slot->tts_tid = gamma_meta_cv_convert_tid(rgid, row);
}
