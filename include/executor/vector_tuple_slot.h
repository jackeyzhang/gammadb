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

#ifndef VECTOR_TUPLE_SLOT_H
#define VECTOR_TUPLE_SLOT_H

#include "access/relscan.h"
#include "access/sdir.h"
#include "utils/relcache.h"
#include "executor/tuptable.h"

#include "storage/gamma_rg.h"
#include "utils/vdatum/vdatum.h"

#define GAMMA_VSLOT_FLAGS_NON_SKIP					(1)

/*
 * VectorTupleSlot store a batch of tuples in each slot.
 */
typedef struct VectorTupleSlot
{
	VirtualTupleTableSlot base;

	/* how many tuples does this slot contain */ 
	int32			dim;

	/* flags */
	int32			flags;

	/* skip array to represent filtered tuples */
	bool			skip[VECTOR_SIZE];

	/* 
	 * If it is not NULL, the row pointed by the index can be used.
	 * For example, in the Agg operator, rows belonging to the same
	 * entry are linked in this array to provide the aggregate
	 * function for batch execution.
	 */
	short *row_indexarr;
} VectorTupleSlot;

#define VSlotSetNonSkip(vslot) ((vslot)->flags |= GAMMA_VSLOT_FLAGS_NON_SKIP)
#define VSlotHasNonSkip(vslot) ((vslot)->flags & GAMMA_VSLOT_FLAGS_NON_SKIP)
#define VSlotClearNonSkip(vslot) \
	if (VSlotHasNonSkip(vslot)) \
		((vslot)->flags ^= GAMMA_VSLOT_FLAGS_NON_SKIP);

/* the interface for vector tuple slot */
extern TupleTableSlotOps TTSOpsVector;

#define TTS_IS_VECTOR(slot) ((slot)->tts_ops == &TTSOpsVector)

extern void ttsops_vector_init(void);
extern void tts_vector_slot_getallattrs(TupleTableSlot *slot,
		HeapTupleData *pin_tuples, Buffer *pin_buffers);
extern const TupleTableSlotOps* ttsops_vector_slot_callbacks(Relation relation);

extern uint32 tts_vector_slot_from_rg(TupleTableSlot *slot, RowGroup *rg,
										Bitmapset *bms_proj, uint32 offset);
extern void tts_slot_copy_values(TupleTableSlot *slot, TupleTableSlot *src_slot);
extern void tts_vector_slot_copy_values(TupleTableSlot *slot,
										TupleTableSlot *src_slot);
extern uint32 tts_slot_from_rg(TupleTableSlot *slot, RowGroup *rg,
								Bitmapset *bms_proj, uint32 offset);
extern bool tts_vector_slot_fill_tuple(TableScanDesc scandesc,
									   ScanDirection direction,
									   TupleTableSlot *slot);
extern void tts_vector_slot_copy_one_row(TupleTableSlot *slot,
									TupleTableSlot *src_slot, int row);
extern void tts_vector_slot_fill_vector(TupleTableSlot *slot,
									TupleTableSlot *src_slot, int row);
static inline int
tts_vector_get_dim(TupleTableSlot *slot)
{
	VectorTupleSlot *vslot = (VectorTupleSlot *) slot;
	return vslot->dim;
}

static inline bool
tts_vector_slot_get_skip(TupleTableSlot *slot, int row)
{
	VectorTupleSlot *vslot = (VectorTupleSlot *) slot;

	if (slot == NULL)
		return true;

	if (vslot->row_indexarr != NULL)
	{
		int i;
		for (i = 0; i < VECTOR_SIZE; i++)
		{
			if (vslot->row_indexarr[i] == -1)
				return false;

			if (vslot->row_indexarr[i] == row)
				return true;
		}

		return false;
	}

	return vslot->skip[row];
}

#endif
