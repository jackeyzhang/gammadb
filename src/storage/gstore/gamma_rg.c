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

#include "access/detoast.h"
#include "access/htup_details.h"
#include "access/tupmacs.h"
#include "storage/shmem.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"

#include "storage/gamma_cvtable_am.h"
#include "storage/gamma_meta.h"
#include "storage/gamma_rg.h"

//TODO: check if > 128
#define GAMMA_COLUMN_VECTOR_CACHE   128
static Datum cache_values[GAMMA_COLUMN_VECTOR_CACHE][GAMMA_COLUMN_VECTOR_SIZE ];
static bool cache_isnull[GAMMA_COLUMN_VECTOR_CACHE][GAMMA_COLUMN_VECTOR_SIZE];

RowGroup*
gamma_rg_build(Relation rel)
{
	return gamma_rg_build_desc(RelationGetDescr(rel));
}

RowGroup*
gamma_rg_build_desc(TupleDesc tupdesc)
{
	int i;
	int attcount = tupdesc->natts;
	ColumnVector *cv;

	RowGroup *rg = (RowGroup *)palloc0(SizeOfRowGroup(attcount));

	rg->delbitmap = (bool *) &cache_isnull[attcount][0];

	for (i = 0; i < attcount; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		cv = &(rg->cvs[i]);

		cv->dim = 0;
		cv->elemtype = att->atttypid;
		cv->elemlen = att->attlen;
		cv->elembyval = att->attbyval;
		cv->elemalign = att->attalign;
		cv->delbitmap = (bool *)rg->delbitmap;
		cv->isnull = &cache_isnull[i][0];
		cv->values = &cache_values[i][0];
	}


	return rg;
}

void
gamma_rg_free(RowGroup *rg)
{
	pfree(rg);
}

ColumnVector *
gamma_rg_get_cv(RowGroup *rg, int idx)
{
	return &(rg->cvs[idx]);
}

void
gamma_fill_rowgroup(Relation rel, HeapTupleData *pin_tuples, bool *delbitmap,
					RowGroup *rg, int32 rowcount)
{
	TupleDesc tupdesc = rel->rd_att;
	HeapTuple tuple;
	HeapTupleHeader tup;
	int32 row;
	bool		hasnulls;
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	long		off;			/* offset in tuple data */
	bits8	   *bp;		/* ptr to null bitmap in tuple */
	bool		slow;			/* can we use/set attcacheoff? */
	ColumnVector *cv;

	/* set delete bitmap */
	if (delbitmap != NULL)
	{
		memcpy(rg->delbitmap, delbitmap, rowcount * sizeof(bool));
	}

	for (row = 0; row < rowcount; row++)
	{
		tuple = &pin_tuples[row];
		tup = tuple->t_data;
		bp = tup->t_bits;
		hasnulls = HeapTupleHasNulls(tuple);

		/* vectorize engine deform once for now */
		off = 0;
		slow = false;

		tp = (char *) tup + tup->t_hoff;

		for (attnum = 0; attnum < tupdesc->natts; attnum++)
		{
			Form_pg_attribute thisatt = TupleDescAttr(tupdesc, attnum);
			cv = gamma_rg_get_cv(rg, attnum);

			Assert(cv->elemtype == thisatt->atttypid);

			if (hasnulls && att_isnull(attnum, bp))
			{
				cv->values[row] = (Datum) 0;
				cv->isnull[row] = true;
				slow = true;		/* can't use attcacheoff anymore */
				continue;
			}

			cv->isnull[row] = false;

			if (!slow && thisatt->attcacheoff >= 0)
				off = thisatt->attcacheoff;
			else if (thisatt->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be no
				 * pad bytes in any case: then the offset will be valid for either
				 * an aligned or unaligned value.
				 */
				if (!slow &&
						off == att_align_nominal(off, thisatt->attalign))
					thisatt->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, thisatt->attalign, -1,
							tp + off);
					slow = true;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, thisatt->attalign);

				if (!slow)
					thisatt->attcacheoff = off;
			}

			cv->values[row] = fetchatt(thisatt, tp + off);

			off = att_addlength_pointer(off, thisatt->attlen, tp + off);

			if (thisatt->attlen <= 0)
				slow = true;		/* can't use attcacheoff anymore */
		}
	}


	rg->dim = rowcount;
	for (attnum = 0; attnum < tupdesc->natts; attnum++)
	{
		cv = (ColumnVector *)&(rg->cvs[attnum]);
		cv->dim = rowcount;
	}

	return;
}

bool
gamma_rg_fetch_slot(Relation rel, Snapshot snapshot, ItemPointer tid,
						TupleTableSlot *slot, Bitmapset *bms_proj)
{
	bool result = false;
	CVScanDesc cvscan;

	uint32 rgid = gamma_meta_ptid_get_rgid(tid);
	int32 rowid = gamma_meta_ptid_get_rowid(tid);

	if (snapshot == NULL)
		snapshot = GetTransactionSnapshot();
	cvscan = cvtable_beginscan(rel, snapshot, 0, NULL, NULL, 0);
	cvscan->bms_proj = bms_proj;

	result = cvtable_load_rowslot(cvscan, rgid, rowid, slot);

	cvtable_endscan(cvscan);

	return result;
}

bool
gamma_rg_check_visible(Relation rel, Snapshot snapshot, ItemPointer tid)
{
	bool result = false;
	CVScanDesc cvscan;

	uint32 rgid = gamma_meta_ptid_get_rgid(tid);
	int32 rowid = gamma_meta_ptid_get_rowid(tid);

	if (snapshot == NULL)
		snapshot = GetTransactionSnapshot();
	cvscan = cvtable_beginscan(rel, snapshot, 0, NULL, NULL, 0);

	cvtable_load_delbitmap(cvscan, rgid);

	if (cvscan->rg->delbitmap == NULL)
		result = true;
	else
		result = !cvscan->rg->delbitmap[rowid];

	cvtable_endscan(cvscan);

	return result;
}
