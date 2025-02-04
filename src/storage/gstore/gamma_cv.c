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

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#include "access/detoast.h"
#include "access/tupmacs.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "storage/gamma_cv.h"
#include "utils/gamma_fmgr.h"

ColumnVector*
gamma_cv_build(Form_pg_attribute attr, int dim)
{
	ColumnVector *cv = (ColumnVector *)palloc0(sizeof(ColumnVector));
	cv->dim = dim;
	cv->elemtype = attr->atttypid;
	cv->elemlen = attr->attlen;
	cv->elembyval = attr->attbyval;
	cv->elemalign = attr->attalign;
	cv->delbitmap = NULL;

	return cv;
}

void
gamma_cv_serialize(ColumnVector *cv, StringInfo serial_data)
{
	bool datumbyval = cv->elembyval;
	char datumalign = cv->elemalign;
	int16	datumlen = cv->elemlen;

	int dim = cv->dim;
	int row;

	if (datumbyval && datumlen > 0)
	{
		enlargeStringInfo(serial_data, datumlen * dim);
	}

	for (row = 0; row < dim; row++)
	{
		Datum datum = cv->values[row];
		Datum datum_detoast  = datum;
		bool isnull = cv->isnull[row];
		uint32 data_len;
		uint32 data_align_len;

		char *data_cur_ptr;


		/* detoast datum, get the real data */
		if (!isnull && datumlen == -1 && VARATT_IS_EXTENDED(datum))
		{
			struct varlena *detoast_value =
							(struct varlena *) DatumGetPointer(datum);
			datum_detoast = PointerGetDatum(detoast_attr(detoast_value));
		}

		/* get the datum length */
		if (datumlen > 0 && datumbyval)
		{
			data_len = sizeof(Datum);
			data_align_len = sizeof(Datum);
		}
		else
		{
			data_len = att_addlength_datum(0, datumlen, datum_detoast);
			data_align_len = att_align_nominal(data_len, datumalign);
		}

		enlargeStringInfo(serial_data, data_align_len);

		data_cur_ptr = serial_data->data + serial_data->len;
		memset(data_cur_ptr, 0, data_align_len);

		/* serial data */
		if (!isnull)
		{
			if (datumlen > 0)
			{
				if (datumbyval)
				{
					/* data type is fix length, treat it as Datum */
					gamma_store_att_byval(data_cur_ptr, datum_detoast, data_len);
				}
				else
				{
					memcpy(data_cur_ptr, DatumGetPointer(datum_detoast), data_len);
				}
			}
			else
			{
				Assert(!datumbyval);
				memcpy(data_cur_ptr, DatumGetPointer(datum_detoast), data_len);
			}
		}

		serial_data->len += data_align_len;

		/* free the detoast memorys */
		if (datum != datum_detoast)
			pfree(DatumGetPointer(datum_detoast));
	}

	return;
}

void
gamma_cv_fill_data(ColumnVector *cv, char *data, uint32 length,
					bool *nulls, uint32 count)
{
	uint32 i = 0;
	uint32 offset = 0;
	char *begin;

	cv->dim = count;

	if (cv->elembyval && cv->elemlen > 0)
	{
		//memcpy((char *)cv->values, data, sizeof(Datum) * count);
		//if (nulls != NULL)
		//	memcpy((char *)cv->isnull, nulls, sizeof(bool) * count);
		//else
		//	memset((char *)cv->isnull, false, sizeof(bool) * count);

		cv->values = (Datum *)data;
		if (nulls != NULL)
			cv->isnull = nulls;
		else
		{
			cv->isnull = NULL;
			CVSetNonNull(cv);
		}

		CVSetRef(cv);
		return;
	}

	for (i = 0; i < count; i++)
	{
		if (nulls != NULL && nulls[i])
		{
			cv->values[i] = (Datum)0;
			cv->isnull[i] = nulls[i];
			continue;
		}

		begin = data + offset;

		cv->values[i] = fetch_att(begin, cv->elembyval, cv->elemlen);
		cv->isnull[i] = (nulls != NULL ? nulls[i] : false);

		offset = att_addlength_datum(offset, cv->elemlen, cv->values[i]);
		offset = att_align_nominal(offset, cv->elemalign);
		if (offset > length)
		{
			ereport(ERROR,
					(errmsg("offset: %d, data length: %d", offset, length)));
		}
	}
}

bool
gamma_cv_get_metainfo(Relation rel, Relation cvrel, int32 attno, ColumnVector *cv,
			Datum *p_min, Datum *p_max, bool *p_has_null)
{
	int i;
	Datum min;
	Datum max;
	bool has_null = false;
	TupleDesc tupdesc = rel->rd_att;
	Form_pg_attribute attr = &tupdesc->attrs[attno - 1];
	Oid typcoll = attr->attcollation;
	Oid typid = attr->atttypid;
	TypeCacheEntry *typentry;
	FmgrInfo *cmp_func;

	typentry = lookup_type_cache(typid, TYPECACHE_CMP_PROC_FINFO);
	cmp_func = &typentry->cmp_proc_finfo;

	max = min = cv->values[0];
	has_null = cv->isnull[0];

	for (i = 1; i < cv->dim; i++)
	{
		if (cv->isnull[i])
		{
			has_null = true;
			continue;
		}

		if (DatumGetInt32(FunctionCall2Coll(cmp_func, typcoll, max, cv->values[i])) < 0)
		{
			max = cv->values[i];
		}

		if (DatumGetInt32(FunctionCall2Coll(cmp_func, typcoll, min, cv->values[i])) > 0)
		{
			min = cv->values[i];
		}
	}

	if (p_min)
		*p_min = min;

	if (p_max)
		*p_max = max;

	if (p_has_null)
		*p_has_null = has_null;

	return true;
}
