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
#include "catalog/pg_type.h"
#include "utils/date.h"

#include "utils/vdatum/vdate.h"
#include "utils/vdatum/vtimestamp.h"

PG_FUNCTION_INFO_V1(vdate_le_timestamp);
PG_FUNCTION_INFO_V1(vdate_mi_interval);
PG_FUNCTION_INFO_V1(vdate_le);
PG_FUNCTION_INFO_V1(vdate_ge_const);
PG_FUNCTION_INFO_V1(vdate_in);
PG_FUNCTION_INFO_V1(vdate_out);

PG_FUNCTION_INFO_V1(vdate_larger);
PG_FUNCTION_INFO_V1(vdate_smaller);

static vtimestamp* vdate2vtimestamp(vdate* vdateVal);
/*
 * Internal routines for promoting date to timestamp and timestamp with
 * time zone
 */

vdate* buildvdate(int dim, bool *skip)
{
	return (vdate *)buildvdatum(DATEOID, dim, skip);	
}


static vtimestamp*
vdate2vtimestamp(vdate* vdateVal)
{
	vtimestamp	*result;
	int 		i;
	DateADT		dateVal;
	Timestamp	tmp;	
	
	result = buildvtimestamp(vdateVal->dim, vdateVal->skipref);
	for (i = 0; i < VECTOR_SIZE; i++)
	{
		if (vdateVal->skipref[i])
			continue;
		dateVal = DatumGetDateADT(VDATUM_DATUM(vdateVal, i));	
#ifdef HAVE_INT64_TIMESTAMP
		/* date is days since 2000, timestamp is microseconds since same... */
		tmp = dateVal * USECS_PER_DAY;
		
		/* Date's range is wider than timestamp's, so must check for overflow */
		if (tmp / USECS_PER_DAY != dateVal)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("date out of range for timestamp")));

		VDATUM_SET_DATUM(result, i, TimestampGetDatum(tmp));
#else
		/* date is days since 2000, timestamp is seconds since same... */
		VDATUM_SET_DATUM(result, i, TimestampGetDatum(dateVal * (double) SECS_PER_DAY));
#endif
	}
	return result;
}
Datum
vdate_le_timestamp(PG_FUNCTION_ARGS)
{
	vdate		*vdateVal = (vdate *)PG_GETARG_POINTER(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	vtimestamp	*vdt1;
	int			i;
	vbool		*result;
	Timestamp	dt1;	
	
	vdt1 = vdate2vtimestamp(vdateVal);

	result = buildvbool(vdt1->dim, vdt1->skipref);
#ifdef HAVE_INT64_TIMESTAMP
	for (i = 0; i < VECTOR_SIZE; i++ )
	{
		if (vdt1->skipref[i])
			continue;
		dt1 = DatumGetTimestamp(VDATUM_DATUM(vdt1, i));
		VDATUM_SET_DATUM(result, i, BoolGetDatum((dt1 <= dt2) ? true :false));
	}
	return PointerGetDatum(result);
#else
	elog(ERROR, "HAVE_INT64_TIMESTAMP must be enabled in vectorize executor.");
#endif
}


Datum vdate_mi_interval(PG_FUNCTION_ARGS)
{
	vdate		*vdateVal = (vdate *)PG_GETARG_POINTER(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	vtimestamp	*vdateStamp;

	vdateStamp = vdate2vtimestamp(vdateVal);

	return DirectFunctionCall2(vtimestamp_mi_interval,
							   PointerGetDatum(vdateStamp),
							   PointerGetDatum(span));
}

Datum
vdate_le(PG_FUNCTION_ARGS)
{
	vdate		*vdt1 = (vdate *)PG_GETARG_POINTER(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);
	vbool		*result;
	int			i;
	
	result = buildvbool(vdt1->dim, vdt1->skipref);
	for (i = 0; i < VECTOR_SIZE; i++ )
	{
		if (vdt1->skipref != NULL && vdt1->skipref[i])
			continue;
		VDATUM_SET_DATUM(result,i,BoolGetDatum(DatumGetDateADT(VDATUM_DATUM(vdt1, i)) <= dateVal2));
	}
	PG_RETURN_POINTER(result);
}

Datum
vdate_ge_const(PG_FUNCTION_ARGS)
{
	vdate		*vdt1 = (vdate *)PG_GETARG_POINTER(0);
	DateADT		dateVal2 = PG_GETARG_DATEADT(1);
	vbool		*result;
	int			i;
	
	result = buildvbool(vdt1->dim, vdt1->skipref);
	for (i = 0; i < VECTOR_SIZE; i++ )
	{
		if (vdt1->skipref != NULL && vdt1->skipref[i])
			continue;
		VDATUM_SET_DATUM(result,i,BoolGetDatum(DatumGetDateADT(VDATUM_DATUM(vdt1, i)) >= dateVal2));
	}
	PG_RETURN_POINTER(result);
}


Datum vdate_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vdate_in not supported");
}

Datum vdate_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vdate_out not supported");
}

/* min/max(date) -- transition function */
static DateADT
gamma_minmax_date_vector(vdatum *vec_value, bool max)
{
	int i = 0;
	short indexarr = -1;
	DateADT value = 0;
	bool first = true;

	if (vec_value == NULL)
		return 0;

	if (vec_value->indexarr != NULL)
	{
		i = 0;
		while ((indexarr = vec_value->indexarr[i]) != -1)
		{
			if (unlikely(i >= VECTOR_SIZE))
				break;

			if (first)
			{
				value = DatumGetDateADT(VDATUM_DATUM(vec_value, indexarr));
				first = false;
				i++;
				continue;
			}
			else if (max)
			{
				/* max */
				if (value < DatumGetDateADT(VDATUM_DATUM(vec_value, indexarr)))
				{
					value = DatumGetDateADT(VDATUM_DATUM(vec_value, indexarr));
				}
			}
			else
			{
				/* min */
				if (value > DatumGetDateADT(VDATUM_DATUM(vec_value, indexarr)))
				{
					value = DatumGetDateADT(VDATUM_DATUM(vec_value, indexarr));
				}	
			}
			i++;
		}
	}
	else
	{
		for (i = 0; i < vec_value->dim; i++)
		{
			if (vec_value->skipref[i])
				continue;

			if (first)
			{
				value = DatumGetDateADT(VDATUM_DATUM(vec_value, i));
				first = false;
				continue;
			}
			else if (max)
			{
				/* max */
				if (value < DatumGetDateADT(VDATUM_DATUM(vec_value, i)))
				{
					value = DatumGetDateADT(VDATUM_DATUM(vec_value, i));
				}
			}
			else
			{
				/* min */
				if (value > DatumGetDateADT(VDATUM_DATUM(vec_value, i)))
				{
					value = DatumGetDateADT(VDATUM_DATUM(vec_value, i));
				}	
			}
		}
	}

	return value;
}

Datum
vdate_larger(PG_FUNCTION_ARGS)
{
	DateADT     dateVal1 = PG_GETARG_DATEADT(0);
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);
	DateADT		dateVal2 = gamma_minmax_date_vector(vec_value, true/*max*/);

	PG_RETURN_DATEADT((dateVal1 > dateVal2) ? dateVal1 : dateVal2);
}

/* min(date) -- transition function */
Datum
vdate_smaller(PG_FUNCTION_ARGS)
{
	DateADT     dateVal1 = PG_GETARG_DATEADT(0);
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);
	DateADT		dateVal2 = gamma_minmax_date_vector(vec_value, false/*min*/);

	PG_RETURN_DATEADT((dateVal1 < dateVal2) ? dateVal1 : dateVal2);
}
