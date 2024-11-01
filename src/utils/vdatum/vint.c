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
#include "common/int.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "utils/gamma_fmgr.h"
#include "utils/vdatum/vint.h"
#include "utils/vdatum/vdatum.h"


PG_FUNCTION_INFO_V1(vint2int4pl_u);

PG_FUNCTION_INFO_V1(vint8inc_any);
PG_FUNCTION_INFO_V1(vint4_sum);
PG_FUNCTION_INFO_V1(vint8inc);
PG_FUNCTION_INFO_V1(vint2_sum);
PG_FUNCTION_INFO_V1(vint8_avg_accum);
PG_FUNCTION_INFO_V1(vint2_avg_accum);
PG_FUNCTION_INFO_V1(vint4_avg_accum);

/*vint2 + int4 = vint4 */
Datum
vint2int4pl_u(PG_FUNCTION_ARGS)
{
	int size = 0;
	int i = 0;

	int32 arg1 = PG_GETARG_INT32(1);
	vdatum *arg2 = (vdatum *)PG_GETARG_POINTER(0);
	vint4 *res = buildvint4(VECTOR_SIZE, arg2->skipref);

	size = arg2->dim;

	while (i < size)
	{
		VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(arg2, i));
		i++;
	}

	i = 0;
	while (i < size)
	{
		if (arg2->skipref[i])
		{
			i++;
			continue;
		}

		if (!VDATUM_ISNULL(res, i))
		{
			VDATUM_SET_DATUM(res, i, (Int32GetDatum(arg1 + DatumGetInt16(VDATUM_DATUM(arg2, i)))));
		}
		i++;
	}

	res->dim = arg2->dim;

	PG_RETURN_POINTER(res);
}

/* count(*) aggregates */
Datum vint8inc_any(PG_FUNCTION_ARGS)
{
	vdatum *vec_value;
	int64 value = 0;
	int i;
	short indexarr = -1;

	vec_value = (vdatum *)PG_GETARG_POINTER(1);

	if (vec_value->indexarr != NULL)
	{
		i = 0;
		while ((indexarr = vec_value->indexarr[i]) != -1)
		{
			if (unlikely(i >= VECTOR_SIZE))
				break;
			value += 1;
			i++;
		}
	}
	else
	{
		for (i = 0; i < VECTOR_SIZE; i++)
		{
			if (vec_value->skipref[i])
				continue;

			value += 1;
		}
	}

#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *arg = (int64 *) PG_GETARG_POINTER(0);

		if (unlikely(pg_add_s64_overflow(*arg, value, arg)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_POINTER(arg);
	}
	else
#endif
	{
		/* Not called as an aggregate, so just do it the dumb way */
		int64		arg = PG_GETARG_INT64(0);
		int64		result;

		if (unlikely(pg_add_s64_overflow(arg, value, &result)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_INT64(result);
	}
}

/* count(vint8/vint4) aggregates */
Datum vint8inc(PG_FUNCTION_ARGS)
{
	vdatum *vec_value;
	int64 value = 0;
	int i;

	vec_value = (vdatum *) PG_GETARG_POINTER(1);

	for (i = 0; i < VECTOR_SIZE; i++)
	{
		if (vec_value->skipref[i])
			continue;

		value += 1;
	}

#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *arg = (int64 *) PG_GETARG_POINTER(0);

		if (unlikely(pg_add_s64_overflow(*arg, value, arg)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_POINTER(arg);
	}
	else
#endif
	{
		/* Not called as an aggregate, so just do it the dumb way */
		int64		arg = PG_GETARG_INT64(0);
		int64		result;

		if (unlikely(pg_add_s64_overflow(arg, value, &result)))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint out of range")));

		PG_RETURN_INT64(result);
	}
}

/* sum(vint4) aggregates*/
Datum
vint4_sum(PG_FUNCTION_ARGS)
{
	vdatum *vec_value;
	int64 value = 0;
	int i;
	short indexarr = -1;

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();	/* still no non-null */
	}
	
	if (!PG_ARGISNULL(1))
	{
		vec_value = (vdatum *) PG_GETARG_POINTER(1);

		if (vec_value->indexarr != NULL)
		{
			i = 0;
			while ((indexarr = vec_value->indexarr[i]) != -1)
			{
				if (unlikely(i >= VECTOR_SIZE))
					break;
				value += DatumGetInt32(VDATUM_DATUM(vec_value, indexarr));
				i++;
			}
		}
		else
		{
			for (i = 0; i < VECTOR_SIZE; i++)
			{
				if (vec_value->skipref[i])
					continue;


				value += DatumGetInt32(VDATUM_DATUM(vec_value, i));
			}
		}
	}

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_INT64(value);
	}

#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) value;

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		value = oldsum + value;

		PG_RETURN_INT64(value);
	}
}

/* sum(vint2) aggregates*/
Datum
vint2_sum(PG_FUNCTION_ARGS)
{
	vdatum *vec_value;
	int64 value = 0;
	int i;
	short indexarr = -1;

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();	/* still no non-null */
	}
	
	if (!PG_ARGISNULL(1))
	{
		vec_value = (vdatum *) PG_GETARG_POINTER(1);

		if (vec_value->indexarr != NULL)
		{
			i = 0;
			while ((indexarr = vec_value->indexarr[i]) != -1)
			{
				if (unlikely(i >= VECTOR_SIZE))
					break;
				value += DatumGetInt16(VDATUM_DATUM(vec_value, indexarr));
				i++;
			}
		}
		else
		{
			for (i = 0; i < VECTOR_SIZE; i++)
			{
				if (vec_value->skipref[i])
					continue;


				value += DatumGetInt16(VDATUM_DATUM(vec_value, i));
			}
		}
	}

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_INT64(value);
	}

#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) value;

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		value = oldsum + value;

		PG_RETURN_INT64(value);
	}
}

/*********************************sum/avg******************************/
static int
gamma_accum_vector(vdatum *vec_value, int128 *value)
{
	int128 accum = 0;
	int i = 0;
	int count = 0;
	short indexarr = -1;

	if (vec_value == NULL)
		return 0;

	if (vec_value->indexarr != NULL)
	{
		i = 0;
		while ((indexarr = vec_value->indexarr[i]) != -1)
		{
			if (unlikely(i >= VECTOR_SIZE))
				break;
			accum += DatumGetInt64(VDATUM_DATUM(vec_value, indexarr));
			count++;
			i++;
		}
	}
	else
	{
		for (i = 0; i < vec_value->dim; i++)
		{
			if (vec_value->skipref[i])
				continue;

			accum += DatumGetInt64(VDATUM_DATUM(vec_value, i));
			count++;
		}
	}

	if (value != NULL)
		*value = accum;

	return count;
}

/* sum(vint8) aggregates -- transition function */
#ifdef HAVE_INT128
typedef struct Int128AggState
{
	bool        calcSumX2;      /* if true, calculate sumX2 */
	int64       N;              /* count of processed numbers */
	int128      sumX;           /* sum of processed numbers */
	int128      sumX2;          /* sum of squares of processed numbers */
} Int128AggState;

static Int128AggState *
makeInt128AggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	Int128AggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (Int128AggState *) palloc0(sizeof(Int128AggState));
	state->calcSumX2 = calcSumX2;

	MemoryContextSwitchTo(old_context);

	return state;
}

typedef Int128AggState PolyNumAggState;
#define makePolyNumAggState makeInt128AggState

#else
typedef struct NumericAggState
{
	bool        calcSumX2;      /* if true, calculate sumX2 */
	MemoryContext agg_context;  /* context we're calculating in */
	int64       N;              /* count of processed numbers */
	NumericSumAccum sumX;       /* sum of processed numbers */
	NumericSumAccum sumX2;      /* sum of squares of processed numbers */
	int         maxScale;       /* maximum scale seen so far */
	int64       maxScaleCount;  /* number of values seen with maximum scale */
	/* These counts are *not* included in N!  Use NA_TOTAL_COUNT() as needed */
	int64       NaNcount;       /* count of NaN values */
	int64       pInfcount;      /* count of +Inf values */
	int64       nInfcount;      /* count of -Inf values */
} NumericAggState;
typedef NumericAggState PolyNumAggState;
#endif

Datum
vint8_avg_accum(PG_FUNCTION_ARGS)
{
	//Datum state = PointerGetDatum(NULL);
	PolyNumAggState *state;
	vdatum *vec_value;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
	{
		//pstate = (PolyNumAggState *)state;
		//pstate->N = pstate->N + count - 1;
		int128 value = 0;
		int count = 0;
		vec_value = (vdatum *) PG_GETARG_POINTER(1);
		count = gamma_accum_vector(vec_value, &value);
		state->sumX = state->sumX + value;
		state->N = state->N + count;

#if 0
		state = DirectFunctionCall2Mem(int8_avg_accum, 
					(fmNodePtr) fcinfo->context,	
					PG_ARGISNULL(0) ? PointerGetDatum(NULL) : PG_GETARG_DATUM(0),
					Int64GetDatum(value));
#endif
	}

	PG_RETURN_POINTER(state);
}


/* avg(int2) -- transition functions */

typedef struct Int8TransTypeData
{
	int64       count;
	int64       sum;
} Int8TransTypeData;

Datum
vint2_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	//int16       newval = PG_GETARG_INT16(1);
	Int8TransTypeData *transdata;
	int128 value = 0;
	int count = 0;
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);

	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
			ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");


	count = gamma_accum_vector(vec_value, &value);

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count = transdata->count + count;
	transdata->sum += value;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
vint4_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	//int32       newval = PG_GETARG_INT32(1);
	Int8TransTypeData *transdata;
	int128 value = 0;
	int count = 0;
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);

	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
			ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	count = gamma_accum_vector(vec_value, &value);
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count = transdata->count + count;
	transdata->sum += value;

	PG_RETURN_ARRAYTYPE_P(transarray);
}
