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

#include "math.h"

#include "postgres.h"

#include "utils/array.h"
#include "utils/float.h"
#include "catalog/pg_type.h"

#include "utils/vdatum/vfloat.h"
#include "utils/vdatum/vdatum.h"

PG_FUNCTION_INFO_V1(vfloat8vfloat8mul2);
PG_FUNCTION_INFO_V1(vfloat8pl);
PG_FUNCTION_INFO_V1(vfloat8_accum);
PG_FUNCTION_INFO_V1(vfloat8_avg);

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n);

#if 0
#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)			\
do {															\
	if (isinf(val) && !(inf_is_valid))							\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		  errmsg("value out of range: overflow")));				\
																\
	if ((val) == 0.0 && !(zero_is_valid))						\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		 errmsg("value out of range: underflow")));				\
} while(0)
#endif

Datum vfloat8vfloat8mul2(PG_FUNCTION_ARGS)
{
	vdatum		*arg1 = (vdatum *)PG_GETARG_POINTER(0);
	vdatum		*arg2 = (vdatum *)PG_GETARG_POINTER(1);
	vdatum		*result;
	float8		mul;
	int			i;

	result = buildvdatum(FLOAT8OID, VECTOR_SIZE, arg1->skipref);

	for (i = 0; i < VECTOR_SIZE; i++ )
	{
		if (arg1->skipref[i])
			continue;
		mul = DatumGetFloat8(VDATUM_DATUM(arg1, i)) * DatumGetFloat8(VDATUM_DATUM(arg2, i));
		VDATUM_SET_DATUM(result, i, Float8GetDatum(mul));
	}

	PG_RETURN_POINTER(result);
}

Datum vfloat8pl(PG_FUNCTION_ARGS)
{
	vdatum *vec_value;
	float8 value = 0.;
	float8 result;
	int i;
	bool inf_arg = false;
	bool inf_result = false;
	short indexarr = -1;

	Assert (!PG_ARGISNULL(0));

	result = PG_GETARG_FLOAT8(0);
	inf_result = isinf(result);

	if (unlikely(inf_result))
		PG_RETURN_FLOAT8(result);

	if (PG_ARGISNULL(1))
	{
		PG_RETURN_FLOAT8(result);
	}

	vec_value = (vdatum *) PG_GETARG_POINTER(1);

	if (vec_value->indexarr != NULL)
	{
		i = 0;
		while ((indexarr = vec_value->indexarr[i++]) != -1)
		{
			if (unlikely(i >= VECTOR_SIZE))
				break;
			value = DatumGetFloat8(VDATUM_DATUM(vec_value, indexarr));
			if (unlikely(isinf(value)))
			{
				result += value;
				inf_arg = true;
				break;
			}
			else
			{
				result += DatumGetFloat8(VDATUM_DATUM(vec_value, indexarr));
			}

		}
	}
	else
	{
		for (i = 0; i < VECTOR_SIZE; i++)
		{
			if (vec_value->skipref[i])
				continue;

			value = DatumGetFloat8(VDATUM_DATUM(vec_value, i));
			if (unlikely(isinf(value)))
			{
				result += value;
				inf_arg = true;
				break;
			}
			else
			{
				result += DatumGetFloat8(VDATUM_DATUM(vec_value, i));
			}
		}
	}

	if (unlikely(isinf(result)) && !inf_arg)
		float_overflow_error();

	PG_RETURN_FLOAT8(result);
}

Datum
vfloat8_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	vdatum *vec_value = (vdatum *) PG_GETARG_POINTER(1);
	float8 newval;
	float8 *transvalues;
	float8 N, Sx, Sxx, tmp;
	bool inf_x = false;
	int i;
	short indexarr = -1;

	transvalues = check_float8_array(transarray, "float8_accum", 3);
	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];

	inf_x = isinf(Sx);

	if (vec_value->indexarr != NULL)
	{
		i = 0;
		while ((indexarr = vec_value->indexarr[i++]) != -1)
		{
			if (unlikely(i >= VECTOR_SIZE))
				break;

			newval = DatumGetFloat8(VDATUM_DATUM(vec_value, indexarr));

			/*
			 * Use the Youngs-Cramer algorithm to incorporate the new value into the
			 * transition values.
			 */
			N += 1.0;
			Sx += newval;
			if (transvalues[0] > 0.0)
			{
				tmp = newval * N - Sx;
				Sxx += tmp * tmp / (N * transvalues[0]);

				/*
				 * Overflow check.  We only report an overflow error when finite
				 * inputs lead to infinite results.  Note also that Sxx should be NaN
				 * if any of the inputs are infinite, so we intentionally prevent Sxx
				 * from becoming infinite.
				 */
				if (isinf(Sx) || isinf(Sxx))
				{
					if (!inf_x && !isinf(newval))
						float_overflow_error();

					Sxx = get_float8_nan();
				}
			}
			else
			{
				/*
				 * At the first input, we normally can leave Sxx as 0.  However, if
				 * the first input is Inf or NaN, we'd better force Sxx to NaN;
				 * otherwise we will falsely report variance zero when there are no
				 * more inputs.
				 */
				if (isnan(newval) || isinf(newval))
					Sxx = get_float8_nan();
			}
		}
	}
	else
	{
		for (i = 0; i < VECTOR_SIZE; i++)
		{
			if (vec_value->skipref[i])
				continue;

			newval = DatumGetFloat8(VDATUM_DATUM(vec_value, i));

			/*
			 * Use the Youngs-Cramer algorithm to incorporate the new value into the
			 * transition values.
			 */
			N += 1.0;
			Sx += newval;
			if (transvalues[0] > 0.0)
			{
				tmp = newval * N - Sx;
				Sxx += tmp * tmp / (N * transvalues[0]);

				/*
				 * Overflow check.  We only report an overflow error when finite
				 * inputs lead to infinite results.  Note also that Sxx should be NaN
				 * if any of the inputs are infinite, so we intentionally prevent Sxx
				 * from becoming infinite.
				 */
				if (isinf(Sx) || isinf(Sxx))
				{
					if (!inf_x && !isinf(newval))
						float_overflow_error();

					Sxx = get_float8_nan();
				}
			}
			else
			{
				/*
				 * At the first input, we normally can leave Sxx as 0.  However, if
				 * the first input is Inf or NaN, we'd better force Sxx to NaN;
				 * otherwise we will falsely report variance zero when there are no
				 * more inputs.
				 */
				if (isnan(newval) || isinf(newval))
					Sxx = get_float8_nan();
			}
		}
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = Sx;
		transvalues[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);

		result = construct_array(transdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}
