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

#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "executor/gamma_vec_qual.h"
#include "utils/vdatum/vdatum.h"

PG_FUNCTION_INFO_V1(gamma_vec_bool_expr_and);
Datum
gamma_vec_bool_expr_and(PG_FUNCTION_ARGS)
{
	int i;
	int j;
	vbool *result = NULL;
	bool skip = true;
	int nargs = PG_NARGS();

	/* fast path */
	if (nargs == 1 && !PG_ARGISNULL(0))
		return PG_GETARG_DATUM(0);

	for (j = 0; j < nargs; j++)
	{
		if (PG_ARGISNULL(j))
		{
			result = buildvdatum(BOOLOID, VECTOR_SIZE, NULL);
			memset(result->isnull, false, sizeof(result->isnull));
			memset(result->values, BoolGetDatum(false), sizeof(result->values));
			return PointerGetDatum(result);
		}

	}

	for (j = 0; j < nargs; j++)
	{
		skip = true;

		if(result == NULL)
		{
			result = (vbool *)PG_GETARG_POINTER(j);
			Assert(!result->ref);
			skip = false;
		}
		else
		{
			vbool *next = (vbool *)PG_GETARG_POINTER(j);
			for(i = 0; i < result->dim; i++)
			{
				result->isnull[i] = result->isnull[i] && next->isnull[i];
				result->values[i] =
					result->values[i] & next->values[i] & (!result->isnull[i]);

			}

			if (skip) 
			{
				for(i = 0; i < result->dim; i++)
				{
					if (DatumGetBool(result->values[i]))
					{
						skip = false;
						break;
					}
				}
			}
		}

		if(skip)
		{
			return PointerGetDatum(result);
		}
	}

	return PointerGetDatum(result);
}

/*
 * Replacing BoolExpr(OR_EXPR) in vectorized mode
 */
PG_FUNCTION_INFO_V1(gamma_vec_bool_expr_or);
Datum
gamma_vec_bool_expr_or(PG_FUNCTION_ARGS)
{
	int i;
	int j;
	vbool *result = NULL;
	bool skip = true;
	int nargs = PG_NARGS();

	/* TODO: need? */
	for (j = 0; j < nargs; j++)
	{
		if (PG_ARGISNULL(j))
		{
			result = buildvdatum(BOOLOID, VECTOR_SIZE, NULL);
			memset(result->isnull, false, sizeof(result->isnull));
			memset(result->values, BoolGetDatum(false), sizeof(result->values));
			return PointerGetDatum(result);
		}

	}

	for (j = 0; j < nargs; j++)
	{
		skip = true;

		if(result == NULL)
		{
			result = (vbool *)PG_GETARG_POINTER(j);
			//Assert(NULL != result->isnull);
			for(i = 0; i < result->dim; i++)
			{
				if(VDATUM_ISNULL(result, i) || !DatumGetBool(VDATUM_DATUM(result, i)))
				{
					skip = false;
					break;
				}
			}
		}
		else
		{
			vbool *next = (vbool *)PG_GETARG_POINTER(j);
			//Assert(NULL != result->isnull && NULL != next->isnull);
			for(i = 0; i < result->dim; i++)
			{
				VDATUM_SET_DATUM(result, i, (VDATUM_DATUM(result, i) || VDATUM_DATUM(next, i)));
				VDATUM_SET_ISNULL(result, i, (VDATUM_ISNULL(result, i) || VDATUM_ISNULL(next, i)));
				if (skip && (VDATUM_ISNULL(result, i) || VDATUM_DATUM(result, i)))
					skip = false;
			}
		}

		if(skip)
		{
			return PointerGetDatum(result);
		}
	}

	return PointerGetDatum(result);
}

/*
 * Replacing BoolExpr(NOT_EXPR) in vectorized mode
 */
PG_FUNCTION_INFO_V1(gamma_vec_bool_expr_not);
Datum
gamma_vec_bool_expr_not(PG_FUNCTION_ARGS)
{
	int i;
	vbool *result = NULL;

	if (PG_ARGISNULL(0))
	{
		result = buildvdatum(BOOLOID, VECTOR_SIZE, NULL);
		memset(result->isnull, false, sizeof(result->isnull));
		memset(result->values, BoolGetDatum(false), sizeof(result->values));
		return PointerGetDatum(result);
	}

	result = (vbool *)PG_GETARG_POINTER(0);
	//Assert(NULL != result->isnull);
	for(i = 0; i < result->dim; i++)
	{
		VDATUM_SET_DATUM(result, i, !VDATUM_DATUM(result, i));
	}

	return PointerGetDatum(result);
}
