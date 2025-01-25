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
#include "utils/builtins.h"

#include "utils/gamma_re.h"
#include "utils/vdatum/vlike.h"
#include "utils/vdatum/vvarlena.h"
#include "utils/vdatum/vdatum.h"

PG_FUNCTION_INFO_V1(vtext_like_const);
PG_FUNCTION_INFO_V1(vtext_nlike_const);



Datum
vtext_like_const(PG_FUNCTION_ARGS)
{
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(0);
	Datum arg_con = PG_GETARG_DATUM(1);
	vbool *res = buildvbool(VECTOR_SIZE, vec_value->skipref);
	int i;

#ifdef _AVX2_
	text *tp = DatumGetTextPP(arg_con);
	StringContext context;
	cstring_init_pattern(&context, VARDATA_ANY(tp), VARSIZE_ANY_EXHDR(tp));
#endif

	for (i = 0; i < vec_value->dim; i++)
	{
		Datum arg;
		VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));
		VDATUM_SET_DATUM(res, i, Int32GetDatum(0));

		if (vec_value->skipref[i])
			continue;

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(res, i, true);
			continue;
		}

		arg = VDATUM_DATUM(vec_value, i);

#ifdef _AVX2_
		{
			bool match = false;
			text *tt = DatumGetTextPP(arg);
			cstring_is_substring(&context, VARDATA_ANY(tt),
												VARSIZE_ANY_EXHDR(tt), &match);
			VDATUM_SET_DATUM(res, i, match);
		}
#else
		VDATUM_SET_DATUM(res, i, DirectFunctionCall2Coll(textlike, PG_GET_COLLATION(),
													arg, arg_con));
#endif
	}

	PG_RETURN_POINTER(res);

}

Datum
vtext_nlike_const(PG_FUNCTION_ARGS)
{
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(0);
	Datum arg_con = PG_GETARG_DATUM(1);
	vbool *res = buildvbool(VECTOR_SIZE, vec_value->skipref);
	int i;

	for (i = 0; i < vec_value->dim; i++)
	{
		Datum arg;
		VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));
		VDATUM_SET_DATUM(res, i, Int32GetDatum(0));

		if (vec_value->skipref[i])
			continue;

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(res, i, true);
			continue;
		}

		arg = VDATUM_DATUM(vec_value, i);
		VDATUM_SET_DATUM(res, i, DirectFunctionCall2(textnlike, arg, arg_con));
	}

	PG_RETURN_POINTER(res);

}
