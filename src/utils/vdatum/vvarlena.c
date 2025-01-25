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
#include "catalog/pg_collation.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/pg_locale.h"
#include "utils/varlena.h"

#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"
#include "utils/vdatum/vvarlena.h"

PG_FUNCTION_INFO_V1(vtext_in);
PG_FUNCTION_INFO_V1(vtext_out);
PG_FUNCTION_INFO_V1(vtext_ne_const);
PG_FUNCTION_INFO_V1(vtext_length);

PG_FUNCTION_INFO_V1(vtext_larger);
PG_FUNCTION_INFO_V1(vtext_smaller);

PG_FUNCTION_INFO_V1(vtextregexreplace_noopt);
PG_FUNCTION_INFO_V1(vtextregexreplace);

static void
check_collation_set(Oid collid)
{
	if (!OidIsValid(collid))
	{
		/*
		 * This typically means that the parser could not resolve a conflict
		 * of implicit collations, so report it that way.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string comparison"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));
	}
} 

/* text_cmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
static int
text_cmp(text *arg1, text *arg2, Oid collid)
{
	char       *a1p,
			   *a2p;
	int         len1,
				len2;
	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	return varstr_cmp(a1p, len1, a2p, len2, collid);
}

Datum
vtext_length(PG_FUNCTION_ARGS)
{
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(0);
	vint4 *res = buildvint4(VECTOR_SIZE, vec_value->skipref);
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
		VDATUM_SET_DATUM(res, i, DirectFunctionCall1(textlen, arg));
	}

	PG_RETURN_POINTER(res);
}

Datum
vtext_ne_const(PG_FUNCTION_ARGS)
{
	Oid collid = PG_GET_COLLATION();
	bool result = false;
	vdatum *vec_value = NULL;
	int i = 0;
	vbool *res = NULL;

	check_collation_set(collid);

	if (lc_collate_is_c(collid) ||
			collid == DEFAULT_COLLATION_OID ||
			pg_newlocale_from_collation(collid)->deterministic)
	{
		Datum       arg1;
		Datum       arg2 = PG_GETARG_DATUM(1);
		Size        len1,
					len2;

		text *targ2 = DatumGetTextPP(arg2);
		len2 = toast_raw_datum_size(arg2);

		vec_value = (vdatum *) PG_GETARG_POINTER(0);

		res = buildvbool(VECTOR_SIZE, vec_value->skipref);

		for (i = 0; i < vec_value->dim; i++)
		{
			VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));

			if (vec_value->skipref[i])
				continue;

			if (VDATUM_ISNULL(vec_value, i))
				continue;

			arg1 = VDATUM_DATUM(vec_value, i);

			/* See comment in texteq() */
			len1 = toast_raw_datum_size(arg1);
			if (len1 != len2)
				result = true;
			else
			{
				text       *targ1 = DatumGetTextPP(arg1);
				result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2),
							len1 - VARHDRSZ) != 0);

				if ((void *)arg1 != (void *)targ1)
					pfree(targ1);
			}

			//vec_value->skipref[i] = !result;
			VDATUM_SET_DATUM(res, i, result);
		}

		if ((void *)arg2 != (void *)targ2)
			pfree(targ2);
	}
	else
	{
		text	   *arg1;
		text       *arg2 = PG_GETARG_TEXT_PP(1);
		vec_value = (vdatum *) PG_GETARG_POINTER(0);

		res = buildvbool(VECTOR_SIZE, vec_value->skipref);

		for (i = 0; i < vec_value->dim; i++)
		{
			VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));

			if (vec_value->skipref[i])
				continue;

			if (VDATUM_ISNULL(vec_value, i))
				continue;

			arg1 = DatumGetTextPP(VDATUM_DATUM(vec_value, i));

			/* text_cmp */
			result = (text_cmp(arg1, arg2, collid) != 0);

			if ((void *)arg1 != (void *)VDATUM_DATUM(vec_value, i))
				pfree(arg1);

			VDATUM_SET_DATUM(res, i, result);
		}

		PG_FREE_IF_COPY(arg2, 1);
	}

	PG_RETURN_POINTER(res);
}

/* min/max(text) -- transition function */
static text *
gamma_minmax_text_vector(vdatum *vec_value, Oid colloid, bool max)
{
	int i = 0;
	short indexarr = -1;
	text *result = NULL;
	bool first = true;
	text *arg;

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
				result = DatumGetTextPP(VDATUM_DATUM(vec_value, indexarr));
				first = false;
				i++;
				continue;
			}
			
			arg = DatumGetTextPP(VDATUM_DATUM(vec_value, indexarr));
			if (max)
			{
				result = ((text_cmp(result, arg, colloid) > 0) ? result : arg);
			}
			else
			{
				/* min */
				result = ((text_cmp(result, arg, colloid) < 0) ? result : arg);
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
				result = DatumGetTextPP(VDATUM_DATUM(vec_value, i));
				first = false;
				continue;
			}

			arg = DatumGetTextPP(VDATUM_DATUM(vec_value, i));

			if (max)
			{
				result = ((text_cmp(result, arg, colloid) > 0) ? result : arg);
			}
			else
			{
				/* min */
				result = ((text_cmp(result, arg, colloid) < 0) ? result : arg);
			}
		}
	}

	return result;
}

Datum
vtext_smaller(PG_FUNCTION_ARGS)
{
	Oid colloid = PG_GET_COLLATION();
	bool first = PG_ARGISNULL(0);
	text *arg1;// = PG_GETARG_TEXT_PP(0);
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);
	text *arg2 = gamma_minmax_text_vector(vec_value, colloid, false/*min*/);

	if (first)
		PG_RETURN_TEXT_P(arg2);

	arg1 = PG_GETARG_TEXT_PP(0);
	arg1 = (text_cmp(arg1, arg2, colloid) < 0 ? arg1 : arg2);
	PG_RETURN_TEXT_P(arg1);
}

Datum
vtext_larger(PG_FUNCTION_ARGS)
{
	Oid colloid = PG_GET_COLLATION();
	bool first = PG_ARGISNULL(0);
	text *arg1;// = PG_GETARG_TEXT_PP(0);
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(1);
	text *arg2 = gamma_minmax_text_vector(vec_value, colloid, true/*max*/);

	if (first)
		PG_RETURN_TEXT_P(arg2);

	arg1 = PG_GETARG_TEXT_PP(0);
	arg1 = (text_cmp(arg1, arg2, colloid) > 0 ? arg1 : arg2);
	PG_RETURN_TEXT_P(arg1);
}

/*
 * vtextregexreplace_noopt()
 *      Return a string matched by a regular expression, with replacement.
 *
 * This version doesn't have an option argument: we default to case
 * sensitive match, replace the first instance only.
 */
Datum
vtextregexreplace_noopt(PG_FUNCTION_ARGS)
{
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(0);
	vtext *res = buildvtext(VECTOR_SIZE, vec_value->skipref);
	int i;
	text       *s;// = PG_GETARG_TEXT_PP(0);
	text       *p = PG_GETARG_TEXT_PP(1);
	text       *r = PG_GETARG_TEXT_PP(2);
	//regex_t    *re;

	//re = RE_compile_and_cache(p, REG_ADVANCED, PG_GET_COLLATION());

	for (i = 0; i < vec_value->dim; i++)
	{
		VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));
		VDATUM_SET_DATUM(res, i, Int32GetDatum(0));

		if (vec_value->skipref[i])
			continue;

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(res, i, true);
			continue;
		}

		s = DatumGetTextPP(VDATUM_DATUM(vec_value, i));
		VDATUM_SET_DATUM(res, i, 
			PointerGetDatum(replace_text_regexp(s, p, r,
												REG_ADVANCED, PG_GET_COLLATION(),
												0, 1)));
	}

	PG_RETURN_POINTER(res);
}

/*
 * vtextregexreplace()
 *		Return a string matched by a regular expression, with replacement.
 */
Datum
vtextregexreplace(PG_FUNCTION_ARGS)
{
	vdatum *vec_value = (vdatum *)PG_GETARG_POINTER(0);
	vtext *res = buildvtext(VECTOR_SIZE, vec_value->skipref);
	int i;
	text       *s;// = PG_GETARG_TEXT_PP(0);
	text       *p = PG_GETARG_TEXT_PP(1);
	text       *r = PG_GETARG_TEXT_PP(2);
	//text       *opt = PG_GETARG_TEXT_PP(3);
	//regex_t    *re;
	//pg_re_flags flags;

	//parse_re_flags(&flags, opt);

	//re = RE_compile_and_cache(p, REG_ADVANCED/*flags.cflags*/, PG_GET_COLLATION());

	for (i = 0; i < vec_value->dim; i++)
	{
		VDATUM_SET_ISNULL(res, i, VDATUM_ISNULL(vec_value, i));
		VDATUM_SET_DATUM(res, i, Int32GetDatum(0));

		if (vec_value->skipref[i])
			continue;

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(res, i, true);
			continue;
		}

		s = DatumGetTextPP(VDATUM_DATUM(vec_value, i));
		VDATUM_SET_DATUM(res, i, //TODO:
			PointerGetDatum(replace_text_regexp(s, p, r,
												REG_ADVANCED, PG_GET_COLLATION(),
												0, 1)));
	}

	PG_RETURN_POINTER(res);
}

uint32
gamma_hash_text(text *key, Oid collid)
{
	pg_locale_t mylocale = 0;
	Datum		result;

	if (!collid)
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string hashing"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));

	if (!lc_collate_is_c(collid))
		mylocale = pg_newlocale_from_collation(collid);

	if (!mylocale || mylocale->deterministic)
	{
		result = gamma_hash_bytes((const char *) VARDATA_ANY(key),
						  VARSIZE_ANY_EXHDR(key));
	}
	else
	{
#ifdef USE_ICU
		if (mylocale->provider == COLLPROVIDER_ICU)
		{
			int32_t		ulen = -1;
			UChar	   *uchar = NULL;
			Size		bsize;
			uint8_t    *buf;

			ulen = icu_to_uchar(&uchar, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

			bsize = ucol_getSortKey(mylocale->info.icu.ucol,
									uchar, ulen, NULL, 0);
			buf = palloc(bsize);
			ucol_getSortKey(mylocale->info.icu.ucol,
							uchar, ulen, buf, bsize);
			pfree(uchar);

			result = gamma_hash_bytes((const char *) buf, bsize);

			pfree(buf);
		}
		else
#endif
			/* shouldn't happen */
			elog(ERROR, "unsupported collprovider: %c", mylocale->provider);
	}

	return result;
}
