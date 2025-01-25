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

#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"
#include "utils/vdatum/vnumeric.h"

PG_FUNCTION_INFO_V1(vnumeric_in);
PG_FUNCTION_INFO_V1(vnumeric_out);

vnumeric* buildvnumeric(int dim, bool *skip)
{
	return (vnumeric*)buildvdatum(TIMESTAMPOID, dim, skip);
}

Datum vnumeric_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vnumeric_in not supported");
}
Datum vnumeric_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vnumeric_out not supported");
}

#define NUMERIC_SIGN_MASK   0xC000
#define NUMERIC_POS         0x0000
#define NUMERIC_NEG         0x4000
#define NUMERIC_SHORT       0x8000
#define NUMERIC_SPECIAL     0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_SHORT(n)     (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_SPECIAL(n)   (NUMERIC_FLAGBITS(n) == NUMERIC_SPECIAL)

uint32
gamma_hash_numeric(Numeric key)
{
	Datum d = NumericGetDatum(key);
	char *str = DatumGetCString(DirectFunctionCall1(numeric_out, d));
	return gamma_hash_bytes((const char *) str, strlen(str));
}
