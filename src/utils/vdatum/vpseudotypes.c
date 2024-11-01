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
#include "utils/vdatum/vpseudotypes.h"
#include "utils/vdatum/vdatum.h"

PG_FUNCTION_INFO_V1(vany_in);
PG_FUNCTION_INFO_V1(vany_out);


vany* buildvany(int dim, bool *skip)
{
	return (vany *)buildvdatum(ANYOID, dim, skip);	
}

Datum vany_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_in not supported");
}

Datum vany_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vany_out not supported");
}
