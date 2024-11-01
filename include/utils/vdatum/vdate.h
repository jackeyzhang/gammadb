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

#ifndef VECTOR_ENGINE_VDATUM_VDATE_H
#define VECTOR_ENGINE_VDATUM_VDATE_H
#include "postgres.h"
#include "fmgr.h"
typedef struct vdatum vdate;
extern vdate* buildvdate(int dim, bool *skip);

/* vdate oper op const */
extern Datum vdate_mi_interval(PG_FUNCTION_ARGS);

/* vdate oper cmp const */
extern Datum vdate_le_timestamp(PG_FUNCTION_ARGS);
extern Datum vdate_le(PG_FUNCTION_ARGS);
extern Datum vdate_ge_const(PG_FUNCTION_ARGS);

extern Datum vdate_in(PG_FUNCTION_ARGS);
extern Datum vdate_out(PG_FUNCTION_ARGS);

#endif
