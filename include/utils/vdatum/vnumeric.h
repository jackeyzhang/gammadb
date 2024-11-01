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

#ifndef VECTOR_ENGINE_VDATUM_VNUMERIC_H
#define VECTOR_ENGINE_VDATUM_VNUMERIC_H
#include "postgres.h"

#include "fmgr.h"
#include "utils/numeric.h"

#include "vdatum.h"


typedef vdatum vnumeric;

extern vnumeric* buildvnumeric(int dim, bool *skip);

extern Datum vnumeric_in(PG_FUNCTION_ARGS);
extern Datum vnumeric_out(PG_FUNCTION_ARGS);

extern uint32 gamma_hash_numeric(Numeric key);
#endif
