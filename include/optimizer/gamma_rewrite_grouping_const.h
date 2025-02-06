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

#ifndef GAMMA_REWRITE_GROUPING_CONST_H
#define GAMMA_REWRITE_GROUPING_CONST_H

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"

extern Query * gamma_rewrite_grouping_const(Query *parse);

#endif /* GAMMA_REWRITE_GROUPING_CONST_H */
