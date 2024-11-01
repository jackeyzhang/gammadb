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

#ifndef GAMMA_INDEXSCAN_H
#define GAMMA_INDEXSCAN_H

#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"

extern const CustomPathMethods* gamma_indexscan_methods(void);
extern void gamma_indexscan_init(void);
extern bool gamma_is_indexscan_customscan(CustomScan *cscan);

#endif   /* GAMMA_INDEXSCAN_H */
