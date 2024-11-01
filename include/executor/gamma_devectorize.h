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

#ifndef GAMMA_DEVECTORIZE_H
#define GAMMA_DEVECTORIZE_H

#include "nodes/extensible.h"

extern void gamma_vec_devector_init(void);
extern const CustomPathMethods* gamma_vec_devector_path_methods(void);
extern Plan* gamma_add_devector(CustomScan *cscan, Plan *subplan);

#endif   /* GAMMA_DEVECTORIZE_H */
