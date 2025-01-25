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

#ifndef GAMMA_CONVERTER_H
#define GAMMA_CONVERTER_H

#include "nodes/nodes.h"

extern Plan* gamma_vec_convert_plan(Node *node);
extern Node* gamma_vec_convert_node(Node *node);
extern Plan* gamma_convert_plantree(Plan *plan, bool devec);

#endif /* GAMMA_CONVERTER_H */
