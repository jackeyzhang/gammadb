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

#ifndef _GAMMA_NODES_H_
#define _GAMMA_NODES_H_

#include "nodes/extensible.h"

typedef enum GammaNodeTag
{
	T_GammaPlanType,
} GammaNodeTag;

extern const char* GammaTagNames[];

typedef struct GammaNode
{
	ExtensibleNode extensible;
	GammaNodeTag gtag;
} GammaNode;

typedef enum GammaPlanTypeEnum
{
	GAMMA_PLAN_TYPE_NONE,
	GAMMA_PLAN_TYPE_TABLESCAN,
	GAMMA_PLAN_TYPE_INDEXSCAN,
	GAMMA_PLAN_TYPE_AGG,
	GAMMA_PLAN_TYPE_SORT,
	GAMMA_PLAN_TYPE_RESULT,
} GammaPlanTypeEnum;

typedef struct gamma_plan_type
{
	GammaNode header;
	GammaPlanTypeEnum plantype;
} gamma_plan_type;

#define GammaNodeTag(nodeptr) gamma_node_tag((Node*) nodeptr)

static inline int
gamma_node_tag(Node *node)
{
	Assert (IsA(node, ExtensibleNode));
	return ((GammaNode*)(node))->gtag;
}

static inline GammaNode *
gamma_new_node(size_t size, GammaNodeTag tag)
{
	GammaNode	   *result;

	Assert(size >= sizeof(GammaNode));
	result = (GammaNode *) palloc0(size);
	result->extensible.type = T_ExtensibleNode;
	result->extensible.extnodename = GammaTagNames[tag];
	result->gtag = (int) (tag);

	return result;
}

#define GammaIsA(nodeptr,_type_)	(GammaNodeTag(nodeptr) == T_##_type_)
#define GammaMakeNode(_type_) ((_type_ *) gamma_new_node(sizeof(_type_),T_##_type_))

extern void gamma_register_nodes(void);

#endif /* _GAMMA_NODES_H_ */
