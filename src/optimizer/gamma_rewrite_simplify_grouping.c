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

#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_clause.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"

#include "optimizer/gamma_rewrite_simplify_grouping.h"

/* GUC */
bool gammadb_rewrite_simplify_grouping = true;

static Node * gamma_check_can_simplify(Node *expr);
static bool gamma_check_can_simplify_walker(Node *node, void *ctx);
static TargetEntry * gamma_find_tle_by_node(Node *node, List *tlist);
static bool gamma_find_sgc_by_ref(int32 ref, List *sgclist);


Query *
gamma_rewrite_simplify_grouping(Query *parse)
{
	ListCell *lcg;

	List *new_group_clause = NULL;
	List *new_target_list = (List *)copyObject(parse->targetList);

	if (!gammadb_rewrite_simplify_grouping)
		return parse;

	if (parse == NULL)
		return NULL;

	if (parse->groupClause == NULL)
		return parse;

	if (parse->groupingSets != NULL)
		return parse;

	foreach (lcg, parse->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lcg);
		TargetEntry *gte = get_sortgroupclause_tle(sgc, parse->targetList);
		Var *var;
		TargetEntry *tte;

		if (IsA(gte->expr, Var))
		{
			new_group_clause = lappend(new_group_clause, sgc);
			continue;
		}

		var = (Var *) gamma_check_can_simplify((Node *)gte->expr);

		if (var == NULL)
		{
			new_group_clause = lappend(new_group_clause, sgc);
			continue;
		}

		tte = gamma_find_tle_by_node((Node *)var, new_target_list);

		if (tte == NULL)
		{
			TargetEntry *nte = makeTargetEntry((Expr *)var,
												list_length(new_target_list) + 1,
												NULL, true);
			assignSortGroupRef(nte, new_target_list);
			sgc->tleSortGroupRef = nte->ressortgroupref;

			new_target_list = lappend(new_target_list, nte);
			new_group_clause = lappend(new_group_clause, sgc);
			continue;
		}
		else
		{
			if (tte->ressortgroupref == 0)
			{
				assignSortGroupRef(tte, new_target_list);
				sgc->tleSortGroupRef = tte->ressortgroupref;
				new_group_clause = lappend(new_group_clause, sgc);
			}
			else
			{
				if (gamma_find_sgc_by_ref(tte->ressortgroupref, new_group_clause))
				{
					/* useless sgc */
					continue;
				}

				sgc->tleSortGroupRef = tte->ressortgroupref;
				new_group_clause = lappend(new_group_clause, sgc);
			}
		}
	}

	parse->targetList = new_target_list;
	parse->groupClause = new_group_clause;

	return parse;
}

/* check target entry */
typedef struct GammaCanSimplifyContext
{
	Var *var;
	bool pass;
} GammaCanSimplifyContext;

static Node *
gamma_check_can_simplify(Node *expr)
{
	GammaCanSimplifyContext context;
	context.var = NULL;
	context.pass = true;

	gamma_check_can_simplify_walker(expr, &context);

	if (context.pass)
		return (Node *)context.var;
	else
		return NULL;
}

static bool
gamma_check_can_simplify_walker(Node *node, void *ctx)
{
	GammaCanSimplifyContext *context = (GammaCanSimplifyContext *) ctx;

	if (!context->pass)
		return false;

	if (IsA(node, OpExpr))
	{
		OpExpr *op_expr = (OpExpr *) node;
		char *op = get_opname(op_expr->opno);
		if (!strstr("+-*/%", op))
		{
			context->pass = false;
			return false;
		}
	}
	else if (IsA(node, Var))
	{
		if (context->var != NULL)
			context->pass = false;

		context->var = (Var *) node;
	}
	else if (IsA(node, Const) || IsA(node, RelabelType))
	{
		/* do nothing */
	}
	else
	{
		context->pass = false;
		return false;
	}

	return expression_tree_walker(node, gamma_check_can_simplify_walker,
									(void *)context);
}

static TargetEntry *
gamma_find_tle_by_node(Node *node, List *tlist)
{
	ListCell *lct;

	foreach (lct, tlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lct);

		if (equal((const void *)node, (const void *)te->expr))
		{
			return te;
		}	
	}

	return NULL;
}

static bool
gamma_find_sgc_by_ref(int32 ref, List *sgclist)
{
	ListCell *lcg;

	foreach (lcg, sgclist)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lcg);

		if (sgc->tleSortGroupRef == ref)
		{
			return true;
		}
	}

	return false;

}
