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

#include "optimizer/gamma_rewrite_grouping_const.h"

/* GUC */
bool gammadb_rewrite_grouping_const = true;

Query *
gamma_rewrite_grouping_const(Query *parse)
{
	ListCell *lcg;
	ListCell *lct;

	List *new_group_clause = NULL;

	if (!gammadb_rewrite_grouping_const)
		return parse;

	if (parse == NULL)
		return NULL;

	if (parse->groupClause == NULL)
		return parse;

	foreach (lcg, parse->groupClause)
	{
		bool is_const = false;
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lcg);

		foreach (lct, parse->targetList)
		{
			TargetEntry *te = (TargetEntry *) lfirst(lct);

			if (sgc->tleSortGroupRef == te->ressortgroupref)
			{
				if (IsA(te->expr, Const))
					is_const = true;

			}	
		}

		if (!is_const)
			new_group_clause = lappend(new_group_clause, sgc);
	}

	parse->groupClause = new_group_clause;

	return parse;
}
