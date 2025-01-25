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

#include "fmgr.h"
#include "optimizer/planner.h"
#include "executor/nodeCustom.h"
#include "utils/guc.h"

#include "executor/gamma_devectorize.h"
#include "executor/gamma_vec_tablescan.h"
#include "executor/vector_tuple_slot.h"
#include "optimizer/gamma_converter.h"
#include "optimizer/gamma_paths.h"


bool enable_gammadb = false;
bool enable_gammadb_notice = false;


static planner_hook_type planner_hook_prev = NULL;

static PlannedStmt* gamma_vec_planner(Query	*parse,
		const char* query_string,
		int	cursorOptions,
		ParamListInfo boundParams);

void
gamma_path_planner_methods(void)
{
	static bool gamma_path_planner_initialized = false;

	if (!gamma_path_planner_initialized)
	{
		planner_hook_prev = planner_hook;
		planner_hook = gamma_vec_planner;

		gamma_path_planner_initialized = true;
	}

	return;
}

static PlannedStmt *
gamma_vec_planner(Query	*parse,
		const char* query_string,
		int cursorOptions,
		ParamListInfo boundParams)
{
	PlannedStmt	*stmt;
	List *subplans;
	ListCell *lc;

	if (planner_hook_prev)
		stmt = planner_hook_prev(parse, query_string,
								 cursorOptions, boundParams);
	else
		stmt = standard_planner(parse, query_string,
								 cursorOptions, boundParams);

	stmt->planTree = gamma_convert_plantree(stmt->planTree, true);

	subplans = stmt->subplans;
	stmt->subplans = NULL;
	foreach (lc, subplans)
	{
		Plan *sub_plan_tree = (Plan *) lfirst(lc);
		sub_plan_tree = gamma_convert_plantree(sub_plan_tree, true);
		stmt->subplans = lappend(stmt->subplans, sub_plan_tree);
	}

	return stmt;
}
