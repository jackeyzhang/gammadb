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

#include "catalog/pg_class.h"

#include "optimizer/gamma_checker.h"
#include "optimizer/gamma_paths.h"
#include "optimizer/paths.h"

static void gamma_vec_inner_and_outer(PlannerInfo *root,
		RelOptInfo *joinrel,
		RelOptInfo *outerrel,
		RelOptInfo *innerrel,
		JoinType join_type,
		JoinPathExtraData *extra);

static set_join_pathlist_hook_type set_join_pathlist_prev = NULL;

void
gamma_path_join_methods(void)
{
	static bool gamma_path_join_initialized = false;

	if (!gamma_path_join_initialized)
	{
		set_join_pathlist_prev = set_join_pathlist_hook;
		set_join_pathlist_hook = gamma_vec_inner_and_outer;

		gamma_path_join_initialized = true;
	}

	return;
}

static void
gamma_vec_inner_and_outer(PlannerInfo *root,
		RelOptInfo *joinrel,
		RelOptInfo *outerrel,
		RelOptInfo *innerrel,
		JoinType join_type,
		JoinPathExtraData *extra)
{
	if (set_join_pathlist_prev)
		set_join_pathlist_prev(root,joinrel,outerrel,innerrel,join_type,extra);

	//TODO:
	//	1.check if can add vec custom node to exist paths
	//		1.1 if exist paths use sub custom node:
	//				1.1.1 find non-vec path replace or add dec node to sub custom
	//				1.1.2 check if this join node can be vectorized
	//
	//	2.add vec node to paths
	//	3.enumertic some join path and try to add vec node on it
	//	4.if there are vec node on sub path, try to add devec node on it and
	//	  enumeric it with devec path on other nodes(donot add devec node on
	//	  scan path, it is no need)
	//
}
