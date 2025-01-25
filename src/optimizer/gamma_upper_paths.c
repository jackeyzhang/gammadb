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
#include "optimizer/planner.h"

#include "executor/gamma_vec_agg.h"
#include "executor/gamma_devectorize.h"
#include "executor/gamma_vec_result.h"
#include "executor/gamma_vec_sort.h"
#include "executor/gamma_vec_tablescan.h"
#include "optimizer/gamma_checker.h"
#include "optimizer/gamma_paths.h"
#include "optimizer/paths.h"
#include "utils/vdatum/vdatum.h"


static void gamma_vec_upper_paths(PlannerInfo *root,
		UpperRelationKind stage,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra);
static Path * gamma_create_devector_path(PlannerInfo *root, Path *path);
static void gamma_cost_devector(CustomPath *cpath, Path *path,
								PlannerInfo *root, RelOptInfo *rel);
//static void gamma_process_top_node(PlannerInfo *root, RelOptInfo *final_rel);


static create_upper_paths_hook_type create_upper_paths_prev = NULL;

void
gamma_path_upper_methods(void)
{
	static bool gamma_path_upper_initialized = false;

	if (!gamma_path_upper_initialized)
	{
		create_upper_paths_prev = create_upper_paths_hook;
		create_upper_paths_hook = gamma_vec_upper_paths;

		gamma_path_upper_initialized = true;
	}

	return;
}

static Path *
gamma_vec_projection_path(PlannerInfo *root,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra,
		Path *path)
{
	CustomPath *cpath;

	Assert (gamma_vec_check_path(root, path->parent, path));

	cpath = makeNode(CustomPath);

	cpath->path.pathtype			= T_CustomScan;
	cpath->path.parent				= group_rel;
	cpath->path.pathtarget			= path->pathtarget;
	cpath->path.param_info			= path->param_info;
	cpath->path.parallel_aware		= path->parallel_aware;
	cpath->path.parallel_safe		= path->parallel_safe;
	cpath->path.parallel_workers	= path->parallel_workers;
	cpath->path.rows				= path->rows;
	cpath->path.pathkeys			= path->pathkeys;
	cpath->flags					= 0;
	cpath->custom_paths				= list_make1(path);
	cpath->custom_private			= NULL;
	cpath->methods = (CustomPathMethods *)gamma_vec_result_path_methods();

	/* TODO: compute the cost for vectorized aggregate */
	cpath->path.startup_cost = path->startup_cost - 10;
	cpath->path.total_cost = path->total_cost - 10;

	return (Path *) cpath;
}

//TODO: move to correct directory
static Path *
gamma_vec_sort_path(PlannerInfo *root,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra,
		Path *path)
{
	CustomPath *cpath;

	Assert (gamma_vec_check_path(root, path->parent, path));

	cpath = makeNode(CustomPath);

	cpath->path.pathtype			= T_CustomScan;
	cpath->path.parent				= group_rel;
	cpath->path.pathtarget			= path->pathtarget;
	cpath->path.param_info			= path->param_info;
	cpath->path.parallel_aware		= path->parallel_aware;
	cpath->path.parallel_safe		= path->parallel_safe;
	cpath->path.parallel_workers	= path->parallel_workers;
	cpath->path.rows				= path->rows;
	cpath->path.pathkeys			= path->pathkeys;
	cpath->flags					= 0;
	cpath->custom_paths				= list_make1(path);
	cpath->custom_private			= NULL;
	cpath->methods = (CustomPathMethods *)gamma_vec_sort_path_methods();

	/* TODO: compute the cost for vectorized aggregate */
	cpath->path.startup_cost = path->startup_cost - 10;
	cpath->path.total_cost = path->total_cost - 10;

	return (Path *) cpath;
}

static Path *
gamma_vec_agg_path(PlannerInfo *root,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra,
		Path *path)
{
	CustomPath *cpath;

	cpath = makeNode(CustomPath);

	cpath->path.pathtype			= T_CustomScan;
	cpath->path.parent				= group_rel;
	cpath->path.pathtarget			= path->pathtarget;
	cpath->path.param_info			= path->param_info;
	cpath->path.parallel_aware		= path->parallel_aware;
	cpath->path.parallel_safe		= path->parallel_safe;
	cpath->path.parallel_workers	= path->parallel_workers;
	cpath->path.rows				= path->rows;
	cpath->path.pathkeys			= path->pathkeys;
	cpath->flags					= 0;
	cpath->custom_paths				= list_make1(path);
	cpath->custom_private			= NULL;
	cpath->methods = (CustomPathMethods *)gamma_vec_agg_path_methods();

	/* TODO: compute the cost for vectorized aggregate */
	cpath->path.startup_cost = path->startup_cost - 10;
	cpath->path.total_cost = path->total_cost - 10;

	return (Path *) cpath;
}

typedef enum GammaAggCheckType
{
	GAMMA_AGG_YES,
	GAMMA_AGG_NO,
	GAMMA_AGG_PASS,
} GammaAggCheckType;

static GammaAggCheckType
gamma_agg_path_checker(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *group_rel, void *extra,
						/*Path **parent_path,*/ Path **ppath)
{
	Path *subpath = NULL;
	Path *path = *ppath;
	Path *temp_path;
	GammaAggCheckType subresult = GAMMA_AGG_NO;

	/*TODO: need compare path->parent with input_rel ? */
	if (path->parent == input_rel && path->pathtype == T_CustomScan)
		return GAMMA_AGG_YES;
	else if (path->parent == input_rel && path->pathtype == T_SeqScan)
		return GAMMA_AGG_NO;
	else if (!IS_UPPER_REL(path->parent) &&
			 path->parent != input_rel && path->parent != group_rel)
		return GAMMA_AGG_NO;

	switch (path->pathtype)
	{
		case T_Result:
			{
				subpath = ((ProjectionPath *)path)->subpath;
				break;
			}
		case T_Sort:
			{
				subpath = ((SortPath *)path)->subpath;
				break;
			}
		case T_Agg:
			{
				subpath = ((AggPath *)path)->subpath;
				break;
			}
		case T_Gather:
			{
				subpath = ((GatherPath *)path)->subpath;
				break;
			}
		case T_GatherMerge:
			{
				subpath = ((GatherMergePath *)path)->subpath;
				break;
			}
		default:
			{
				//elog(ERROR, "gamma_agg_path_checker: unsupport path type");
				return GAMMA_AGG_PASS;
			}
	}

	subresult = gamma_agg_path_checker(root, input_rel, group_rel, extra, &subpath);
	if (subresult == GAMMA_AGG_NO)
		return subresult;
	
	if (!gamma_vec_check_path(root, path->parent, path))
		subresult = GAMMA_AGG_PASS;

	switch (path->pathtype)
	{
		case T_Result:
			{
				temp_path = (Path *)makeNode(ProjectionPath);
				memcpy(temp_path, path, sizeof(ProjectionPath));
				((ProjectionPath *)temp_path)->subpath = subpath;
				path = temp_path;

				if (subresult == GAMMA_AGG_YES)
				{
					path = (Path *) gamma_vec_projection_path(root,
							input_rel, group_rel, extra, temp_path);
				}

				break;
			}
		case T_Sort:
			{
				temp_path = (Path *)makeNode(SortPath);
				memcpy(temp_path, path, sizeof(SortPath));
				((SortPath *)temp_path)->subpath = subpath;
				path = temp_path;

				if (subresult == GAMMA_AGG_YES)
				{
					path = (Path *) gamma_vec_sort_path(root,
							input_rel, group_rel, extra, temp_path);
				}

				break;
			}
		case T_Agg:
			{
				temp_path = (Path *)makeNode(AggPath);
				memcpy(temp_path, path, sizeof(AggPath));
				((AggPath *)temp_path)->subpath = subpath;
				path = temp_path;

				if (subresult == GAMMA_AGG_YES)
				{
					path = gamma_vec_agg_path(root, input_rel, group_rel,
							extra, temp_path);
				}

				subresult = GAMMA_AGG_PASS;
				break;
			}
		case T_Gather:
			{
				temp_path = (Path *)makeNode(GatherPath);
				memcpy(temp_path, path, sizeof(GatherPath));
				path = temp_path;
				((GatherPath *)path)->subpath = subpath;
				subresult = GAMMA_AGG_PASS;
				break;
			}
		case T_GatherMerge:
			{
				temp_path = (Path *)makeNode(GatherMergePath);
				memcpy(temp_path, path, sizeof(GatherMergePath));
				path = temp_path;
				((GatherMergePath*)path)->subpath = subpath;
				subresult = GAMMA_AGG_PASS;
				break;
			}
		default:
			{
				elog(ERROR, "gamma_agg_path_checker: unsupport path type");
			}
	}

	*ppath = path;
	return subresult;
}


static void
gamma_vec_group_agg_paths(PlannerInfo *root,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra)
{
	ListCell *lc = NULL;
	List *vec_pathlist = NULL;
	foreach (lc, group_rel->pathlist)
	{
		Path *path = (Path *) lfirst(lc);
		Path *ppath = path;
		gamma_agg_path_checker(root, input_rel, group_rel, extra, &ppath);

		/* ppath: maybe NULL or new vec path */
		if (path != ppath)
			vec_pathlist = lappend(vec_pathlist, ppath);
		else
			vec_pathlist = lappend(vec_pathlist, path);
	}

	group_rel->pathlist = vec_pathlist;
	return;
}

static void
gamma_vec_order_paths(PlannerInfo *root,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra)
{
	ListCell *lc;
	//List *newlist = NULL;

	foreach (lc, group_rel->pathlist)
	{
		Path *path = (Path *) lfirst(lc);
		Path *checkpath = path;
		CustomPath *cpath;
		Path *aggpath;
		SortPath *sortpath;
		//Path *subpath;

		if (checkpath->pathtype == T_CustomScan)
			continue;

		if (IsA(checkpath, ProjectionPath))
		{
			ProjectionPath *ppath = (ProjectionPath *) checkpath;
			checkpath = ppath->subpath;
		}

		Assert(IsA(checkpath, SortPath));

		sortpath = (SortPath *) checkpath;
		checkpath = sortpath->subpath;

		if (checkpath == NULL || checkpath->pathtype != T_CustomScan)
			continue;

		/* it is a vectorized node */
		cpath = (CustomPath*) checkpath;

		/* if it is a vectorized Agg operator, its output is still a scalar */
		aggpath = (Path*) linitial(cpath->custom_paths);
		if (aggpath->pathtype == T_Agg)
			continue;

		checkpath = gamma_create_devector_path(root, checkpath);
		sortpath->subpath = checkpath;
	}

	return;
}


static void
gamma_vec_upper_paths(PlannerInfo *root,
		UpperRelationKind stage,
		RelOptInfo *input_rel,
		RelOptInfo *group_rel,
		void *extra)
{
	if (create_upper_paths_prev)
		(*create_upper_paths_prev)(root, stage, input_rel, group_rel, extra);

	if (stage == UPPERREL_GROUP_AGG)
	{
		gamma_vec_group_agg_paths(root, input_rel, group_rel, extra);
	}

	if (stage == UPPERREL_ORDERED)
	{
		gamma_vec_order_paths(root, input_rel, group_rel, extra);
	}

#if 0
	/* for top level plan node */
	if (stage == UPPERREL_FINAL && root->parent_root == NULL)
	{
		gamma_process_top_node(root, group_rel);
	}
#endif

	return;
}

static Path *
gamma_create_devector_path(PlannerInfo *root, Path *path)
{
	CustomPath *cpath = makeNode(CustomPath);

	Assert(path->pathtype == T_CustomScan);

	cpath->path.pathtype			= T_CustomScan;
	cpath->path.parent				= path->parent;
	cpath->path.pathtarget			= path->pathtarget;
	cpath->path.param_info			= path->param_info;
	cpath->path.parallel_aware		= path->parallel_aware;
	cpath->path.parallel_safe		= path->parallel_safe;
	cpath->path.parallel_workers	= path->parallel_workers;
	cpath->path.rows				= path->rows;
	cpath->path.pathkeys			= NIL;  /* unsorted results */
	cpath->flags					= 0;
	cpath->custom_paths				= list_make1(path);
	cpath->custom_private			= NULL;
	cpath->methods = (CustomPathMethods *)gamma_vec_devector_path_methods();

	gamma_cost_devector(cpath, path, root, path->parent);

	return (Path *) cpath;
}

static void
gamma_cost_devector(CustomPath *cpath, Path *path,
					PlannerInfo *root, RelOptInfo *rel)
{
	cpath->path.startup_cost = path->startup_cost;
	cpath->path.total_cost = path->total_cost * VECTOR_SIZE;
}

#if 0
static void
gamma_process_top_node(PlannerInfo *root, RelOptInfo *final_rel)
{
	ListCell *lc;
	List *newlist = NULL;

	foreach (lc, final_rel->pathlist)
	{
		Path *path = (Path *) lfirst(lc);
		Path *checkpath = path;
		CustomPath *cpath;
		Path *aggpath;

		if (IsA(checkpath, ProjectionPath))
		{
			ProjectionPath *ppath = (ProjectionPath *) checkpath;
			checkpath = ppath->subpath;
		}

		if (checkpath->pathtype != T_CustomScan)
		{
			newlist = lappend(newlist, path);
			continue;
		}

		/* it is a vectorized node */
		cpath = (CustomPath*) checkpath;

		/* if it is a vectorized Agg operator, its output is still a scalar */
		aggpath = (Path*) linitial(cpath->custom_paths);
		if (aggpath->pathtype == T_Agg)
		{
			newlist = lappend(newlist, path);
			continue;
		}

		path = gamma_create_devector_path(root, path);
		newlist = lappend(newlist, path);
	}

	final_rel->pathlist = newlist;
}
#endif
