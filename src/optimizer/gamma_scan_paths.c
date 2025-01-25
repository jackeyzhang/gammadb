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

#include "access/table.h"
#include "catalog/pg_class.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"

#include "executor/gamma_vec_tablescan.h"
#include "executor/gamma_indexscan.h"
#include "executor/gamma_indexonlyscan.h"
#include "optimizer/gamma_checker.h"
#include "optimizer/gamma_paths.h"
#include "optimizer/paths.h"
#include "storage/ctable_am.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

#ifdef _GAMMAX_
#include "optimizer/gammax_scan_paths.h"
#endif

static void gamma_scan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte);
static void gamma_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte);
static void gamma_plain_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte);
static void gamma_partial_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte);
static void gamma_indexscan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte);
static void gamma_cost_seqscan(CustomPath *cpath, Path *scanpath,
							   PlannerInfo *root, RelOptInfo *baserel);

static set_rel_pathlist_hook_type set_rel_pathlist_prev = NULL;

void
gamma_path_scan_methods(void)
{
	static bool gamma_path_scan_initialized = false;

	if (!gamma_path_scan_initialized)
	{
		set_rel_pathlist_prev = set_rel_pathlist_hook;
		set_rel_pathlist_hook = gamma_scan_paths;

		gamma_path_scan_initialized = true;
	}

	return;
}

static List *
gamma_get_proj_for_scan_path(PlannerInfo *root, RelOptInfo *baserel,
							Index rtindex, RangeTblEntry *rte)
{
	List *proj_list = NULL;
	Relation rel = table_open(rte->relid, AccessShareLock);
	TupleDesc tupledesc = rel->rd_att;
	if (baserel->baserestrictinfo != NULL)
	{
		proj_list =
			gamma_pull_vars_of_level((Node *) baserel->baserestrictinfo, 0);
	}

	/* use first attribute */
	if (proj_list == NULL)
	{
		Var *var = makeVar(rtindex, 1,
				tupledesc->attrs[0].atttypid,
				tupledesc->attrs[0].atttypmod,
				tupledesc->attrs[0].attcollation,
				0);

		proj_list = lappend(proj_list, var);
	}

	table_close(rel, AccessShareLock);

	return proj_list;
}

static void
gamma_plain_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte)
{
	List *new_pathlist = NULL;
	ListCell *lc;

	foreach (lc, baserel->pathlist)
	{
		CustomPath *cpath = NULL;
		Path *scanpath = (Path *)lfirst(lc);
		Path *newpath = NULL;

		/* all paths need to reserve (when process seqscan paths) */
		new_pathlist = lappend(new_pathlist, scanpath);

		if (scanpath->pathtype != T_SeqScan)
		{
			continue;
		}

		/* check if scan path can be vectorized */
		if (!gamma_vec_check_path(root, baserel, scanpath))
		{
			continue;
		}

		newpath = makeNode(Path);
		memcpy(newpath, scanpath, sizeof(Path));
		

		cpath = makeNode(CustomPath);

		cpath->path.pathtype			= T_CustomScan;
		cpath->path.parent				= baserel;
		cpath->path.pathtarget			= baserel->reltarget;
		cpath->path.param_info			= scanpath->param_info;
		cpath->path.parallel_aware		= scanpath->parallel_aware;
		cpath->path.parallel_safe		= scanpath->parallel_safe;
		cpath->path.parallel_workers	= scanpath->parallel_workers;
		cpath->path.rows				= scanpath->rows;
		cpath->path.pathkeys			= NIL;  /* unsorted results */
		cpath->flags					= 0;
		cpath->custom_paths				= list_make1(newpath);
		cpath->custom_private			= NULL;
		cpath->methods = (CustomPathMethods *)gamma_vec_tablescan_path_methods();

		/* compute the cost for vectorized seq scan */
		gamma_cost_seqscan(cpath, newpath, root, baserel);

		new_pathlist = lcons(cpath, new_pathlist);
	}

	baserel->pathlist = new_pathlist;

	return;
	
}

static void
gamma_partial_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte)
{
	List *new_pathlist = NULL;
	ListCell *lc;

	foreach (lc, baserel->partial_pathlist)
	{
		CustomPath *cpath = NULL;
		Path *scanpath = (Path *)lfirst(lc);
		Path *newpath = NULL;

		/* all paths need to reserve (when process seqscan paths) */
		new_pathlist = lappend(new_pathlist, scanpath);

		if (scanpath->pathtype != T_SeqScan)
		{
			continue;
		}

		/* check if scan path can be vectorized */
		if (!gamma_vec_check_path(root, baserel, scanpath))
		{
			continue;
		}

		newpath = makeNode(Path);
		memcpy(newpath, scanpath, sizeof(Path));

		cpath = makeNode(CustomPath);

		cpath->path.pathtype			= T_CustomScan;
		cpath->path.parent				= baserel;
		cpath->path.pathtarget			= baserel->reltarget;
		cpath->path.param_info			= scanpath->param_info;
		cpath->path.parallel_aware		= scanpath->parallel_aware;
		cpath->path.parallel_safe		= scanpath->parallel_safe;
		cpath->path.parallel_workers	= scanpath->parallel_workers;
		cpath->path.rows				= scanpath->rows;
		cpath->path.pathkeys			= NIL;  /* unsorted results */
		cpath->flags					= 0;
		cpath->custom_paths				= list_make1(newpath);
		cpath->custom_private			= NULL;
		cpath->methods = (CustomPathMethods *)gamma_vec_tablescan_path_methods();

		/* compute the cost for vectorized seq scan */
		gamma_cost_seqscan(cpath, newpath, root, baserel);

		new_pathlist = lcons(cpath, new_pathlist);
	}

	baserel->partial_pathlist = new_pathlist;

	return;
	
}
static void
gamma_tablescan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte)
{
	gamma_plain_tablescan_paths(root, baserel, rtindex, rte);
	gamma_partial_tablescan_paths(root, baserel, rtindex, rte);	
}

static void
gamma_scan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte)
{
	Oid relid = InvalidOid;

	if (set_rel_pathlist_prev)
		set_rel_pathlist_prev(root, baserel, rtindex, rte);

	if (IS_DUMMY_REL(baserel))
	{
		/* We already proved the relation empty, so nothing more to do */
		return;
	}
	else if (rte->inh)
	{
		return;
	}
	else
	{
		switch (baserel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					return;
				}
				else if (rte->tablesample != NULL)
				{
					return;
				}
				else
				{
					relid = rte->relid;
				}
				break;
			default:
				break;
		}
	}

	if (relid == InvalidOid)
		return;


	/* for count(*) */
	if (baserel->reltarget && baserel->reltarget->exprs == NULL)
	{
		baserel->reltarget->exprs = gamma_get_proj_for_scan_path(root, baserel,
																rtindex, rte);
	}

	gamma_tablescan_paths(root, baserel, rtindex, rte);
	gamma_indexscan_paths(root, baserel, rtindex, rte);

#ifdef _GAMMAX_
	gamma_colindex_scan_paths(root, baserel, rtindex, rte);
#endif
}

static void
gamma_cost_seqscan(CustomPath *cpath, Path *scanpath,
				   PlannerInfo *root, RelOptInfo *baserel)
{
	/*TODO: now is a simple model */

	RangeTblEntry *rte = root->simple_rte_array[baserel->relid];
	Relation relation = table_open(rte->relid, AccessShareLock);

	if (relation->rd_tableam == ctable_tableam_routine())
	{
		cpath->path.startup_cost = scanpath->startup_cost;
		cpath->path.total_cost = scanpath->total_cost / VECTOR_SIZE;
		cpath->path.total_cost = cpath->path.total_cost * 0.75;
	}
	else
	{
		cpath->path.startup_cost = scanpath->startup_cost;
		cpath->path.total_cost = scanpath->total_cost;
	}

	table_close(relation, AccessShareLock);

	return;
}

static void
gamma_indexscan_paths(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Index rtindex,
			   RangeTblEntry *rte)
{
	Path *indexpath = NULL;
	CustomPath *cpath;
	List *new_pathlist = NULL;
	Relation rel;
	ListCell *lc;

	/* check if it is index on columnar tables */
	rel = table_open(rte->relid, AccessShareLock);
	if (rel->rd_tableam != ctable_tableam_routine())
	{
		table_close(rel, NoLock);
		return;
	}

	table_close(rel, NoLock);

	/* begin to process index paths */
	new_pathlist = NULL;
	foreach (lc, baserel->pathlist)
	{
		indexpath = (Path *) lfirst(lc);

		if (indexpath->pathtype != T_IndexScan &&
			indexpath->pathtype != T_IndexOnlyScan)
		{
			new_pathlist = lappend(new_pathlist, indexpath);
			continue;
		}

		cpath = makeNode(CustomPath);

		cpath->path.pathtype			= T_CustomScan;
		cpath->path.parent				= baserel;
		cpath->path.pathtarget			= baserel->reltarget;
		cpath->path.param_info			= indexpath->param_info;
		cpath->path.parallel_aware		= indexpath->parallel_aware;
		cpath->path.parallel_safe		= indexpath->parallel_safe;
		cpath->path.parallel_workers	= indexpath->parallel_workers;
		cpath->path.rows				= indexpath->rows;
		cpath->path.pathkeys			= NIL;  /* unsorted results */
		cpath->flags					= 0;
		cpath->custom_paths				= list_make1(indexpath);
		cpath->custom_private			= NULL;

		if (indexpath->pathtype == T_IndexScan)
			cpath->methods = (CustomPathMethods *)gamma_indexscan_methods();
		else
			cpath->methods = (CustomPathMethods *)gamma_indexonlyscan_methods();

		cpath->path.startup_cost = indexpath->startup_cost;
		cpath->path.total_cost = indexpath->total_cost;

		new_pathlist = lappend(new_pathlist, cpath);
	}

	baserel->pathlist = new_pathlist;

	return;
}
