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

#include "catalog/namespace.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "tcop/utility.h"

#include "commands/gamma_vacuum.h"
#include "storage/gamma_meta.h"
#include "tcop/gamma_utility.h"

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void gamma_ProcessUtility(PlannedStmt *pstmt,
						const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						QueryCompletion *qc);


void
gamma_utility_startup(void)
{
	static bool gamma_utility_initialized = false;

	if (!gamma_utility_initialized)
	{
		prev_ProcessUtility = ProcessUtility_hook;
		ProcessUtility_hook = gamma_ProcessUtility;

		gamma_utility_initialized = true;
	}

	return;
}
static void
gamma_ProcessUtility(PlannedStmt *pstmt,
						const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context,
						ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						QueryCompletion *qc)
{
	Node *parsetree = pstmt->utilityStmt;

	switch (nodeTag(parsetree))
	{
		/* do not support parallel when create index on gamma table */
		case T_IndexStmt:
			{
				IndexStmt  *stmt = (IndexStmt *) parsetree;
				Oid			relid;
				LOCKMODE	lockmode;

				lockmode = stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock;
				relid = RangeVarGetRelidExtended(stmt->relation, lockmode,
							0,
							RangeVarCallbackOwnsRelation,
							NULL);

				if (gamma_meta_is_gamma_table(relid))
				{
					if (stmt->concurrent)
						elog(ERROR, "Create index concurrently is not supported"
							   " on GAMMA table");

					(void) set_config_option("max_parallel_maintenance_workers",
							"0",
							PGC_USERSET, PGC_S_SESSION,
							GUC_ACTION_SAVE, true, 0, false);
				}
			}
			break;
		case T_VacuumStmt:
			{
				ParseState *pstate;
				GammaVacuumContext gvcontext;
				bool isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
				gvcontext.gamma_rels = NULL;
				gvcontext.other_rels = NULL;
				gamma_analyze_extract_rels((VacuumStmt *) parsetree, &gvcontext);

				if (gvcontext.gamma_rels != NULL)
				{
					pstate = make_parsestate(NULL);
					pstate->p_sourcetext = queryString;
					pstate->p_queryEnv = queryEnv;
					((VacuumStmt *) parsetree)->rels = gvcontext.gamma_rels;
					gamma_exec_vacuum(pstate, (VacuumStmt *) parsetree, isTopLevel);

				}

				if (gvcontext.other_rels != NULL)
				{
					((VacuumStmt *) parsetree)->rels = gvcontext.other_rels;
				}
				else
				{
					return;
				}
			}
			break;
		default:
			break;
	}

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree,
				context, params, queryEnv,
				dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
				context, params, queryEnv,
				dest, qc);

	return;	
}
