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

#ifndef GAMMA_VACUUM_H
#define GAMMA_VACUUM_H

#include "commands/vacuum.h"
#include "nodes/parsenodes.h"

typedef struct GammaVacuumContext
{
	List *gamma_rels;
	List *other_rels;
} GammaVacuumContext;

/* Use VACOPT_GAMMA_ANALYZE to distinguish between manual-analyze and autoanalyze */
#define VACOPT_GAMMA_ANALYZE 0x10000000

extern void gamma_analyze_extract_rels(VacuumStmt *vacstmt, GammaVacuumContext *gvctx);
extern bool gamma_autoanalyze_needed(Oid relid);
extern void gamma_autoanalyze_rel(Oid relid, VacuumParams *params,
								BufferAccessStrategy bstrategy);
extern bool gamma_analyze_check(VacuumStmt *vacstmt);
extern void gamma_exec_vacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel);

#endif /* GAMMA_VACUUM_H */
