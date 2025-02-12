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

#include "access/relation.h"
#include "access/relscan.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/syscache.h"

#include "commands/gamma_vacuum.h"
#include "storage/gamma_meta.h"

static int gamma_acquire_sample_rows(Relation onerel, int elevel,
					HeapTuple *rows, int targrows,
					double *totalrows, double *totaldeadrows);
static void gamma_vacuum(List *relations, VacuumParams *params,
	   BufferAccessStrategy bstrategy, bool isTopLevel);

#include "../src/postgres/commands/analyze.c"
#include "../src/postgres/commands/vacuum.c"

void
gamma_analyze_extract_rels(VacuumStmt *vacstmt, GammaVacuumContext *gvctx)
{
	ListCell *lc;
	List *relations = vacstmt->rels;

	if (relations == NULL)
		relations = get_all_vacuum_rels(0);

	foreach(lc, relations)
	{
		VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
		Oid relid = vrel->oid;
		if (!OidIsValid(vrel->oid))
		{
			relid = RangeVarGetRelidExtended(vrel->relation, AccessShareLock,
										0, NULL, NULL);
		}

		if (gamma_meta_is_gamma_table(relid))
			gvctx->gamma_rels = lappend(gvctx->gamma_rels, vrel);
		else
			gvctx->other_rels = lappend(gvctx->other_rels, vrel);
	}

	return;
}

bool
gamma_autoanalyze_needed(Oid relid)
{
	float4 reltuples;		/* pg_class.reltuples */
	float4 anlthresh;
	float4 anltuples;

	PgStat_StatTabEntry *tabentry;
	Form_pg_class classForm;
	HeapTuple classTup;

	if (!AutoVacuumingActive())
		return false;

	classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(classTup))
		return NULL;

	classForm = (Form_pg_class) GETSTRUCT(classTup);
	tabentry = pgstat_fetch_stat_tabentry_ext(classForm->relisshared, relid);
	if (tabentry == NULL)
		return false;

	reltuples = classForm->reltuples;
	anltuples = tabentry->changes_since_analyze;

	if (reltuples < 0)
		reltuples = 0;

	anlthresh = (float4) autovacuum_anl_thresh + autovacuum_anl_scale * reltuples;

	heap_freetuple(classTup);

	return (anltuples > anlthresh);
}

void
gamma_autoanalyze_rel(Oid relid, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	Relation	onerel;
	int			elevel;
	AcquireSampleRowsFunc acquirefunc = NULL;
	BlockNumber relpages = 0;
	bool in_outer_xact = true; //TODO:check

	if (params->options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	CHECK_FOR_INTERRUPTS();

	if (ConditionalLockRelationOid(relid, ShareUpdateExclusiveLock))
		onerel = try_relation_open(relid, NoLock);

	if (!onerel)
		return;

	acquirefunc = gamma_acquire_sample_rows;
	relpages = RelationGetNumberOfBlocks(onerel);

	pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE,
								  RelationGetRelid(onerel));

	do_analyze_rel(onerel, params, NULL/* all cols */, acquirefunc,
			relpages, false, in_outer_xact, elevel);

	if (onerel->rd_rel->relhassubclass)
		do_analyze_rel(onerel, params, NULL, acquirefunc, relpages,
					   true, in_outer_xact, elevel);

	relation_close(onerel, NoLock);

	pgstat_progress_end_command();

	return;
}


static void
gamma_analyze_rel(Oid relid, RangeVar *relation,
			VacuumParams *params, List *va_cols, bool in_outer_xact,
			BufferAccessStrategy bstrategy)
{
	Relation	onerel;
	int			elevel;
	AcquireSampleRowsFunc acquirefunc = NULL;
	BlockNumber relpages = 0;

	/* Select logging level */
	if (params->options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	/* Set up static variables */
	vac_strategy = bstrategy;

	/*
	 * Check for user-requested abort.
	 */
	CHECK_FOR_INTERRUPTS();

	onerel = vacuum_open_relation(relid, relation, params->options & ~(VACOPT_VACUUM),
								  params->log_min_duration >= 0,
								  ShareUpdateExclusiveLock);

	/* leave if relation could not be opened or locked */
	if (!onerel)
		return;

	if (!vacuum_is_relation_owner(RelationGetRelid(onerel),
								  onerel->rd_rel,
								  params->options & VACOPT_ANALYZE))
	{
		relation_close(onerel, ShareUpdateExclusiveLock);
		return;
	}

	acquirefunc = gamma_acquire_sample_rows;
	/* Also get regular table's size */
	relpages = RelationGetNumberOfBlocks(onerel);

	/*
	 * OK, let's do it.  First, initialize progress reporting.
	 */
	pgstat_progress_start_command(PROGRESS_COMMAND_ANALYZE,
								  RelationGetRelid(onerel));

	params->options |= VACOPT_GAMMA_ANALYZE;

	do_analyze_rel(onerel, params, va_cols, acquirefunc,
			relpages, false, in_outer_xact, elevel);

	relation_close(onerel, NoLock);

	pgstat_progress_end_command();
}


extern double gammadb_stats_analyze_tuple_factor;

static int
gamma_acquire_sample_rows(Relation onerel, int elevel,
					HeapTuple *rows, int targrows,
					double *totalrows, double *totaldeadrows)
{
	int numrows = 0;	/* # rows now in reservoir */
	double samplerows = 0; /* total # rows collected */
	double liverows = 0;	/* # live rows seen */
	double deadrows = 0;	/* # dead rows seen */
	double rowstoskip = -1;	/* -1 means not set yet */
	TransactionId OldestXmin;
	ReservoirStateData rstate;
	TupleTableSlot *slot;
	TableScanDesc scan;

	Assert(targrows > 0);

	OldestXmin = GetOldestNonRemovableTransactionId(onerel);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	scan = table_beginscan_analyze(onerel);
	slot = table_slot_create(onerel, NULL);

	vacuum_delay_point();

	while (table_scan_analyze_next_tuple(scan, OldestXmin, &liverows, &deadrows, slot))
	{
		if (numrows < targrows)
			rows[numrows++] = ExecCopySlotHeapTuple(slot);
		else
		{
			if (rowstoskip < 0)
				rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

			if (rowstoskip <= 0)
			{
				int	k = (int) (targrows * sampler_random_fract(&rstate.randstate));

				Assert(k >= 0 && k < targrows);
				heap_freetuple(rows[k]);
				rows[k] = ExecCopySlotHeapTuple(slot);
			}

			rowstoskip -= 1;
		}

		samplerows += 1;
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);

	if (numrows == targrows)
		qsort_interruptible((void *) rows, numrows, sizeof(HeapTuple),
							compare_rows, NULL);

	*totalrows = samplerows / gammadb_stats_analyze_tuple_factor;
	*totaldeadrows = 0.0;

	return numrows;
}

void
gamma_exec_vacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel)
{
	VacuumParams params;
	bool		verbose = false;
	bool		skip_locked = false;
	bool		analyze = false;
	bool		freeze = false;
	bool		full = false;
	bool		disable_page_skipping = false;
	bool		process_toast = true;
	ListCell   *lc;

	/* index_cleanup and truncate values unspecified for now */
	params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params.truncate = VACOPTVALUE_UNSPECIFIED;

	/* By default parallel vacuum is enabled */
	params.nworkers = 0;

	/* Parse options list */
	foreach(lc, vacstmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		/* Parse common options for VACUUM and ANALYZE */
		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "skip_locked") == 0)
			skip_locked = defGetBoolean(opt);
		else if (!vacstmt->is_vacuumcmd)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized ANALYZE option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));

		/* Parse options available on VACUUM */
		else if (strcmp(opt->defname, "analyze") == 0)
			analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "freeze") == 0)
			freeze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "full") == 0)
			full = defGetBoolean(opt);
		else if (strcmp(opt->defname, "disable_page_skipping") == 0)
			disable_page_skipping = defGetBoolean(opt);
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			/* Interpret no string as the default, which is 'auto' */
			if (!opt->arg)
				params.index_cleanup = VACOPTVALUE_AUTO;
			else
			{
				char	   *sval = defGetString(opt);

				/* Try matching on 'auto' string, or fall back on boolean */
				if (pg_strcasecmp(sval, "auto") == 0)
					params.index_cleanup = VACOPTVALUE_AUTO;
				else
					params.index_cleanup = get_vacoptval_from_boolean(opt);
			}
		}
		else if (strcmp(opt->defname, "process_toast") == 0)
			process_toast = defGetBoolean(opt);
		else if (strcmp(opt->defname, "truncate") == 0)
			params.truncate = get_vacoptval_from_boolean(opt);
		else if (strcmp(opt->defname, "parallel") == 0)
		{
			if (opt->arg == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel option requires a value between 0 and %d",
								MAX_PARALLEL_WORKER_LIMIT),
						 parser_errposition(pstate, opt->location)));
			}
			else
			{
				int			nworkers;

				nworkers = defGetInt32(opt);
				if (nworkers < 0 || nworkers > MAX_PARALLEL_WORKER_LIMIT)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("parallel workers for vacuum must be between 0 and %d",
									MAX_PARALLEL_WORKER_LIMIT),
							 parser_errposition(pstate, opt->location)));

				/*
				 * Disable parallel vacuum, if user has specified parallel
				 * degree as zero.
				 */
				if (nworkers == 0)
					params.nworkers = -1;
				else
					params.nworkers = nworkers;
			}
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized VACUUM option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	/* Set vacuum options */
	params.options =
		(vacstmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
		(verbose ? VACOPT_VERBOSE : 0) |
		(skip_locked ? VACOPT_SKIP_LOCKED : 0) |
		(analyze ? VACOPT_ANALYZE : 0) |
		(freeze ? VACOPT_FREEZE : 0) |
		(full ? VACOPT_FULL : 0) |
		(disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0) |
		(process_toast ? VACOPT_PROCESS_TOAST : 0);

	/* sanity checks on options */
	Assert(params.options & (VACOPT_VACUUM | VACOPT_ANALYZE));
	Assert((params.options & VACOPT_VACUUM) ||
		   !(params.options & (VACOPT_FULL | VACOPT_FREEZE)));

	if ((params.options & VACOPT_FULL) && params.nworkers > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM FULL cannot be performed in parallel")));

	/*
	 * Make sure VACOPT_ANALYZE is specified if any column lists are present.
	 */
	if (!(params.options & VACOPT_ANALYZE))
	{
		ListCell   *lc;

		foreach(lc, vacstmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (vrel->va_cols != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("ANALYZE option must be specified when a column list is provided")));
		}
	}

	/*
	 * All freeze ages are zero if the FREEZE option is given; otherwise pass
	 * them as -1 which means to use the default values.
	 */
	if (params.options & VACOPT_FREEZE)
	{
		params.freeze_min_age = 0;
		params.freeze_table_age = 0;
		params.multixact_freeze_min_age = 0;
		params.multixact_freeze_table_age = 0;
	}
	else
	{
		params.freeze_min_age = -1;
		params.freeze_table_age = -1;
		params.multixact_freeze_min_age = -1;
		params.multixact_freeze_table_age = -1;
	}

	/* user-invoked vacuum is never "for wraparound" */
	params.is_wraparound = false;

	/* user-invoked vacuum uses VACOPT_VERBOSE instead of log_min_duration */
	params.log_min_duration = -1;

	/* Now go through the common routine */
	gamma_vacuum(vacstmt->rels, &params, NULL, isTopLevel);
}

static void
gamma_vacuum(List *relations, VacuumParams *params,
	   BufferAccessStrategy bstrategy, bool isTopLevel)
{
	static bool in_vacuum = false;

	const char *stmttype;
	volatile bool in_outer_xact,
				use_own_xacts;

	Assert(params != NULL);

	stmttype = (params->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";

	if (params->options & VACOPT_VACUUM)
	{
		PreventInTransactionBlock(isTopLevel, stmttype);
		in_outer_xact = false;
	}
	else
		in_outer_xact = IsInTransactionBlock(isTopLevel);

	if (in_vacuum)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s cannot be executed from VACUUM or ANALYZE",
						stmttype)));

	/*
	 * Sanity check DISABLE_PAGE_SKIPPING option.
	 */
	if ((params->options & VACOPT_FULL) != 0 &&
		(params->options & VACOPT_DISABLE_PAGE_SKIPPING) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")));

	/* sanity check for PROCESS_TOAST */
	if ((params->options & VACOPT_FULL) != 0 &&
		(params->options & VACOPT_PROCESS_TOAST) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PROCESS_TOAST required with VACUUM FULL")));

	vac_context = AllocSetContextCreate(PortalContext,
										"Vacuum",
										ALLOCSET_DEFAULT_SIZES);

	if (bstrategy == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(vac_context);

		bstrategy = GetAccessStrategy(BAS_VACUUM);
		MemoryContextSwitchTo(old_context);
	}
	vac_strategy = bstrategy;

	if (relations != NIL)
	{
		List	   *newrels = NIL;
		ListCell   *lc;

		foreach(lc, relations)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
			List	   *sublist;
			MemoryContext old_context;

			sublist = expand_vacuum_rel(vrel, params->options);
			old_context = MemoryContextSwitchTo(vac_context);
			newrels = list_concat(newrels, sublist);
			MemoryContextSwitchTo(old_context);
		}
		relations = newrels;
	}
	else
		relations = get_all_vacuum_rels(params->options);

	if (params->options & VACOPT_VACUUM)
		use_own_xacts = true;
	else
	{
		Assert(params->options & VACOPT_ANALYZE);
		if (IsAutoVacuumWorkerProcess())
			use_own_xacts = true;
		else if (in_outer_xact)
			use_own_xacts = false;
		else if (list_length(relations) > 1)
			use_own_xacts = true;
		else
			use_own_xacts = false;
	}

	if (use_own_xacts)
	{
		Assert(!in_outer_xact);

		/* ActiveSnapshot is not set by autovacuum */
		if (ActiveSnapshotSet())
			PopActiveSnapshot();

		/* matches the StartTransaction in PostgresMain() */
		CommitTransactionCommand();
	}

	/* Turn vacuum cost accounting on or off, and set/clear in_vacuum */
	PG_TRY();
	{
		ListCell   *cur;

		in_vacuum = true;
		VacuumCostActive = (VacuumCostDelay > 0);
		VacuumCostBalance = 0;
		VacuumPageHit = 0;
		VacuumPageMiss = 0;
		VacuumPageDirty = 0;
		VacuumCostBalanceLocal = 0;
		VacuumSharedCostBalance = NULL;
		VacuumActiveNWorkers = NULL;

		/*
		 * Loop to process each selected relation.
		 */
		foreach(cur, relations)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, cur);

			if (params->options & VACOPT_VACUUM)
			{
				if (!vacuum_rel(vrel->oid, vrel->relation, params))
					continue;
			}

			if (params->options & VACOPT_ANALYZE)
			{
				/*
				 * If using separate xacts, start one for analyze. Otherwise,
				 * we can use the outer transaction.
				 */
				if (use_own_xacts)
				{
					StartTransactionCommand();
					/* functions in indexes may want a snapshot set */
					PushActiveSnapshot(GetTransactionSnapshot());
				}

				gamma_analyze_rel(vrel->oid, vrel->relation, params,
							vrel->va_cols, in_outer_xact, vac_strategy);

				if (use_own_xacts)
				{
					PopActiveSnapshot();
					CommitTransactionCommand();
				}
				else
				{
					/*
					 * If we're not using separate xacts, better separate the
					 * ANALYZE actions with CCIs.  This avoids trouble if user
					 * says "ANALYZE t, t".
					 */
					CommandCounterIncrement();
				}
			}
		}
	}
	PG_FINALLY();
	{
		in_vacuum = false;
		VacuumCostActive = false;
	}
	PG_END_TRY();

	/*
	 * Finish up processing.
	 */
	if (use_own_xacts)
	{
		StartTransactionCommand();
	}

	if ((params->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess())
	{
		/*
		 * Update pg_database.datfrozenxid, and truncate pg_xact if possible.
		 * (autovacuum.c does this for itself.)
		 */
		vac_update_datfrozenxid();
	}

	/*
	 * Clean up working storage --- note we must do this after
	 * StartTransactionCommand, else we might be trying to delete the active
	 * context!
	 */
	MemoryContextDelete(vac_context);
	vac_context = NULL;
}
