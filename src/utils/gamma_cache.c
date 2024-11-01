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
#include "access/htup.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "nodes/nodes.h"
#include "utils/acl.h"

#include "executor/gamma_vec_exec_grouping.h"
#include "executor/gamma_vec_tablescan.h"
#include "optimizer/gamma_converter.h"
#include "utils/gamma_cache.h"
#include "utils/utils.h"

static Oid boolexpr_and_oid = InvalidOid;
static Oid boolexpr_or_oid = InvalidOid;
static Oid boolexpr_not_oid = InvalidOid;

static ExprStateEvalFunc exec_interp_expr = NULL;

static void
gamma_startup_interp_expr_proc()
{
	ExprState *state = gamma_vec_init_interp_expr_proc();

	exec_interp_expr = (ExprStateEvalFunc)state->evalfunc_private;

	pfree(state);
	return;
}

static void
gamma_uninstall_interp_expr_proc()
{
	exec_interp_expr = NULL;
}

ExprStateEvalFunc
gamma_get_interp_expr_proc()
{
	if (unlikely(exec_interp_expr == NULL))
		gamma_startup_interp_expr_proc();

	return exec_interp_expr;
}

/*
 * Get the function oid for replacing BoolExpr(AND_EXPR) in vectorized mode
 */ 
static void
gamma_startup_boolexpr_and()
{
	Oid		funcid = InvalidOid;
	Oid		retype;
	List	*funcname = NULL;
	Oid		*argtypes;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	Oid		   *true_oid_array;

	funcname = lappend(funcname, makeString("gamma_vec_bool_expr_and"));

	argtypes = palloc(sizeof(Oid));
	argtypes[0] = en_vec_type(BOOLOID);

	func_get_detail(funcname, NIL, NIL,
			1, argtypes, true, false, false,
			&funcid, &retype, &retset,
			&nvargs, &vatype,
			&true_oid_array, NULL);


	boolexpr_and_oid = funcid;
	return;
}

static void
gamma_uninstall_boolexpr_and()
{
	boolexpr_and_oid = InvalidOid;
}

Oid
gamma_get_boolexpr_and_oid()
{
	if (unlikely(boolexpr_and_oid == InvalidOid))
		gamma_startup_boolexpr_and();

	return boolexpr_and_oid;
}

/*
 * Get the function oid for replacing BoolExpr(OR_EXPR) in vectorized mode
 */ 
static void
gamma_startup_boolexpr_or()
{
	Oid		funcid = InvalidOid;
	Oid		retype;
	List	*funcname = NULL;
	Oid		*argtypes;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	Oid		   *true_oid_array;

	funcname = lappend(funcname, makeString("gamma_vec_bool_expr_or"));

	argtypes = palloc(sizeof(Oid));
	argtypes[0] = en_vec_type(BOOLOID);

	func_get_detail(funcname, NIL, NIL,
			1, argtypes, true, false, false,
			&funcid, &retype, &retset,
			&nvargs, &vatype,
			&true_oid_array, NULL);


	boolexpr_or_oid = funcid;
	return;
}

static void
gamma_uninstall_boolexpr_or()
{
	boolexpr_or_oid = InvalidOid;
}

Oid
gamma_get_boolexpr_or_oid()
{
	if (unlikely(boolexpr_or_oid == InvalidOid))
		gamma_startup_boolexpr_or();

	return boolexpr_or_oid;
}

/*
 * Get the function oid for replacing BoolExpr(NOT_EXPR) in vectorized mode
 */ 
static void
gamma_startup_boolexpr_not()
{
	Oid		funcid = InvalidOid;
	Oid		retype;
	List	*funcname = NULL;
	Oid		*argtypes;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	Oid		   *true_oid_array;

	funcname = lappend(funcname, makeString("gamma_vec_bool_expr_not"));

	argtypes = palloc(sizeof(Oid));
	argtypes[0] = en_vec_type(BOOLOID);

	func_get_detail(funcname, NIL, NIL,
			1, argtypes, true, false, false,
			&funcid, &retype, &retset,
			&nvargs, &vatype,
			&true_oid_array, NULL);


	boolexpr_not_oid = funcid;
	return;
}

static void
gamma_uninstall_boolexpr_not()
{
	boolexpr_not_oid = InvalidOid;
}

Oid
gamma_get_boolexpr_not_oid()
{
	if (unlikely(boolexpr_not_oid == InvalidOid))
		gamma_startup_boolexpr_not();

	return boolexpr_not_oid;
}


/*
 * keep some caches to enhance the performance
 */
void
gamma_cache_startup()
{
	/* 
	 * The boolexpr functions call the en_vec_type function, which affects
	 * the hash table mapping for both regular types and vector types.
	 */
	//gamma_startup_boolexpr_and();
	//gamma_startup_boolexpr_or();
	//gamma_startup_boolexpr_not();
	gamma_startup_interp_expr_proc();
}

void
gamma_cache_uninstall()
{
	gamma_uninstall_boolexpr_and();
	gamma_uninstall_boolexpr_or();
	gamma_uninstall_boolexpr_not();
	gamma_uninstall_interp_expr_proc();
}
