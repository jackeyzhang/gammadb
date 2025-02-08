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

#include "catalog/pg_aggregate.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "nodes/nodeFuncs.h"

#include "optimizer/gamma_rewrite_extract_agg.h"

/* GUC */
bool gammadb_rewrite_extract_agg = true;

static bool gamma_check_can_extract(Aggref *aggref);
static Expr * gamma_coerce_type(Node *node, Oid targetType, Oid inputType);
static Aggref * gamma_create_count_aggref(void);
static Aggref * gamma_create_new_aggref(char *aggname, Oid argType, Oid resultType);
static TargetEntry * gamma_extract_agg(TargetEntry *te);

Query *
gamma_rewrite_extract_agg(Query *parse)
{
	ListCell *lct;

	List *new_target_list = NULL;

	if (!gammadb_rewrite_extract_agg)
		return parse;

	if (parse == NULL)
		return NULL;

	if (parse->groupingSets != NULL)
		return parse;

	if (!parse->hasAggs)
	   return parse;	

	foreach (lct, parse->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lct);
		TargetEntry *new_te = NULL;

		if (!IsA(te->expr, Aggref))
		{
			new_target_list = lappend(new_target_list, te);
			continue;
		}

		if (!gamma_check_can_extract((Aggref *) te->expr))		
		{
			new_target_list = lappend(new_target_list, te);
			continue;
		}

		new_te = gamma_extract_agg(te);
		if (new_te != NULL)
			new_target_list = lappend(new_target_list, new_te);
		else
			new_target_list = lappend(new_target_list, te);
	}

	return parse;
}

static bool
gamma_check_can_extract(Aggref *aggref)
{
	OpExpr *op_expr;
	Node *left;
	Node *right;
	TargetEntry *te_arg;
	
	if (aggref->aggdistinct)
		return false;

	if (aggref->aggstar)
		return false;

	if (list_length(aggref->args) != 1)
		return false;

	te_arg = (TargetEntry *) linitial(aggref->args);
	if (!IsA(te_arg, TargetEntry))
		return false;

	op_expr = (OpExpr *) te_arg->expr;
	if (!IsA(op_expr, OpExpr))
		return false;

	if (list_length(op_expr->args) != 2)
		return false;

	left = (Node *) linitial(op_expr->args);
	right = (Node *) lsecond(op_expr->args);
	if (IsA(left, Var) && IsA(right, Const))
	{}
	else if (IsA(left, Const) && IsA(right, Var))
	{}
	else
		return false;
	
	return true;
}

TargetEntry *
gamma_extract_agg(TargetEntry *te)
{
	OpExpr *op_expr;
	Var *var;
	Const *con;
	Aggref *aggref;
	TargetEntry *te_arg;

	HeapTuple proctup;
	Form_pg_proc procform;
	char *proname;
	List *funcname = NULL;

	Oid var_type = InvalidOid;
	Oid con_type = InvalidOid;
	Oid op_type = InvalidOid;
	Oid agg_type = InvalidOid;

	Var *new_var = NULL;
	Aggref *new_count_aggref = NULL;

	aggref = (Aggref *) te->expr;
	te_arg = (TargetEntry *) linitial(aggref->args);
	op_expr = (OpExpr *) te_arg->expr;
	if (IsA(linitial(op_expr->args), Var))
	{
		var = (Var *) linitial(op_expr->args);
		con = (Const *) lsecond(op_expr->args);
	}
	else
	{
		var = (Var *) lsecond(op_expr->args);
		con = (Const *) linitial(op_expr->args);
	}

	op_type = exprType((const Node *) op_expr);
	var_type = exprType((const Node *) var);
	con_type = exprType((const Node *) con);
	agg_type = exprType((const Node *) aggref);

	if (var_type == op_type)
	{
		new_var = var;
	}
	else
	{
		/* new_var maybe NULL, it will be processed in below codes */
		new_var = (Var *) gamma_coerce_type((Node *)var, op_type, var_type);
	}

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggref->aggfnoid));
	procform = (Form_pg_proc) GETSTRUCT(proctup);
	proname = NameStr(procform->proname);
	funcname = lappend(funcname, makeString(proname));

	if (new_var == NULL)
	{
		Aggref *tmp_aggref = gamma_create_new_aggref(proname, var_type, agg_type);
		if (tmp_aggref == NULL)
		{
			ReleaseSysCache(proctup);
			return NULL;
		}

		tmp_aggref->args = lappend(tmp_aggref->args, te_arg);
		aggref = tmp_aggref;
		new_var = var;
	}

	if (pg_strcasecmp(proname, "sum") == 0)
	{
		OpExpr *const_count;
		HeapTuple tuple_multi = NULL;
		Form_pg_operator multi_oper;

		/* make count(*) */
		new_count_aggref = gamma_create_count_aggref();

		/* make const * count(*) */
		tuple_multi = oper(NULL, list_make1(makeString("*")),
							con_type, INT8OID, true, -1);
		if (tuple_multi == NULL)
		{
			ReleaseSysCache(proctup);
			return NULL;
		}

		multi_oper = (Form_pg_operator)GETSTRUCT(tuple_multi);

		const_count = makeNode(OpExpr);
		const_count->opno = oprid(tuple_multi);
		const_count->opfuncid = multi_oper->oprcode;
		const_count->opresulttype = multi_oper->oprresult;
		const_count->opretset = get_func_retset(multi_oper->oprcode);
		const_count->args = lappend(const_count->args, con);
		const_count->args = lappend(const_count->args, new_count_aggref);
		const_count->location = -1;

		con = (Const *)const_count;
		con_type = exprType((const Node *) const_count);

		ReleaseSysCache(tuple_multi);
	}
	else if (pg_strcasecmp(proname, "avg") == 0 ||
			 pg_strcasecmp(proname, "max") == 0 ||
			 pg_strcasecmp(proname, "min") == 0)
	{}
	else
	{
		ReleaseSysCache(proctup);
		return NULL;
	}

	{
		/* make agg + const */
		OpExpr *agg_const;
		HeapTuple tuple_plus = NULL;
		Form_pg_operator plus_oper;

		tuple_plus = oper(NULL, list_make1(makeString(get_opname(op_expr->opno))),
							agg_type, con_type, true, -1);
		if (tuple_plus == NULL)
		{
			ReleaseSysCache(proctup);
			return NULL;
		}

		plus_oper = (Form_pg_operator)GETSTRUCT(tuple_plus);

		if (plus_oper->oprleft != agg_type || plus_oper->oprright != con_type)
		{
			//TODO: check if can do coerce
			ReleaseSysCache(proctup);
			return NULL;
		}

		agg_const = makeNode(OpExpr);
		agg_const->opno = oprid(tuple_plus);
		agg_const->opfuncid = plus_oper->oprcode;
		agg_const->opresulttype = plus_oper->oprresult;
		agg_const->opretset = get_func_retset(plus_oper->oprcode);
		agg_const->args = lappend(agg_const->args, aggref);
		agg_const->args = lappend(agg_const->args, con);
		agg_const->location = -1;

		/* change arg of aggref, use new var */
		te_arg->expr = (Expr *) new_var;

		ReleaseSysCache(tuple_plus);

		/*coerce the result type */
		if (exprType((const Node *) agg_const) == agg_type)
		{
			te->expr = (Expr *) agg_const;
		}
		else
		{
			Oid agg_const_type = exprType((const Node *) agg_const);
			Expr *agg_const_expr;
			agg_const_expr = gamma_coerce_type((Node *)var, agg_type, agg_const_type);
			if (agg_const_expr == NULL)
			{
				ReleaseSysCache(proctup);
				return NULL;
			}

			te->expr = agg_const_expr;
		}
	}
	
	ReleaseSysCache(proctup);
	return te;
}

static Aggref *
gamma_create_count_aggref(void)
{
	Oid	retype;
	bool retset;
	int	nvargs;
	Oid vatype;
	Oid *true_oid_array;
	FuncDetailCode fdresult;

	List *funcname = NULL;
	Aggref *aggref = makeNode(Aggref);

	aggref->aggtype = INT8OID;
	aggref->aggstar = true;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggsplit = AGGSPLIT_SIMPLE;
	aggref->aggno = -1;
	aggref->aggtransno = -1;
	
	funcname = lappend(funcname, makeString("count"));

	fdresult = func_get_detail(funcname, NIL, NIL,
			0, NULL, false, false, false,
			&aggref->aggfnoid, &retype, &retset,
			&nvargs, &vatype,
			&true_oid_array, NULL);

	if (fdresult != FUNCDETAIL_AGGREGATE || !OidIsValid(aggref->aggfnoid))
	{
		pfree(aggref);
		return NULL;
	}

	return aggref;
}

static Aggref *
gamma_create_new_aggref(char *aggname, Oid argType, Oid resultType)
{
	Oid	retype;
	bool retset;
	int	nvargs;
	Oid vatype;
	Oid *true_oid_array;
	FuncDetailCode fdresult;

	Oid	*argtypes;

	List *funcname = NULL;
	Aggref *aggref = makeNode(Aggref);

	aggref->aggtype = INT8OID;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggsplit = AGGSPLIT_SIMPLE;
	aggref->aggno = -1;
	aggref->aggtransno = -1;

	funcname = lappend(funcname, makeString(aggname));

	argtypes = palloc(sizeof(Oid));
	*argtypes = argType;
	fdresult = func_get_detail(funcname, NIL, NIL,
			1, argtypes, false, false, false,
			&aggref->aggfnoid, &retype, &retset,
			&nvargs, &vatype,
			&true_oid_array, NULL);

	if (fdresult != FUNCDETAIL_AGGREGATE ||
		!OidIsValid(aggref->aggfnoid) ||
		retype != resultType)
	{
		pfree(aggref);
		return NULL;
	}

	return aggref;
}

static Expr *
gamma_coerce_type(Node *node, Oid targetType, Oid inputType)
{
	RelabelType *rt = NULL;
	CoercionPathType pathtype;
	Oid funcId;

	pathtype = find_coercion_pathway(targetType, inputType, COERCION_IMPLICIT,
			&funcId);

	if (pathtype == COERCION_PATH_RELABELTYPE)
	{
		rt = makeRelabelType((Expr *) node,
				targetType, -1,
				InvalidOid,
				COERCE_IMPLICIT_CAST);

	}

	return (Expr *) rt;
}
