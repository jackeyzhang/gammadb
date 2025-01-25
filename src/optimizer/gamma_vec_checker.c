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
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "optimizer/gamma_checker.h"
#include "utils/utils.h"

#define EXTRACT_TIME_OID	6202
#define TEXT_LENGTH_OID		1257
#define LENGTH_OID			1317

#define REGEXP_REPLACE_NOOPT_OID	2284
#define REGEXP_REPLACE_OID			2285

#define DATE_TRUNC_OID		2020

static bool
gamma_vec_check_func_expr(Oid funcoid)
{
	if (funcoid == EXTRACT_TIME_OID)
		return true;
	else if (funcoid == TEXT_LENGTH_OID)
		return true;
	else if (funcoid == LENGTH_OID)
		return true;
	else if (funcoid == REGEXP_REPLACE_OID)
		return true;
	else if (funcoid == REGEXP_REPLACE_NOOPT_OID)
		return true;
	else if (funcoid == DATE_TRUNC_OID)
		return true;

	return false;
}

static bool
gamma_vec_check_type(Oid typid)
{
	return (en_vec_type(typid) != InvalidOid);
}

static bool
gamma_vec_check_expr_recursive(Node *node, void *context)
{
	if(node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *var = (Var *) node;
				if (var->varattno <= 0)
					return true;

				/* note the result values */
				return !gamma_vec_check_type(var->vartype);
			}
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *)node;
				Oid aggfnoid;
				Oid	retype;
				HeapTuple proctup;
				Form_pg_proc procform;
				List *funcname = NULL;
				int	i;
				Oid	*argtypes;
				char *proname;
				bool retset;
				int	nvargs;
				Oid	vatype;
				Oid	*true_oid_array;
				FuncDetailCode fdresult;

				aggfnoid = aggref->aggfnoid;

				proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
				if (!HeapTupleIsValid(proctup))
				{
					elog(WARNING, "cache lookup failed for function %u", aggfnoid);
					return true;
				}

				procform = (Form_pg_proc) GETSTRUCT(proctup);
				proname = NameStr(procform->proname);
				funcname = lappend(funcname, makeString(proname));

				if (procform->pronargs > 0)
				{
					argtypes = palloc(sizeof(Oid) * procform->pronargs);
					for (i = 0; i < procform->pronargs; i++)
						argtypes[i] = en_vec_type(procform->proargtypes.values[i]);
				}
				else
				{
					argtypes = NULL;
				}
				
				fdresult = func_get_detail(funcname, NIL, NIL,
						procform->pronargs, argtypes, false, false, false,
						&aggfnoid, &retype, &retset,
						&nvargs, &vatype,
						&true_oid_array, NULL);

				ReleaseSysCache(proctup);

				if (fdresult != FUNCDETAIL_AGGREGATE || !OidIsValid(aggfnoid))
					return true;

				if (aggref->args != NULL)
				{
					if (gamma_vec_check_expr_recursive(linitial(aggref->args), context))
						return true;
				}

				return false;
			}
		case T_OpExpr:
			{
				OpExpr *opexpr = (OpExpr *) node;
				Oid	ltype, rtype, rettype;
				HeapTuple tuple;
				List *opname;
				Node *arg1,*arg2;
				Const *con;

				rettype = en_vec_type(opexpr->opresulttype);
				if (InvalidOid == rettype)
					return true;

				if (list_length(opexpr->args) != 2)
					return true;

				arg1 = (Node *)linitial(opexpr->args);
				arg2 = (Node *)lsecond(opexpr->args);

				ltype = exprType(arg1);
				rtype = exprType(arg2);

				if (!IsA(arg1, Const))
				{
					ltype = en_vec_type(ltype);
					if (ltype == InvalidOid)
						return true;
				}
				else
				{
					con = (Const *) arg1;
					ltype = con->consttype;
				}

				if (!IsA(arg2, Const))
				{
				rtype = en_vec_type(rtype);
				if (rtype == InvalidOid)
					return true;
				}
				else
				{
					con = (Const *) arg2;
					rtype = con->consttype;
				}

				/* try to get the vectorized operator function */
				opname = list_make1(makeString(get_opname(opexpr->opno)));
				tuple = oper(NULL, opname, ltype, rtype, true, -1);
				if(tuple == NULL)
				{
					return true;
				}

				ReleaseSysCache(tuple);

				/* check op function arguments */
				if (gamma_vec_check_expr_recursive(linitial(opexpr->args), context))
					return true;

				if (gamma_vec_check_expr_recursive(lsecond(opexpr->args), context))
					return true;

				return false;
			}
		case T_FuncExpr:
			{
				FuncExpr *f = (FuncExpr*)node;
				Node* expr = NULL;

				if (gamma_vec_check_func_expr(f->funcid))
					return false;

				if(list_length(f->args) != 1)
					return true;

				expr = (Node*)linitial(f->args);
				if(!IsA(expr, Const))
					return true;

				return false;
			}
		case T_RestrictInfo:
			{
				RestrictInfo *info = (RestrictInfo *) node;
				return gamma_vec_check_expr_recursive((Node*)info->clause, context);
			}
		//TODO: support in future
		case T_CaseExpr:
		case T_CaseWhen:
		case T_ScalarArrayOpExpr:
			return true;
		default:
			break;

	}

	return expression_tree_walker(node, gamma_vec_check_expr_recursive, NULL); 
}

bool
gamma_vec_check_expr(Node *node)
{
	/*
	 * gamma_vec_check_expr_recursive return true means can not vectorized.
	 */
	return !gamma_vec_check_expr_recursive(node, NULL);
}

static bool
gamma_vec_check_relation(PlannerInfo *root, RelOptInfo *rel, Path *path)
{
	Index varno = rel->relid;
	RangeTblEntry *rte = planner_rt_fetch(varno, root);
	Relation relation;
	int attrno, numattrs;
	bool result = true;

	if (rte->rtekind == RTE_RELATION)
	{
		/* Assume we already have adequate lock */
		relation = table_open(rte->relid, NoLock);

		numattrs = RelationGetNumberOfAttributes(relation);
		for (attrno = 0; attrno < numattrs; attrno++)
		{
			Form_pg_attribute att_tup = TupleDescAttr(relation->rd_att, attrno);

			if (!gamma_vec_check_type(att_tup->atttypid))
			{
				result = false;
				break;
			}
		}

		table_close(relation, NoLock);
	}

	return result;
}

bool
gamma_vec_check_path(PlannerInfo *root, RelOptInfo *rel, Path *path)
{
	bool result = false;

	/* check targetlist */
	if (path->pathtarget == rel->reltarget)
		result = gamma_vec_check_expr((Node *) rel->reltarget->exprs);
	else
		result = gamma_vec_check_expr((Node *) path->pathtarget->exprs);
	if (!result)
		return false;

	/* check clauses */
	result = gamma_vec_check_expr((Node *) rel->baserestrictinfo);
	if (!result)
		return false;

	switch (path->pathtype)
	{
		case T_IndexScan:
		case T_IndexOnlyScan:
			{
				IndexOptInfo *indexinfo = ((IndexPath *)path)->indexinfo;
				result = gamma_vec_check_expr((Node*)(indexinfo->indrestrictinfo));
				if (!result)
					return false;
			}
			/* fall through */
		case T_SeqScan:
			{
				//TODO: Optimize performance by checking only once
				if (!gamma_vec_check_relation(root, rel, path))
					return false;
			}
			break;
		case T_SubqueryScan:
			{
				/* subquery scan path contains sub-path */
				//TODO: check if the subpath is vectorized
			}
			break;
		default:
			break;
	}

	return true;
}
