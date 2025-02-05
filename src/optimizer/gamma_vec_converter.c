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
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "parser/parse_coerce.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "nodes/plannodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "executor/gamma_devectorize.h"
#include "executor/gamma_indexscan.h"
#include "executor/gamma_indexonlyscan.h"
#include "executor/gamma_vec_tablescan.h"
#include "optimizer/gamma_converter.h"
#include "utils/gamma_cache.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

typedef struct ConverterContext
{
	Oid retType;
}ConverterContext;

static void mutate_plan_fields(Plan *newplan, Plan *oldplan,
							   Node *(*mutator) (), void *context);
static Node * plan_tree_mutator(Node *node, Node *(*mutator) (), void *context);
static Node * gamma_agg_targetlist_mutator(Node *node, void *context);
static Node * gamma_vec_convert_mutator(Node *node, ConverterContext *ctx);


#define EXTRACT_TIME_OID	6202
#define TEXT_LENGTH_OID		1257
#define LENGTH_OID			1317

#define REGEXP_REPLACE_NOOPT_OID	2284
#define REGEXP_REPLACE_OID			2285

#define DATE_TRUNC_OID		2020

static char *
gamma_vec_convert_func_expr(Oid funcoid)
{
	if (funcoid == EXTRACT_TIME_OID)
		return "vextract_time";
	else if (funcoid == TEXT_LENGTH_OID)
		return "vtext_length";
	else if (funcoid == LENGTH_OID)
		return "vtext_length";
	else if (funcoid == REGEXP_REPLACE_NOOPT_OID)
		return "vtextregexreplace_noopt";
	else if (funcoid == REGEXP_REPLACE_OID)
		return "vtextregexreplace";
	else if (funcoid == DATE_TRUNC_OID)
		return "vtimestamp_trunc";

	return NULL;
}

/* Agg targetlist need to process differently */
static Node *
gamma_process_agg_targetlist(Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return gamma_agg_targetlist_mutator(expr, NULL);
}

static Node *
gamma_agg_targetlist_mutator(Node *node, void *context)
{
	if (IsA(node, Aggref))
	{
		return gamma_vec_convert_mutator(node, NULL);
	}

	return expression_tree_mutator(node, gamma_agg_targetlist_mutator, context);
}

static Node*
gamma_vec_convert_mutator(Node *node, ConverterContext *ctx)
{
	if(NULL == node)
		return NULL;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *newnode;
				Oid vdatum;

				newnode = (Var*)plan_tree_mutator(node, gamma_vec_convert_mutator, ctx);
				vdatum = en_vec_type(newnode->vartype);
				if(InvalidOid == vdatum)
				{
					sleep(10000000);
					elog(ERROR, "Cannot find vdatum for type %d", newnode->vartype);
				}
				newnode->vartype = vdatum;
				return (Node *)newnode;
			}

		case T_Aggref:
			{
				Aggref *newnode;
				Oid	oldfnOid;
				Oid	retype;
				HeapTuple proctup;
				Form_pg_proc procform;
				List *funcname = NULL;
				int	i;
				Oid	*argtypes;
				char *proname;
				bool retset;
				int	nvargs;
				Oid vatype;
				Oid *true_oid_array;
				FuncDetailCode fdresult;

				newnode = (Aggref *)plan_tree_mutator(node, gamma_vec_convert_mutator, ctx);
				oldfnOid = newnode->aggfnoid;

				/* for agg_func(distinct) */
				if (newnode->aggdistinct)
					return (Node*)newnode;

				/* for count(*) */
				if (newnode->aggstar)
					return (Node *)newnode;

				proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oldfnOid));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for function %u", oldfnOid);
				procform = (Form_pg_proc) GETSTRUCT(proctup);
				proname = NameStr(procform->proname);
				funcname = lappend(funcname, makeString(proname));

				argtypes = palloc(sizeof(Oid) * procform->pronargs);
				for (i = 0; i < procform->pronargs; i++)
					argtypes[i] = en_vec_type(procform->proargtypes.values[i]);
				
				fdresult = func_get_detail(funcname, NIL, NIL,
						procform->pronargs, argtypes, false, false, false,
						&newnode->aggfnoid, &retype, &retset,
						&nvargs, &vatype,
						&true_oid_array, NULL);

				ReleaseSysCache(proctup);

				if (fdresult != FUNCDETAIL_AGGREGATE || !OidIsValid(newnode->aggfnoid))
					elog(ERROR, "aggreate function not defined");
				return (Node *)newnode;
			}

		case T_OpExpr:
			{
				OpExpr	*newnode;
				Oid		ltype, rtype, rettype;
				Form_pg_operator	voper;
				HeapTuple			tuple;

				/* mutate OpExpr itself in plan_tree_mutator firstly. */
				newnode = (OpExpr *)plan_tree_mutator(node, gamma_vec_convert_mutator, ctx);
				rettype = en_vec_type(newnode->opresulttype);
				if (InvalidOid == rettype)
				{
					elog(ERROR, "Cannot find vdatum for type %d", newnode->opresulttype);
				}

				if (list_length(newnode->args) != 2)
				{
					elog(ERROR, "Unary operator not supported");
				}

				ltype = exprType(linitial(newnode->args));
				rtype = exprType(lsecond(newnode->args));

				//get the vectorized operator functions
				tuple = oper(NULL, list_make1(makeString(get_opname(newnode->opno))),
						ltype, rtype, true, -1);
				if(NULL == tuple)
				{
					elog(ERROR, "Vectorized operator not found");
				}

				voper = (Form_pg_operator)GETSTRUCT(tuple);
				if(voper->oprresult != rettype)
				{
					ReleaseSysCache(tuple);
					elog(ERROR, "Vectorize operator rettype not correct");
				}

				newnode->opresulttype = rettype;
				newnode->opfuncid = voper->oprcode;

				ReleaseSysCache(tuple);
				return (Node *)newnode;
			}

		case T_BoolExpr:
			{
				BoolExpr *boolexpr = (BoolExpr*) node;
				BoolExpr *newboolexpr;
				List *vargs = (List *)plan_tree_mutator((Node *)boolexpr->args,
													gamma_vec_convert_mutator,
													ctx);

				if (boolexpr->boolop == AND_EXPR)
				{
					newboolexpr = (BoolExpr *)makeBoolExpr(AND_EXPR, vargs, -1); 
				}
				else if (boolexpr->boolop == OR_EXPR)
				{
					newboolexpr = (BoolExpr *)makeBoolExpr(OR_EXPR, vargs, -1); 
				}
				else
				{
					newboolexpr = (BoolExpr *)makeBoolExpr(NOT_EXPR, vargs, -1); 
					Assert(boolexpr->boolop == NOT_EXPR);
				}

				return (Node *)newboolexpr;
			}
		case T_FuncExpr:
			{
				FuncExpr *funcexpr = (FuncExpr *) node;
				FuncExpr *newexpr;
				HeapTuple proctup;
				List *funcname = NULL;
				Form_pg_proc procform;
				char *proname;
				Oid *argtypes;
				bool retset;
				int	nvargs;
				Oid vatype;
				Oid *true_oid_array;
				Oid oldfnOid = funcexpr->funcid;
				int i = 0;
				Oid funcid = InvalidOid;
				ListCell *lc;
				Oid retype;

				List *vargs;

				newexpr = (FuncExpr *) plan_tree_mutator((Node *)funcexpr,
													gamma_vec_convert_mutator,
													ctx);

				vargs = newexpr->args;

				proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oldfnOid));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for function %u", oldfnOid);
				procform = (Form_pg_proc) GETSTRUCT(proctup);
				proname = gamma_vec_convert_func_expr(oldfnOid);
				if(proname == NULL)
				{
					ReleaseSysCache(proctup);
					elog(ERROR, "Vectorize operator rettype not correct");
				}

				funcname = lappend(funcname, makeString(proname));

				argtypes = palloc(sizeof(Oid) * procform->pronargs);
				foreach (lc, vargs)
				{
					Node *expr = (Node *) lfirst(lc);
					argtypes[i++] = exprType(expr);
				}

				func_get_detail(funcname, NIL, NIL,
						i, argtypes, false, false, false,
						&funcid, &retype, &retset,
						&nvargs, &vatype,
						&true_oid_array, NULL);

				ReleaseSysCache(proctup);

				if (!OidIsValid(funcid))
					elog(ERROR, "aggreate function not defined");

				//newexpr = makeNode(FuncExpr);
				newexpr->funcid = funcid;
				newexpr->funcresulttype = retype;//TODO:
				//newexpr->funcretset = false;
				//newexpr->funcvariadic = true;
				//newexpr->funcformat = COERCE_EXPLICIT_CALL; //TODO:
				//newexpr->funccollid = InvalidOid;
				//newexpr->inputcollid = InvalidOid;
				newexpr->args = vargs;
				//newexpr->location = -1;
				return (Node *)newexpr;
			}
		case T_SubPlan:
			{
				SubPlan *subplan = (SubPlan *) node;
				SubPlan *newsubplan = makeNode(SubPlan);
				memcpy(newsubplan, subplan, sizeof(SubPlan));
				newsubplan->firstColType = en_vec_type(subplan->firstColType);
				return (Node *)newsubplan;
			}
		default:
			return plan_tree_mutator(node, gamma_vec_convert_mutator, ctx);
	}
}


static Node *
plan_tree_mutator(Node *node,
				  Node *(*mutator) (),
				  void *context)
{
	Plan *plan = NULL;

	/*
	 * The mutator has already decided not to modify the current node, but we
	 * must call the mutator for any sub-nodes.
	 */
#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define CHECKFLATCOPY(newnode, node, nodetype)	\
	( AssertMacro(IsA((node), nodetype)), \
	  (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define MUTATE(newfield, oldfield, fieldtype)  \
		( (newfield) = (fieldtype) mutator((Node *) (oldfield), context) )

#define PLANMUTATE(newplan, oldplan) \
		mutate_plan_fields((Plan*)(newplan), (Plan*)(oldplan), mutator, context)

/* This is just like  PLANMUTATE because Scan adds only scalar fields. */
#define SCANMUTATE(newplan, oldplan) \
		mutate_plan_fields((Plan*)(newplan), (Plan*)(oldplan), mutator, context)

#define JOINMUTATE(newplan, oldplan) \
		mutate_join_fields((Join*)(newplan), (Join*)(oldplan), mutator, context)

#define COPYARRAY(dest,src,lenfld,datfld) \
	do { \
		(dest)->lenfld = (src)->lenfld; \
		if ( (src)->lenfld > 0  && \
             (src)->datfld != NULL) \
		{ \
			Size _size = ((src)->lenfld*sizeof(*((src)->datfld))); \
			(dest)->datfld = palloc(_size); \
			memcpy((dest)->datfld, (src)->datfld, _size); \
		} \
		else \
		{ \
			(dest)->datfld = NULL; \
		} \
	} while (0)


	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_CustomScan:
			return node;
		case T_Param:
			return node;
		case T_Sort:
			{
				Sort *vsort;
	
				FLATCOPY(vsort, node, Sort);

				PLANMUTATE(vsort, node);
				return (Node *)vsort;
			}
		case T_Result:
			{
				Result *vresult;
	
				FLATCOPY(vresult, node, Result);

				PLANMUTATE(vresult, node);
				return (Node *)vresult;
			}
		case T_SeqScan:
			{
				SeqScan	*vscan;

				FLATCOPY(vscan, node, SeqScan);

				SCANMUTATE(vscan, node);
				return (Node *)vscan;
			}
		case T_Agg:
			{
				Agg			*vagg;
				List *qual = NULL;
				List *targetlist = NULL;
				plan = (Plan *) node;

				qual = plan->qual;
				targetlist = plan->targetlist;
				plan->qual = NULL;
				plan->targetlist = NULL;

				FLATCOPY(vagg, node, Agg);

				PLANMUTATE(vagg, node);

				((Plan *)vagg)->qual = qual;
				((Plan *)vagg)->targetlist =
						(List *)gamma_process_agg_targetlist((Node *)targetlist);
				return (Node *)vagg;
			}
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *vindexscan;
				plan = (Plan *) node;
				FLATCOPY(vindexscan, node, IndexOnlyScan);
				PLANMUTATE(vindexscan, node);
				return (Node *)vindexscan;
			}
		case T_Const:
			{
				Const	   *oldnode = (Const *) node;
				Const	   *newnode;

				FLATCOPY(newnode, oldnode, Const);
				return (Node *) newnode;
			}

		case T_Var:
		{
			Var		   *var = (Var *)node;
			Var		   *newnode;

			FLATCOPY(newnode, var, Var);
			return (Node *)newnode;
		}

		case T_OpExpr:
			{
				OpExpr	   *expr = (OpExpr *)node;
				OpExpr	   *newnode;

				FLATCOPY(newnode, expr, OpExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *)newnode;
			}

		case T_FuncExpr:
			{
				FuncExpr	   *expr = (FuncExpr *)node;
				FuncExpr	   *newnode;

				FLATCOPY(newnode, expr, FuncExpr);
				MUTATE(newnode->args, expr->args, List *);
				return (Node *)newnode;
			}

		case T_List:
			{
				/*
				 * We assume the mutator isn't interested in the list nodes
				 * per se, so just invoke it on each list element. NOTE: this
				 * would fail badly on a list with integer elements!
				 */
				List	   *resultlist;
				ListCell   *temp;

				resultlist = NIL;
				foreach(temp, (List *) node)
				{
					resultlist = lappend(resultlist,
										 mutator((Node *) lfirst(temp),
												 context));
				}
				return (Node *) resultlist;
			}

		case T_TargetEntry:
			{
				TargetEntry *targetentry = (TargetEntry *) node;
				TargetEntry *newnode;

				FLATCOPY(newnode, targetentry, TargetEntry);
				MUTATE(newnode->expr, targetentry->expr, Expr *);
				return (Node *) newnode;
			}
		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				Aggref	   *newnode;

				FLATCOPY(newnode, aggref, Aggref);
				/* assume mutation doesn't change types of arguments */
				newnode->aggargtypes = list_copy(aggref->aggargtypes);
				MUTATE(newnode->aggdirectargs, aggref->aggdirectargs, List *);
				MUTATE(newnode->args, aggref->args, List *);
				MUTATE(newnode->aggorder, aggref->aggorder, List *);
				MUTATE(newnode->aggdistinct, aggref->aggdistinct, List *);
				MUTATE(newnode->aggfilter, aggref->aggfilter, Expr *);
				return (Node *) newnode;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr *caseexpr = (CaseExpr *) node;
				CaseExpr *newnode;

				FLATCOPY(newnode, caseexpr, CaseExpr);
				return (Node *) newnode;
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *saoe = (ScalarArrayOpExpr *) node;
				ScalarArrayOpExpr *newnode;

				FLATCOPY(newnode, saoe, ScalarArrayOpExpr);
				return (Node *) newnode;
			}
		case T_SortGroupClause:
			{
				SortGroupClause *sgc = (SortGroupClause *) node;
				SortGroupClause *newnode;

				FLATCOPY(newnode, sgc, SortGroupClause);
				return (Node *) newnode;
			}

		default:
			elog(ERROR, "node type %d not supported", nodeTag(node));
			break;
	}
}

static void
mutate_plan_fields(Plan *newplan, Plan *oldplan, Node *(*mutator) (), void *context)
{
	/*
	 * Scalar fields startup_cost total_cost plan_rows plan_width nParamExec
	 * need no mutation.
	 */

	/* Node fields need mutation. */
	MUTATE(newplan->targetlist, oldplan->targetlist, List *);
	MUTATE(newplan->qual, oldplan->qual, List *);
	MUTATE(newplan->initPlan, oldplan->initPlan, List *);

	/* Bitmapsets aren't nodes but need to be copied to palloc'd space. */
	newplan->extParam = bms_copy(oldplan->extParam);
	newplan->allParam = bms_copy(oldplan->allParam);
}

/*
 * Replace the non-vectorirzed type to vectorized type
 */
Plan* 
gamma_vec_convert_plan(Node *node)
{
	return (Plan *)plan_tree_mutator(node, gamma_vec_convert_mutator, NULL);
}

Node *
gamma_vec_convert_node(Node *node)
{
	return (Node *)plan_tree_mutator(node, gamma_vec_convert_mutator, NULL);
}

Plan *
gamma_convert_plantree(Plan *plan, bool devec)
{
	bool sub_devec = devec;

	if (plan == NULL)
		return NULL;

	if (devec && IsA(plan, CustomScan))
		sub_devec = false;

	plan->lefttree = gamma_convert_plantree(plan->lefttree, sub_devec);
	plan->righttree = gamma_convert_plantree(plan->righttree, sub_devec);

	if (IsA(plan, CustomScan))
	{
		Plan *subplan = NULL;
		CustomScan *cscan = (CustomScan *) plan;
		if (cscan->custom_plans == NULL)
			return plan;

		if (gamma_is_indexscan_customscan(cscan) ||
			gamma_is_indexonlyscan_customscan(cscan))
			return plan;

		Assert(list_length(cscan->custom_plans) == 1);

		subplan = (Plan *) linitial(cscan->custom_plans);		
		subplan = gamma_convert_plantree(subplan, false);
		subplan = gamma_vec_convert_plan((Node *) subplan);

		cscan->custom_plans = list_make1(subplan);

		if (!devec && !IsA(subplan, Agg))
		{
			plan->targetlist =
				(List *) gamma_vec_convert_node((Node *) plan->targetlist);
		}
		else if (devec && !IsA(subplan, Agg))
		{
			plan = gamma_add_devector(cscan, subplan);
		}
	}

	return plan;
}
