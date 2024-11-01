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

#include "access/nbtree.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execExpr.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/subscripting.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "executor/gamma_expr.h"
#include "executor/vector_tuple_slot.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"

typedef struct GammaSubPlanState
{
	SubPlanState state;

	/* the data type of result */
	Oid typeoid;
	bool typlen;
	bool typbyval;
	char typalign;

	/* row expression context */
	bool init_slot;
	ExprContext *row_exprcontext;
} GammaSubPlanState;

/*
 * Use computed-goto-based opcode dispatch when computed gotos are available.
 * But use a separate symbol so that it's easy to adjust locally in this file
 * for development and testing.
 */
#ifdef HAVE_COMPUTED_GOTO
#define EEO_USE_COMPUTED_GOTO
#endif							/* HAVE_COMPUTED_GOTO */

/*
 * Macros for opcode dispatch.
 *
 * EEO_SWITCH - just hides the switch if not in use.
 * EEO_CASE - labels the implementation of named expression step type.
 * EEO_DISPATCH - jump to the implementation of the step type for 'op'.
 * EEO_OPCODE - compute opcode required by used expression evaluation method.
 * EEO_NEXT - increment 'op' and jump to correct next step type.
 * EEO_JUMP - jump to the specified step number within the current expression.
 */
#if defined(EEO_USE_COMPUTED_GOTO)

/* struct for jump target -> opcode lookup table */
typedef struct ExprEvalOpLookup
{
	const void *opcode;
	ExprEvalOp	op;
} ExprEvalOpLookup;

/* to make dispatch_table accessible outside ExecInterpExpr() */
static const void **dispatch_table = NULL;

/* jump target -> opcode lookup table */
static ExprEvalOpLookup reverse_dispatch_table[EEOP_LAST];

#define EEO_SWITCH()
#define EEO_CASE(name)		CASE_##name:
#define EEO_DISPATCH()		goto *((void *) op->opcode)
#define EEO_OPCODE(opcode)	((intptr_t) dispatch_table[opcode])

#else							/* !EEO_USE_COMPUTED_GOTO */

#define EEO_SWITCH()		starteval: switch ((ExprEvalOp) op->opcode)
#define EEO_CASE(name)		case name:
#define EEO_DISPATCH()		goto starteval
#define EEO_OPCODE(opcode)	(opcode)

#endif							/* EEO_USE_COMPUTED_GOTO */

#define EEO_NEXT() \
	do { \
		op++; \
		EEO_DISPATCH(); \
	} while (0)

#define EEO_JUMP(stepno) \
	do { \
		op = &state->steps[stepno]; \
		EEO_DISPATCH(); \
	} while (0)

static Datum gamma_exec_interp_expr(ExprState *state,
				ExprContext *econtext, bool *isnull);
static pg_attribute_always_inline void
gamma_exec_agg_plain_trans_byval(AggState *aggstate, AggStatePerTrans pertrans,
					   AggStatePerGroup pergroup,
					   ExprContext *aggcontext, int setno);
static pg_attribute_always_inline void
gamma_exec_agg_plain_trans_byref(AggState *aggstate, AggStatePerTrans pertrans,
					   AggStatePerGroup pergroup,
					   ExprContext *aggcontext, int setno);
static void gamma_exec_eval_subplan(ExprState *state,
								ExprEvalStep *op, ExprContext *econtext);


#if defined(EEO_USE_COMPUTED_GOTO)
/*
 * Comparator used when building address->opcode lookup table for
 * ExecEvalStepOp() in the threaded dispatch case.
 */
static int
dispatch_compare_ptr(const void *a, const void *b)
{
	const ExprEvalOpLookup *la = (const ExprEvalOpLookup *) a;
	const ExprEvalOpLookup *lb = (const ExprEvalOpLookup *) b;

	if (la->opcode < lb->opcode)
		return -1;
	else if (la->opcode > lb->opcode)
		return 1;
	return 0;
}
#endif

/*
 * Check whether a user attribute in a slot can be referenced by a Var
 * expression.  This should succeed unless there have been schema changes
 * since the expression tree has been created.
 */
static void
CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)
{
	return;
	/*
	 * What we have to check for here is the possibility of an attribute
	 * having been dropped or changed in type since the plan tree was created.
	 * Ideally the plan will get invalidated and not re-used, but just in
	 * case, we keep these defenses.  Fortunately it's sufficient to check
	 * once on the first time through.
	 *
	 * Note: ideally we'd check typmod as well as typid, but that seems
	 * impractical at the moment: in many cases the tupdesc will have been
	 * generated by ExecTypeFromTL(), and that can't guarantee to generate an
	 * accurate typmod in all cases, because some expression node types don't
	 * carry typmod.  Fortunately, for precisely that reason, there should be
	 * no places with a critical dependency on the typmod of a value.
	 *
	 * System attributes don't require checking since their types never
	 * change.
	 */
	if (attnum > 0)
	{
		TupleDesc	slot_tupdesc = slot->tts_tupleDescriptor;
		Form_pg_attribute attr;

		if (attnum > slot_tupdesc->natts)	/* should never happen */
			elog(ERROR, "attribute number %d exceeds number of columns %d",
				 attnum, slot_tupdesc->natts);

		attr = TupleDescAttr(slot_tupdesc, attnum - 1);

		if (attr->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("attribute %d of type %s has been dropped",
							attnum, format_type_be(slot_tupdesc->tdtypeid))));

		if (vartype != attr->atttypid)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("attribute %d of type %s has wrong type",
							attnum, format_type_be(slot_tupdesc->tdtypeid)),
					 errdetail("Table has type %s, but query expects %s.",
							   format_type_be(attr->atttypid),
							   format_type_be(vartype))));
	}
}

/*
 * Verify that the slot is compatible with a EEOP_*_FETCHSOME operation.
 */
static void
CheckOpSlotCompatibility(ExprEvalStep *op, TupleTableSlot *slot)
{
#ifdef USE_ASSERT_CHECKING
	/* there's nothing to check */
	if (!op->d.fetch.fixed)
		return;

	/*
	 * Should probably fixed at some point, but for now it's easier to allow
	 * buffer and heap tuples to be used interchangeably.
	 */
	if (slot->tts_ops == &TTSOpsBufferHeapTuple &&
		op->d.fetch.kind == &TTSOpsHeapTuple)
		return;
	if (slot->tts_ops == &TTSOpsHeapTuple &&
		op->d.fetch.kind == &TTSOpsBufferHeapTuple)
		return;

	/*
	 * At the moment we consider it OK if a virtual slot is used instead of a
	 * specific type of slot, as a virtual slot never needs to be deformed.
	 */
	if (slot->tts_ops == &TTSOpsVirtual)
		return;

	Assert(op->d.fetch.kind == slot->tts_ops);
#endif
}

/*
 * Add steps performing expression setup as indicated by "info".
 * This is useful when building an ExprState covering more than one expression.
 */
void
gamma_exec_push_expr_setup_steps(ExprState *state, ExprSetupInfo *info)
{
	ExprEvalStep scratch = {0};
	ListCell   *lc;

	scratch.resvalue = NULL;
	scratch.resnull = NULL;
#if 0
	/*
	 * Add steps deforming the ExprState's inner/outer/scan slots as much as
	 * required by any Vars appearing in the expression.
	 */
	if (info->last_inner > 0)
	{
		scratch.opcode = EEOP_INNER_FETCHSOME;
		scratch.d.fetch.last_var = info->last_inner;
		scratch.d.fetch.fixed = false;
		scratch.d.fetch.kind = NULL;
		scratch.d.fetch.known_desc = NULL;
		if (ExecComputeSlotInfo(state, &scratch))
			gamma_expr_eval_push_step(state, &scratch);
	}
	if (info->last_outer > 0)
	{
		scratch.opcode = EEOP_OUTER_FETCHSOME;
		scratch.d.fetch.last_var = info->last_outer;
		scratch.d.fetch.fixed = false;
		scratch.d.fetch.kind = NULL;
		scratch.d.fetch.known_desc = NULL;
		if (ExecComputeSlotInfo(state, &scratch))
			gamma_expr_eval_push_step(state, &scratch);
	}
	if (info->last_scan > 0)
	{
		scratch.opcode = EEOP_SCAN_FETCHSOME;
		scratch.d.fetch.last_var = info->last_scan;
		scratch.d.fetch.fixed = false;
		scratch.d.fetch.kind = NULL;
		scratch.d.fetch.known_desc = NULL;
		if (ExecComputeSlotInfo(state, &scratch))
			gamma_expr_eval_push_step(state, &scratch);
	}
#endif
	/*
	 * Add steps to execute any MULTIEXPR SubPlans appearing in the
	 * expression.  We need to evaluate these before any of the Params
	 * referencing their outputs are used, but after we've prepared for any
	 * Var references they may contain.  (There cannot be cross-references
	 * between MULTIEXPR SubPlans, so we needn't worry about their order.)
	 */
	foreach(lc, info->multiexpr_subplans)
	{
		SubPlan    *subplan = (SubPlan *) lfirst(lc);
		SubPlanState *sstate;

		Assert(subplan->subLinkType == MULTIEXPR_SUBLINK);

		/* This should match what gamma_exec_init_expr_rec does for other SubPlans: */

		if (!state->parent)
			elog(ERROR, "SubPlan found with no parent plan");

		sstate = ExecInitSubPlan(subplan, state->parent);

		/* add SubPlanState nodes to state->parent->subPlan */
		state->parent->subPlan = lappend(state->parent->subPlan,
										 sstate);

		scratch.opcode = EEOP_SUBPLAN;
		scratch.d.subplan.sstate = sstate;

		/* The result can be ignored, but we better put it somewhere */
		scratch.resvalue = &state->resvalue;
		scratch.resnull = &state->resnull;

		gamma_expr_eval_push_step(state, &scratch);
	}
}

/*
 * expr_setup_walker: expression walker for ExecCreateExprSetupSteps
 */
bool
gamma_expr_setup_walker(Node *node, ExprSetupInfo *info)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *variable = (Var *) node;
		AttrNumber	attnum = variable->varattno;

		switch (variable->varno)
		{
			case INNER_VAR:
				info->last_inner = Max(info->last_inner, attnum);
				break;

			case OUTER_VAR:
				info->last_outer = Max(info->last_outer, attnum);
				break;

				/* INDEX_VAR is handled by default case */

			default:
				info->last_scan = Max(info->last_scan, attnum);
				break;
		}
		return false;
	}

	/* Collect all MULTIEXPR SubPlans, too */
	if (IsA(node, SubPlan))
	{
		SubPlan    *subplan = (SubPlan *) node;

		if (subplan->subLinkType == MULTIEXPR_SUBLINK)
			info->multiexpr_subplans = lappend(info->multiexpr_subplans,
											   subplan);
	}

	/*
	 * Don't examine the arguments or filters of Aggrefs or WindowFuncs,
	 * because those do not represent expressions to be evaluated within the
	 * calling expression's econtext.  GroupingFunc arguments are never
	 * evaluated at all.
	 */
	if (IsA(node, Aggref))
		return false;
	if (IsA(node, WindowFunc))
		return false;
	if (IsA(node, GroupingFunc))
		return false;
	return expression_tree_walker(node, gamma_expr_setup_walker,
								  (void *) info);
}

/*
 * Add another expression evaluation step to ExprState->steps.
 *
 * Note that this potentially re-allocates es->steps, therefore no pointer
 * into that array may be used while the expression is still being built.
 */
void
gamma_expr_eval_push_step(ExprState *es, const ExprEvalStep *s)
{
	if (es->steps_alloc == 0)
	{
		es->steps_alloc = 16;
		es->steps = palloc(sizeof(ExprEvalStep) * es->steps_alloc);
	}
	else if (es->steps_alloc == es->steps_len)
	{
		es->steps_alloc *= 2;
		es->steps = repalloc(es->steps,
							 sizeof(ExprEvalStep) * es->steps_alloc);
	}

	memcpy(&es->steps[es->steps_len++], s, sizeof(ExprEvalStep));
}

/*
 * Add expression steps performing setup that's needed before any of the
 * main execution of the expression.
 */
static void
gamma_exec_expr_setup_steps(ExprState *state, Node *node)
{
	ExprSetupInfo info = {0, 0, 0, NIL};

	/* Prescan to find out what we need. */
	gamma_expr_setup_walker(node, &info);

	/* And generate those steps. */
	gamma_exec_push_expr_setup_steps(state, &info);
}

/*
 * Perform setup necessary for the evaluation of a function-like expression,
 * appending argument evaluation steps to the steps list in *state, and
 * setting up *scratch so it is ready to be pushed.
 *
 * *scratch is not pushed here, so that callers may override the opcode,
 * which is useful for function-like cases like DISTINCT.
 */
static void
gamma_exec_init_func(ExprEvalStep *scratch, Expr *node, List *args, Oid funcid,
			 Oid inputcollid, ExprState *state)
{
	int			nargs = list_length(args);
	AclResult	aclresult;
	FmgrInfo   *flinfo;
	FunctionCallInfo fcinfo;
	int			argno;
	ListCell   *lc;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(funcid));
	InvokeFunctionExecuteHook(funcid);

	/*
	 * Safety check on nargs.  Under normal circumstances this should never
	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
	 * declared in pg_proc?
	 */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	/* Allocate function lookup data and parameter workspace for this call */
	scratch->d.func.finfo = palloc0(sizeof(FmgrInfo));
	scratch->d.func.fcinfo_data = palloc0(SizeForFunctionCallInfo(nargs));
	flinfo = scratch->d.func.finfo;
	fcinfo = scratch->d.func.fcinfo_data;

	/* Set up the primary fmgr lookup information */
	fmgr_info(funcid, flinfo);
	fmgr_info_set_expr((Node *) node, flinfo);

	/* Initialize function call parameter structure too */
	InitFunctionCallInfoData(*fcinfo, flinfo,
							 nargs, inputcollid, NULL, NULL);

	/* Keep extra copies of this info to save an indirection at runtime */
	scratch->d.func.fn_addr = flinfo->fn_addr;
	scratch->d.func.nargs = nargs;

	/* We only support non-set functions here */
	if (flinfo->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set"),
				 state->parent ?
				 executor_errposition(state->parent->state,
									  exprLocation((Node *) node)) : 0));

	/* Build code to evaluate arguments directly into the fcinfo struct */
	argno = 0;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			/*
			 * Don't evaluate const arguments every round; especially
			 * interesting for constants in comparisons.
			 */
			Const	   *con = (Const *) arg;

			fcinfo->args[argno].value = con->constvalue;
			fcinfo->args[argno].isnull = con->constisnull;
		}
		else
		{
			gamma_exec_init_expr_rec(arg, state,
							&fcinfo->args[argno].value,
							&fcinfo->args[argno].isnull);
		}
		argno++;
	}

	/* Insert appropriate opcode depending on strictness and stats level */
	if (pgstat_track_functions <= flinfo->fn_stats)
	{
		if (flinfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT;
		else
			scratch->opcode = EEOP_FUNCEXPR;
	}
	else
	{
		if (flinfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT_FUSAGE;
		else
			scratch->opcode = EEOP_FUNCEXPR_FUSAGE;
	}
}



void
gamma_exec_init_expr_rec(Expr *node, ExprState *state,
				Datum *resv, bool *resnull)
{
	ExprEvalStep scratch = {0};

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/* Step's output location is always what the caller gave us */
	Assert(resv != NULL && resnull != NULL);
	scratch.resvalue = resv;
	scratch.resnull = resnull;

	/* cases should be ordered as they are in enum NodeTag */
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *variable = (Var *) node;

				if (variable->varattno == InvalidAttrNumber)
				{
					/* whole-row Var */
					elog(ERROR, "ExecInitWholeRowVar is not supported");
					//TODO:ExecInitWholeRowVar(&scratch, variable, state);
				}
				else if (variable->varattno <= 0)
				{
					/* system column */
					scratch.d.var.attnum = variable->varattno;
					scratch.d.var.vartype = variable->vartype;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_SYSVAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_SYSVAR;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_SYSVAR;
							break;
					}
				}
				else
				{
					/* regular user column */
					scratch.d.var.attnum = variable->varattno - 1;
					scratch.d.var.vartype = variable->vartype;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_VAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_VAR;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_VAR;
							break;
					}
				}

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_Const:
			{
				Const	   *con = (Const *) node;

				scratch.opcode = EEOP_CONST;
				scratch.d.constval.value = con->constvalue;
				scratch.d.constval.isnull = con->constisnull;

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_Param:
			{
				Param	   *param = (Param *) node;
				ParamListInfo params;

				switch (param->paramkind)
				{
					case PARAM_EXEC:
						scratch.opcode = EEOP_PARAM_EXEC;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;
						gamma_expr_eval_push_step(state, &scratch);
						break;
					case PARAM_EXTERN:

						/*
						 * If we have a relevant ParamCompileHook, use it;
						 * otherwise compile a standard EEOP_PARAM_EXTERN
						 * step.  ext_params, if supplied, takes precedence
						 * over info from the parent node's EState (if any).
						 */
						if (state->ext_params)
							params = state->ext_params;
						else if (state->parent &&
								 state->parent->state)
							params = state->parent->state->es_param_list_info;
						else
							params = NULL;
						if (params && params->paramCompile)
						{
							params->paramCompile(params, param, state,
												 resv, resnull);
						}
						else
						{
							scratch.opcode = EEOP_PARAM_EXTERN;
							scratch.d.param.paramid = param->paramid;
							scratch.d.param.paramtype = param->paramtype;
							gamma_expr_eval_push_step(state, &scratch);
						}
						break;
					default:
						elog(ERROR, "unrecognized paramkind: %d",
							 (int) param->paramkind);
						break;
				}
				break;
			}

		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;

				scratch.opcode = EEOP_AGGREF;
				scratch.d.aggref.aggno = aggref->aggno;

				if (state->parent && IsA(state->parent, AggState))
				{
					AggState   *aggstate = (AggState *) state->parent;

					aggstate->aggs = lappend(aggstate->aggs, aggref);
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "Aggref found in non-Agg plan node");
				}

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_GroupingFunc:
			{
				GroupingFunc *grp_node = (GroupingFunc *) node;
				Agg		   *agg;

				if (!state->parent || !IsA(state->parent, AggState) ||
					!IsA(state->parent->plan, Agg))
					elog(ERROR, "GroupingFunc found in non-Agg plan node");

				scratch.opcode = EEOP_GROUPING_FUNC;

				agg = (Agg *) (state->parent->plan);

				if (agg->groupingSets)
					scratch.d.grouping_func.clauses = grp_node->cols;
				else
					scratch.d.grouping_func.clauses = NIL;

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				WindowFuncExprState *wfstate = makeNode(WindowFuncExprState);

				wfstate->wfunc = wfunc;

				if (state->parent && IsA(state->parent, WindowAggState))
				{
					WindowAggState *winstate = (WindowAggState *) state->parent;
					int			nfuncs;

					winstate->funcs = lappend(winstate->funcs, wfstate);
					nfuncs = ++winstate->numfuncs;
					if (wfunc->winagg)
						winstate->numaggs++;

					/* for now initialize agg using old style expressions */
					wfstate->args = ExecInitExprList(wfunc->args,
													 state->parent);
					wfstate->aggfilter = ExecInitExpr(wfunc->aggfilter,
													  state->parent);

					/*
					 * Complain if the windowfunc's arguments contain any
					 * windowfuncs; nested window functions are semantically
					 * nonsensical.  (This should have been caught earlier,
					 * but we defend against it here anyway.)
					 */
					if (nfuncs != winstate->numfuncs)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("window function calls cannot be nested")));
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "WindowFunc found in non-WindowAgg plan node");
				}

				scratch.opcode = EEOP_WINDOW_FUNC;
				scratch.d.window_func.wfstate = wfstate;
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_SubscriptingRef:
			{
				elog(ERROR, "T_SubscriptingRef is not supported");
				break;
			}

		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;

				gamma_exec_init_func(&scratch, node,
							 func->args, func->funcid, func->inputcollid,
							 state);
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;

				gamma_exec_init_func(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 state);
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_DistinctExpr:
			{
				DistinctExpr *op = (DistinctExpr *) node;

				gamma_exec_init_func(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 state);

				/*
				 * Change opcode of call instruction to EEOP_DISTINCT.
				 *
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation.  If
				 * we decided to do that here, we'd probably want separate
				 * opcodes for FUSAGE or not.
				 */
				scratch.opcode = EEOP_DISTINCT;
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_NullIfExpr:
			{
				NullIfExpr *op = (NullIfExpr *) node;

				gamma_exec_init_func(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 state);

				/*
				 * Change opcode of call instruction to EEOP_NULLIF.
				 *
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation.  If
				 * we decided to do that here, we'd probably want separate
				 * opcodes for FUSAGE or not.
				 */
				scratch.opcode = EEOP_NULLIF;
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				elog(ERROR, "T_ScalarArrayOpExpr is not supported");
				break;
			}

		case T_BoolExpr:
			{
				elog(ERROR, "BoolExpr is not used in GammaDB");
				break;
			}

		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;
				SubPlanState *sstate;
				GammaSubPlanState *gsstate;
				TypeCacheEntry *typentry;
				EState *estate = state->parent->state;

				/*
				 * Real execution of a MULTIEXPR SubPlan has already been
				 * done. What we have to do here is return a dummy NULL record
				 * value in case this targetlist element is assigned
				 * someplace.
				 */
				if (subplan->subLinkType == MULTIEXPR_SUBLINK)
				{
					elog(ERROR, "MULTIEXPR_SUBLINK is not supported.");
					break;
				}

				if (!state->parent)
					elog(ERROR, "SubPlan found with no parent plan");

				gsstate = (GammaSubPlanState *) palloc0(sizeof(GammaSubPlanState));
				sstate = ExecInitSubPlan(subplan, state->parent);
				memcpy((void*)gsstate, (void*)sstate, sizeof(SubPlanState));

				pfree(sstate);
				sstate = (SubPlanState *) gsstate;
				gsstate->row_exprcontext = CreateExprContext(estate);
				gsstate->init_slot = false;

				gsstate->typeoid = exprType((Node *) node);
				typentry = lookup_type_cache(gsstate->typeoid,
										TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);
				gsstate->typlen = typentry->typlen;
				gsstate->typbyval = typentry->typbyval;
				gsstate->typalign = typentry->typalign;

				/* add SubPlanState nodes to state->parent->subPlan */
				state->parent->subPlan = lappend(state->parent->subPlan,
												 sstate);

				scratch.opcode = EEOP_SUBPLAN;
				scratch.d.subplan.sstate = sstate;

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_FieldSelect:
		case T_FieldStore:
			{
				elog(ERROR, "T_FieldSelect/T_FieldStore is not supported");
				break;
			}

		case T_RelabelType:
			{
				/* relabel doesn't need to do anything at runtime */
				RelabelType *relabel = (RelabelType *) node;

				gamma_exec_init_expr_rec(relabel->arg, state, resv, resnull);
				break;
			}

		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
			{
				elog(ERROR, "T_CoerceViaIO/T_ArrayCoerceExpr is not supported");
				break;
			}

		case T_ConvertRowtypeExpr:
			{
				elog(ERROR, "T_ConvertRowtypeExpr is not supported");
				break;
			}

			/* note that CaseWhen expressions are handled within this block */
		case T_CaseExpr:
			{
				CaseExpr   *caseExpr = (CaseExpr *) node;
				List	   *adjust_jumps = NIL;
				Datum	   *caseval = NULL;
				bool	   *casenull = NULL;
				ListCell   *lc;

				/*
				 * If there's a test expression, we have to evaluate it and
				 * save the value where the CaseTestExpr placeholders can find
				 * it.
				 */
				if (caseExpr->arg != NULL)
				{
					/* Evaluate testexpr into caseval/casenull workspace */
					caseval = palloc(sizeof(Datum));
					casenull = palloc(sizeof(bool));

					gamma_exec_init_expr_rec(caseExpr->arg, state,
									caseval, casenull);

					/*
					 * Since value might be read multiple times, force to R/O
					 * - but only if it could be an expanded datum.
					 */
					if (get_typlen(exprType((Node *) caseExpr->arg)) == -1)
					{
						/* change caseval in-place */
						scratch.opcode = EEOP_MAKE_READONLY;
						scratch.resvalue = caseval;
						scratch.resnull = casenull;
						scratch.d.make_readonly.value = caseval;
						scratch.d.make_readonly.isnull = casenull;
						gamma_expr_eval_push_step(state, &scratch);
						/* restore normal settings of scratch fields */
						scratch.resvalue = resv;
						scratch.resnull = resnull;
					}
				}

				/*
				 * Prepare to evaluate each of the WHEN clauses in turn; as
				 * soon as one is true we return the value of the
				 * corresponding THEN clause.  If none are true then we return
				 * the value of the ELSE clause, or NULL if there is none.
				 */
				foreach(lc, caseExpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(lc);
					Datum	   *save_innermost_caseval;
					bool	   *save_innermost_casenull;
					int			whenstep;

					/*
					 * Make testexpr result available to CaseTestExpr nodes
					 * within the condition.  We must save and restore prior
					 * setting of innermost_caseval fields, in case this node
					 * is itself within a larger CASE.
					 *
					 * If there's no test expression, we don't actually need
					 * to save and restore these fields; but it's less code to
					 * just do so unconditionally.
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = caseval;
					state->innermost_casenull = casenull;

					/* evaluate condition into CASE's result variables */
					gamma_exec_init_expr_rec(when->expr, state, resv, resnull);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;

					/* If WHEN result isn't true, jump to next CASE arm */
					scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
					scratch.d.jump.jumpdone = -1;	/* computed later */
					gamma_expr_eval_push_step(state, &scratch);
					whenstep = state->steps_len - 1;

					/*
					 * If WHEN result is true, evaluate THEN result, storing
					 * it into the CASE's result variables.
					 */
					gamma_exec_init_expr_rec(when->result, state, resv, resnull);

					/* Emit JUMP step to jump to end of CASE's code */
					scratch.opcode = EEOP_JUMP;
					scratch.d.jump.jumpdone = -1;	/* computed later */
					gamma_expr_eval_push_step(state, &scratch);

					/*
					 * Don't know address for that jump yet, compute once the
					 * whole CASE expression is built.
					 */
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);

					/*
					 * But we can set WHEN test's jump target now, to make it
					 * jump to the next WHEN subexpression or the ELSE.
					 */
					state->steps[whenstep].d.jump.jumpdone = state->steps_len;
				}

				/* transformCaseExpr always adds a default */
				Assert(caseExpr->defresult);

				/* evaluate ELSE expr into CASE's result variables */
				gamma_exec_init_expr_rec(caseExpr->defresult, state,
								resv, resnull);

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}

		case T_CaseTestExpr:
			{
				/*
				 * Read from location identified by innermost_caseval.  Note
				 * that innermost_caseval could be NULL, if this node isn't
				 * actually within a CaseExpr, ArrayCoerceExpr, etc structure.
				 * That can happen because some parts of the system abuse
				 * CaseTestExpr to cause a read of a value externally supplied
				 * in econtext->caseValue_datum.  We'll take care of that
				 * scenario at runtime.
				 */
				scratch.opcode = EEOP_CASE_TESTVAL;
				scratch.d.casetest.value = state->innermost_caseval;
				scratch.d.casetest.isnull = state->innermost_casenull;

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				int			nelems = list_length(arrayexpr->elements);
				ListCell   *lc;
				int			elemoff;

				/*
				 * Evaluate by computing each element, and then forming the
				 * array.  Elements are computed into scratch arrays
				 * associated with the ARRAYEXPR step.
				 */
				scratch.opcode = EEOP_ARRAYEXPR;
				scratch.d.arrayexpr.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.arrayexpr.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.arrayexpr.nelems = nelems;

				/* fill remaining fields of step */
				scratch.d.arrayexpr.multidims = arrayexpr->multidims;
				scratch.d.arrayexpr.elemtype = arrayexpr->element_typeid;

				/* do one-time catalog lookup for type info */
				get_typlenbyvalalign(arrayexpr->element_typeid,
									 &scratch.d.arrayexpr.elemlength,
									 &scratch.d.arrayexpr.elembyval,
									 &scratch.d.arrayexpr.elemalign);

				/* prepare to evaluate all arguments */
				elemoff = 0;
				foreach(lc, arrayexpr->elements)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					gamma_exec_init_expr_rec(e, state,
									&scratch.d.arrayexpr.elemvalues[elemoff],
									&scratch.d.arrayexpr.elemnulls[elemoff]);
					elemoff++;
				}

				/* and then collect all into an array */
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_RowExpr:
		case T_RowCompareExpr:
			{
				elog(ERROR, "T_RowExpr/T_RowCompareExpr is not supported");
				break;
			}

		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				List	   *adjust_jumps = NIL;
				ListCell   *lc;

				/* We assume there's at least one arg */
				Assert(coalesce->args != NIL);

				/*
				 * Prepare evaluation of all coalesced arguments, after each
				 * one push a step that short-circuits if not null.
				 */
				foreach(lc, coalesce->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					/* evaluate argument, directly into result datum */
					gamma_exec_init_expr_rec(e, state, resv, resnull);

					/* if it's not null, skip to end of COALESCE expr */
					scratch.opcode = EEOP_JUMP_IF_NOT_NULL;
					scratch.d.jump.jumpdone = -1;	/* adjust later */
					gamma_expr_eval_push_step(state, &scratch);

					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
				}

				/*
				 * No need to add a constant NULL return - we only can get to
				 * the end of the expression if a NULL already is being
				 * returned.
				 */

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP_IF_NOT_NULL);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}

		case T_MinMaxExpr:
		case T_SQLValueFunction:
		case T_XmlExpr:
			{
				elog(ERROR, "T_MinMaxExpr/T_SQLValueFunction/T_XmlExpr is not supported");
				break;
			}

		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;

				if (ntest->nulltesttype == IS_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNULL;
				}
				else if (ntest->nulltesttype == IS_NOT_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNOTNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
				}
				else
				{
					elog(ERROR, "unrecognized nulltesttype: %d",
						 (int) ntest->nulltesttype);
				}
				/* initialize cache in case it's a row test */
				scratch.d.nulltest_row.rowcache.cacheptr = NULL;

				/* first evaluate argument into result variable */
				gamma_exec_init_expr_rec(ntest->arg, state,
								resv, resnull);

				/* then push the test of that argument */
				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;

				/*
				 * Evaluate argument, directly into result datum.  That's ok,
				 * because resv/resnull is definitely not used anywhere else,
				 * and will get overwritten by the below EEOP_BOOLTEST_IS_*
				 * step.
				 */
				gamma_exec_init_expr_rec(btest->arg, state, resv, resnull);

				switch (btest->booltesttype)
				{
					case IS_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_TRUE;
						break;
					case IS_NOT_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_TRUE;
						break;
					case IS_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_FALSE;
						break;
					case IS_NOT_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_FALSE;
						break;
					case IS_UNKNOWN:
						/* Same as scalar IS NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNULL;
						break;
					case IS_NOT_UNKNOWN:
						/* Same as scalar IS NOT NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) btest->booltesttype);
				}

				gamma_expr_eval_push_step(state, &scratch);
				break;
			}

		case T_CoerceToDomain:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
		case T_NextValueExpr:
			{
				elog(ERROR, "T_CoerceToDomain/T_CoerceToDomainValue"
					   "/T_CurrentOfExpr/T_NextValueExpr is not supported");
				break;
			}

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Prepare a compiled expression for execution.  This has to be called for
 * every ExprState before it can be executed.
 *
 * NB: While this currently only calls ExecReadyInterpretedExpr(),
 * this will likely get extended to further expression evaluation methods.
 * Therefore this should be used instead of directly calling
 * ExecReadyInterpretedExpr().
 */
void
gamma_exec_ready_expr(ExprState *state)
{
	//if (jit_compile_expr(state))
	//	return;

	gamma_exec_ready_interp_expr(state);
}

ExprState *
gamma_exec_init_expr(Expr *node, PlanState *parent)
{
	ExprState *state;
	ExprEvalStep scratch = {0};

	/* Special case: NULL expression produces a NULL ExprState pointer */
	if (node == NULL)
		return NULL;

	/* Initialize ExprState with empty step list */
	state = makeNode(ExprState);
	state->expr = node;
	state->parent = parent;
	state->ext_params = NULL;

	/* Insert setup steps as needed */
	gamma_exec_expr_setup_steps(state, (Node *) node);

	/* Compile the expression proper */
	gamma_exec_init_expr_rec(node, state, &state->resvalue, &state->resnull);

	/* Finally, append a DONE step */
	scratch.opcode = EEOP_DONE;
	gamma_expr_eval_push_step(state, &scratch);

	gamma_exec_ready_expr(state);

	return state;
}

/**********************************interpret**********************************/

static void gamma_check_expr_still_valid(ExprState *state, ExprContext *econtext);
static Datum gamma_exec_interp_expr_valid(ExprState *state,
								ExprContext *econtext, bool *isNull);
static ExprEvalOp gamma_exec_eval_step_op(ExprState *state, ExprEvalStep *op);


static void
gamma_exec_init_inter(void)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	/* Set up externally-visible pointer to dispatch table */
	if (dispatch_table == NULL)
	{
		dispatch_table = (const void **)
			DatumGetPointer(gamma_exec_interp_expr(NULL, NULL, NULL));

		/* build reverse lookup table */
		for (int i = 0; i < EEOP_LAST; i++)
		{
			reverse_dispatch_table[i].opcode = dispatch_table[i];
			reverse_dispatch_table[i].op = (ExprEvalOp) i;
		}

		/* make it bsearch()able */
		qsort(reverse_dispatch_table,
			  EEOP_LAST /* nmembers */ ,
			  sizeof(ExprEvalOpLookup),
			  dispatch_compare_ptr);
	}
#endif
}

/*
 * Expression evaluation callback that performs extra checks before executing
 * the expression. Declared extern so other methods of execution can use it
 * too.
 */
static Datum
gamma_exec_interp_expr_valid(ExprState *state, ExprContext *econtext, bool *isNull)
{
	/*
	 * First time through, check whether attribute matches Var.  Might not be
	 * ok anymore, due to schema changes.
	 */
	gamma_check_expr_still_valid(state, econtext);

	/* skip the check during further executions */
	state->evalfunc = (ExprStateEvalFunc) state->evalfunc_private;

	/* and actually execute */
	return state->evalfunc(state, econtext, isNull);
}

/*
 * Function to return the opcode of an expression step.
 *
 * When direct-threading is in use, ExprState->opcode isn't easily
 * decipherable. This function returns the appropriate enum member.
 */
static ExprEvalOp
gamma_exec_eval_step_op(ExprState *state, ExprEvalStep *op)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	if (state->flags & EEO_FLAG_DIRECT_THREADED)
	{
		ExprEvalOpLookup key;
		ExprEvalOpLookup *res;

		key.opcode = (void *) op->opcode;
		res = bsearch(&key,
					  reverse_dispatch_table,
					  EEOP_LAST /* nmembers */ ,
					  sizeof(ExprEvalOpLookup),
					  dispatch_compare_ptr);
		Assert(res);			/* unknown ops shouldn't get looked up */
		return res->op;
	}
#endif
	return (ExprEvalOp) op->opcode;
}

/*
 * Check that an expression is still valid in the face of potential schema
 * changes since the plan has been created.
 */
static void
gamma_check_expr_still_valid(ExprState *state, ExprContext *econtext)
{
	TupleTableSlot *innerslot;
	TupleTableSlot *outerslot;
	TupleTableSlot *scanslot;

	innerslot = econtext->ecxt_innertuple;
	outerslot = econtext->ecxt_outertuple;
	scanslot = econtext->ecxt_scantuple;

	for (int i = 0; i < state->steps_len; i++)
	{
		ExprEvalStep *op = &state->steps[i];

		switch (gamma_exec_eval_step_op(state, op))
		{
			case EEOP_INNER_VAR:
				{
					int			attnum = op->d.var.attnum;

					CheckVarSlotCompatibility(innerslot, attnum + 1, op->d.var.vartype);
					break;
				}

			case EEOP_OUTER_VAR:
				{
					int			attnum = op->d.var.attnum;

					CheckVarSlotCompatibility(outerslot, attnum + 1, op->d.var.vartype);
					break;
				}

			case EEOP_SCAN_VAR:
				{
					int			attnum = op->d.var.attnum;

					CheckVarSlotCompatibility(scanslot, attnum + 1, op->d.var.vartype);
					break;
				}
			default:
				break;
		}
	}
}

/*
 * Prepare ExprState for interpreted execution.
 */
void
gamma_exec_ready_interp_expr(ExprState *state)
{
	/* Ensure one-time interpreter setup has been done */
	gamma_exec_init_inter();

	/* Simple validity checks on expression */
	Assert(state->steps_len >= 1);
	Assert(state->steps[state->steps_len - 1].opcode == EEOP_DONE);

	/*
	 * Don't perform redundant initialization. This is unreachable in current
	 * cases, but might be hit if there's additional expression evaluation
	 * methods that rely on interpreted execution to work.
	 */
	if (state->flags & EEO_FLAG_INTERPRETER_INITIALIZED)
		return;

	/*
	 * First time through, check whether attribute matches Var.  Might not be
	 * ok anymore, due to schema changes. We do that by setting up a callback
	 * that does checking on the first call, which then sets the evalfunc
	 * callback to the actual method of execution.
	 */
	state->evalfunc = gamma_exec_interp_expr_valid;

	/* DIRECT_THREADED should not already be set */
	Assert((state->flags & EEO_FLAG_DIRECT_THREADED) == 0);

	/*
	 * There shouldn't be any errors before the expression is fully
	 * initialized, and even if so, it'd lead to the expression being
	 * abandoned.  So we can set the flag now and save some code.
	 */
	state->flags |= EEO_FLAG_INTERPRETER_INITIALIZED;

	/*
	 * Select fast-path evalfuncs for very simple expressions.  "Starting up"
	 * the full interpreter is a measurable overhead for these, and these
	 * patterns occur often enough to be worth optimizing.
	 */
#if 0
	if (state->steps_len == 3)
	{
		ExprEvalOp	step0 = state->steps[0].opcode;
		ExprEvalOp	step1 = state->steps[1].opcode;

		if (step0 == EEOP_INNER_FETCHSOME &&
			step1 == EEOP_INNER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME &&
				 step1 == EEOP_OUTER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME &&
				 step1 == EEOP_SCAN_VAR)
		{
			state->evalfunc_private = (void *) ExecJustScanVar;
			return;
		}
		else if (step0 == EEOP_INNER_FETCHSOME &&
				 step1 == EEOP_ASSIGN_INNER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignInnerVar;
			return;
		}
		else if (step0 == EEOP_OUTER_FETCHSOME &&
				 step1 == EEOP_ASSIGN_OUTER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignOuterVar;
			return;
		}
		else if (step0 == EEOP_SCAN_FETCHSOME &&
				 step1 == EEOP_ASSIGN_SCAN_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignScanVar;
			return;
		}
		else if (step0 == EEOP_CASE_TESTVAL &&
				 step1 == EEOP_FUNCEXPR_STRICT &&
				 state->steps[0].d.casetest.value)
		{
			state->evalfunc_private = (void *) ExecJustApplyFuncToCase;
			return;
		}
	}
	else if (state->steps_len == 2)
	{
		ExprEvalOp	step0 = state->steps[0].opcode;

		if (step0 == EEOP_CONST)
		{
			state->evalfunc_private = (void *) ExecJustConst;
			return;
		}
		else if (step0 == EEOP_INNER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustInnerVarVirt;
			return;
		}
		else if (step0 == EEOP_OUTER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustOuterVarVirt;
			return;
		}
		else if (step0 == EEOP_SCAN_VAR)
		{
			state->evalfunc_private = (void *) ExecJustScanVarVirt;
			return;
		}
		else if (step0 == EEOP_ASSIGN_INNER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignInnerVarVirt;
			return;
		}
		else if (step0 == EEOP_ASSIGN_OUTER_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignOuterVarVirt;
			return;
		}
		else if (step0 == EEOP_ASSIGN_SCAN_VAR)
		{
			state->evalfunc_private = (void *) ExecJustAssignScanVarVirt;
			return;
		}
	}
#endif
#if defined(EEO_USE_COMPUTED_GOTO)

	/*
	 * In the direct-threaded implementation, replace each opcode with the
	 * address to jump to.  (Use ExecEvalStepOp() to get back the opcode.)
	 */
	for (int off = 0; off < state->steps_len; off++)
	{
		ExprEvalStep *op = &state->steps[off];

		op->opcode = EEO_OPCODE(op->opcode);
	}

	state->flags |= EEO_FLAG_DIRECT_THREADED;
#endif							/* EEO_USE_COMPUTED_GOTO */

	state->evalfunc_private = (void *) gamma_exec_interp_expr;
}

/*
 * Evaluate expression identified by "state" in the execution context
 * given by "econtext".  *isnull is set to the is-null flag for the result,
 * and the Datum value is the function result.
 *
 * As a special case, return the dispatch table's address if state is NULL.
 * This is used by ExecInitInterpreter to set up the dispatch_table global.
 * (Only applies when EEO_USE_COMPUTED_GOTO is defined.)
 */
static Datum
gamma_exec_interp_expr(ExprState *state, ExprContext *econtext, bool *isnull)
{
	ExprEvalStep *op;
	TupleTableSlot *resultslot;
	TupleTableSlot *innerslot;
	TupleTableSlot *outerslot;
	TupleTableSlot *scanslot;

	/*
	 * This array has to be in the same order as enum ExprEvalOp.
	 */
#if defined(EEO_USE_COMPUTED_GOTO)
	static const void *const dispatch_table[] = {
		&&CASE_EEOP_DONE,
		&&CASE_EEOP_INNER_FETCHSOME,
		&&CASE_EEOP_OUTER_FETCHSOME,
		&&CASE_EEOP_SCAN_FETCHSOME,
		&&CASE_EEOP_INNER_VAR,
		&&CASE_EEOP_OUTER_VAR,
		&&CASE_EEOP_SCAN_VAR,
		&&CASE_EEOP_INNER_SYSVAR,
		&&CASE_EEOP_OUTER_SYSVAR,
		&&CASE_EEOP_SCAN_SYSVAR,
		&&CASE_EEOP_WHOLEROW,
		&&CASE_EEOP_ASSIGN_INNER_VAR,
		&&CASE_EEOP_ASSIGN_OUTER_VAR,
		&&CASE_EEOP_ASSIGN_SCAN_VAR,
		&&CASE_EEOP_ASSIGN_TMP,
		&&CASE_EEOP_ASSIGN_TMP_MAKE_RO,
		&&CASE_EEOP_CONST,
		&&CASE_EEOP_FUNCEXPR,
		&&CASE_EEOP_FUNCEXPR_STRICT,
		&&CASE_EEOP_FUNCEXPR_FUSAGE,
		&&CASE_EEOP_FUNCEXPR_STRICT_FUSAGE,
		&&CASE_EEOP_BOOL_AND_STEP_FIRST,
		&&CASE_EEOP_BOOL_AND_STEP,
		&&CASE_EEOP_BOOL_AND_STEP_LAST,
		&&CASE_EEOP_BOOL_OR_STEP_FIRST,
		&&CASE_EEOP_BOOL_OR_STEP,
		&&CASE_EEOP_BOOL_OR_STEP_LAST,
		&&CASE_EEOP_BOOL_NOT_STEP,
		&&CASE_EEOP_QUAL,
		&&CASE_EEOP_JUMP,
		&&CASE_EEOP_JUMP_IF_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_NULL,
		&&CASE_EEOP_JUMP_IF_NOT_TRUE,
		&&CASE_EEOP_NULLTEST_ISNULL,
		&&CASE_EEOP_NULLTEST_ISNOTNULL,
		&&CASE_EEOP_NULLTEST_ROWISNULL,
		&&CASE_EEOP_NULLTEST_ROWISNOTNULL,
		&&CASE_EEOP_BOOLTEST_IS_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_TRUE,
		&&CASE_EEOP_BOOLTEST_IS_FALSE,
		&&CASE_EEOP_BOOLTEST_IS_NOT_FALSE,
		&&CASE_EEOP_PARAM_EXEC,
		&&CASE_EEOP_PARAM_EXTERN,
		&&CASE_EEOP_PARAM_CALLBACK,
		&&CASE_EEOP_CASE_TESTVAL,
		&&CASE_EEOP_MAKE_READONLY,
		&&CASE_EEOP_IOCOERCE,
		&&CASE_EEOP_DISTINCT,
		&&CASE_EEOP_NOT_DISTINCT,
		&&CASE_EEOP_NULLIF,
		&&CASE_EEOP_SQLVALUEFUNCTION,
		&&CASE_EEOP_CURRENTOFEXPR,
		&&CASE_EEOP_NEXTVALUEEXPR,
		&&CASE_EEOP_ARRAYEXPR,
		&&CASE_EEOP_ARRAYCOERCE,
		&&CASE_EEOP_ROW,
		&&CASE_EEOP_ROWCOMPARE_STEP,
		&&CASE_EEOP_ROWCOMPARE_FINAL,
		&&CASE_EEOP_MINMAX,
		&&CASE_EEOP_FIELDSELECT,
		&&CASE_EEOP_FIELDSTORE_DEFORM,
		&&CASE_EEOP_FIELDSTORE_FORM,
		&&CASE_EEOP_SBSREF_SUBSCRIPTS,
		&&CASE_EEOP_SBSREF_OLD,
		&&CASE_EEOP_SBSREF_ASSIGN,
		&&CASE_EEOP_SBSREF_FETCH,
		&&CASE_EEOP_DOMAIN_TESTVAL,
		&&CASE_EEOP_DOMAIN_NOTNULL,
		&&CASE_EEOP_DOMAIN_CHECK,
		&&CASE_EEOP_CONVERT_ROWTYPE,
		&&CASE_EEOP_SCALARARRAYOP,
		&&CASE_EEOP_HASHED_SCALARARRAYOP,
		&&CASE_EEOP_XMLEXPR,
		&&CASE_EEOP_AGGREF,
		&&CASE_EEOP_GROUPING_FUNC,
		&&CASE_EEOP_WINDOW_FUNC,
		&&CASE_EEOP_SUBPLAN,
		&&CASE_EEOP_AGG_STRICT_DESERIALIZE,
		&&CASE_EEOP_AGG_DESERIALIZE,
		&&CASE_EEOP_AGG_STRICT_INPUT_CHECK_ARGS,
		&&CASE_EEOP_AGG_STRICT_INPUT_CHECK_NULLS,
		&&CASE_EEOP_AGG_PLAIN_PERGROUP_NULLCHECK,
		&&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
		&&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL,
		&&CASE_EEOP_AGG_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
		&&CASE_EEOP_AGG_PLAIN_TRANS_STRICT_BYREF,
		&&CASE_EEOP_AGG_PLAIN_TRANS_BYREF,
		&&CASE_EEOP_AGG_ORDERED_TRANS_DATUM,
		&&CASE_EEOP_AGG_ORDERED_TRANS_TUPLE,
		&&CASE_EEOP_LAST
	};

	StaticAssertStmt(EEOP_LAST + 1 == lengthof(dispatch_table),
					 "dispatch_table out of whack with ExprEvalOp");

	if (unlikely(state == NULL))
		return PointerGetDatum(dispatch_table);
#else
	Assert(state != NULL);
#endif							/* EEO_USE_COMPUTED_GOTO */

	/* setup state */
	op = state->steps;
	resultslot = state->resultslot;
	innerslot = econtext->ecxt_innertuple;
	outerslot = econtext->ecxt_outertuple;
	scanslot = econtext->ecxt_scantuple;

#if defined(EEO_USE_COMPUTED_GOTO)
	EEO_DISPATCH();
#endif

	EEO_SWITCH()
	{
		EEO_CASE(EEOP_DONE)
		{
			goto out;
		}

		EEO_CASE(EEOP_INNER_FETCHSOME)
		{
			CheckOpSlotCompatibility(op, innerslot);

			slot_getsomeattrs(innerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_FETCHSOME)
		{
			CheckOpSlotCompatibility(op, outerslot);

			slot_getsomeattrs(outerslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_FETCHSOME)
		{
			CheckOpSlotCompatibility(op, scanslot);

			slot_getsomeattrs(scanslot, op->d.fetch.last_var);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_VAR)
		{
			int attnum = op->d.var.attnum;
			vdatum *vec_value;
			VectorTupleSlot *vinnerslot = (VectorTupleSlot *) innerslot;

			Assert(attnum >= 0 && attnum < innerslot->tts_nvalid);
			vec_value = (vdatum *) innerslot->tts_values[attnum];
			vec_value->skipref = vinnerslot->skip;
			vec_value->indexarr = vinnerslot->row_indexarr;	
			*op->resvalue = PointerGetDatum(vec_value);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_VAR)
		{
			int attnum = op->d.var.attnum;
			vdatum *vec_value;
			VectorTupleSlot *vouterslot = (VectorTupleSlot *) outerslot;

			Assert(attnum >= 0 && attnum < outerslot->tts_nvalid);
			vec_value = (vdatum *) outerslot->tts_values[attnum];
			vec_value->skipref = vouterslot->skip;
			vec_value->indexarr = vouterslot->row_indexarr;	
			*op->resvalue = PointerGetDatum(vec_value);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_VAR)
		{
			int attnum = op->d.var.attnum;
			vdatum *vec_value;
			VectorTupleSlot *vscanslot = (VectorTupleSlot *) scanslot;

			Assert(attnum >= 0 && attnum < scanslot->tts_nvalid);
			vec_value = (vdatum *) scanslot->tts_values[attnum];
			vec_value->skipref = vscanslot->skip;
			vec_value->indexarr = vscanslot->row_indexarr;	
			*op->resvalue = PointerGetDatum(vec_value);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_INNER_SYSVAR)
		{
			ExecEvalSysVar(state, op, econtext, innerslot);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_OUTER_SYSVAR)
		{
			ExecEvalSysVar(state, op, econtext, outerslot);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_SCAN_SYSVAR)
		{
			ExecEvalSysVar(state, op, econtext, scanslot);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_WHOLEROW)
		{
			/* too complex for an inline implementation */
			ExecEvalWholeRowVar(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_INNER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(attnum >= 0 && attnum < innerslot->tts_nvalid);
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = innerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = innerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_OUTER_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(attnum >= 0 && attnum < outerslot->tts_nvalid);
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = outerslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = outerslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_SCAN_VAR)
		{
			int			resultnum = op->d.assign_var.resultnum;
			int			attnum = op->d.assign_var.attnum;

			/*
			 * We do not need CheckVarSlotCompatibility here; that was taken
			 * care of at compilation time.  But see EEOP_INNER_VAR comments.
			 */
			Assert(attnum >= 0 && attnum < scanslot->tts_nvalid);
			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = scanslot->tts_values[attnum];
			resultslot->tts_isnull[resultnum] = scanslot->tts_isnull[attnum];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_values[resultnum] = state->resvalue;
			resultslot->tts_isnull[resultnum] = state->resnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ASSIGN_TMP_MAKE_RO)
		{
			int			resultnum = op->d.assign_tmp.resultnum;

			Assert(resultnum >= 0 && resultnum < resultslot->tts_tupleDescriptor->natts);
			resultslot->tts_isnull[resultnum] = state->resnull;
			if (!resultslot->tts_isnull[resultnum])
				resultslot->tts_values[resultnum] =
					MakeExpandedObjectReadOnlyInternal(state->resvalue);
			else
				resultslot->tts_values[resultnum] = state->resvalue;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CONST)
		{
			*op->resnull = op->d.constval.isnull;
			*op->resvalue = op->d.constval.value;

			EEO_NEXT();
		}

		/*
		 * Function-call implementations. Arguments have previously been
		 * evaluated directly into fcinfo->args.
		 *
		 * As both STRICT checks and function-usage are noticeable performance
		 * wise, and function calls are a very hot-path (they also back
		 * operators!), it's worth having so many separate opcodes.
		 *
		 * Note: the reason for using a temporary variable "d", here and in
		 * other places, is that some compilers think "*op->resvalue = f();"
		 * requires them to evaluate op->resvalue into a register before
		 * calling f(), just in case f() is able to modify op->resvalue
		 * somehow.  The extra line of code can save a useless register spill
		 * and reload across the function call.
		 */
		EEO_CASE(EEOP_FUNCEXPR)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
			Datum		d;

			fcinfo->isnull = false;
			d = op->d.func.fn_addr(fcinfo);
			*op->resvalue = d;
			*op->resnull = fcinfo->isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_STRICT)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
			NullableDatum *args = fcinfo->args;
			int			nargs = op->d.func.nargs;
			Datum		d;

			/* strict function, so check for NULL args */
			for (int argno = 0; argno < nargs; argno++)
			{
				if (args[argno].isnull)
				{
					*op->resnull = true;
					goto strictfail;
				}
			}
			fcinfo->isnull = false;
			d = op->d.func.fn_addr(fcinfo);
			*op->resvalue = d;
			*op->resnull = fcinfo->isnull;

	strictfail:
			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_FUSAGE)
		{
			/* not common enough to inline */
			ExecEvalFuncExprFusage(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_FUNCEXPR_STRICT_FUSAGE)
		{
			/* not common enough to inline */
			ExecEvalFuncExprStrictFusage(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOL_AND_STEP_FIRST)
		EEO_CASE(EEOP_BOOL_AND_STEP)
		EEO_CASE(EEOP_BOOL_AND_STEP_LAST)
		EEO_CASE(EEOP_BOOL_OR_STEP_FIRST)
		EEO_CASE(EEOP_BOOL_OR_STEP)
		EEO_CASE(EEOP_BOOL_OR_STEP_LAST)
		EEO_CASE(EEOP_BOOL_NOT_STEP)
		EEO_CASE(EEOP_QUAL)
		{
			elog(ERROR, "BoolExpr is not used in GammaDB.");
			EEO_NEXT();
		}
		EEO_CASE(EEOP_JUMP)
		{
			/* Unconditionally jump to target step */
			EEO_JUMP(op->d.jump.jumpdone);
		}

		EEO_CASE(EEOP_JUMP_IF_NULL)
		{
			/* Transfer control if current result is null */
			if (*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_NULL)
		{
			/* Transfer control if current result is non-null */
			if (!*op->resnull)
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_JUMP_IF_NOT_TRUE)
		{
			/* Transfer control if current result is null or false */
			if (*op->resnull || !DatumGetBool(*op->resvalue))
				EEO_JUMP(op->d.jump.jumpdone);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNULL)
		{
			*op->resvalue = BoolGetDatum(*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ISNOTNULL)
		{
			*op->resvalue = BoolGetDatum(!*op->resnull);
			*op->resnull = false;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLTEST_ROWISNULL)
		EEO_CASE(EEOP_NULLTEST_ROWISNOTNULL)
		{
			elog(ERROR, "Row is not used in GammaDB.");
			EEO_NEXT();
		}

		/* BooleanTest implementations for all booltesttypes */

		EEO_CASE(EEOP_BOOLTEST_IS_TRUE)
		{
			if (*op->resnull)
			{
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			/* else, input value is the correct output as well */

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_TRUE)
		{
			if (*op->resnull)
			{
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_FALSE)
		{
			if (*op->resnull)
			{
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			else
				*op->resvalue = BoolGetDatum(!DatumGetBool(*op->resvalue));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_BOOLTEST_IS_NOT_FALSE)
		{
			if (*op->resnull)
			{
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			/* else, input value is the correct output as well */

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXEC)
		{
			/* out of line implementation: too large */
			ExecEvalParamExec(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_EXTERN)
		{
			/* out of line implementation: too large */
			ExecEvalParamExtern(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_PARAM_CALLBACK)
		{
			/* allow an extension module to supply a PARAM_EXTERN value */
			op->d.cparam.paramfunc(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_CASE_TESTVAL)
		EEO_CASE(EEOP_DOMAIN_TESTVAL)
		{
			elog(ERROR, "TESTVAL is not used in GammaDB.");
			EEO_NEXT();
		}

		EEO_CASE(EEOP_MAKE_READONLY)
		{
			/*
			 * Force a varlena value that might be read multiple times to R/O
			 */
			if (!*op->d.make_readonly.isnull)
				*op->resvalue =
					MakeExpandedObjectReadOnlyInternal(*op->d.make_readonly.value);
			*op->resnull = *op->d.make_readonly.isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_IOCOERCE)
		{
			elog(ERROR, "EEOP_IOCOERCE is not used in GammaDB.");
			EEO_NEXT();
		}

		EEO_CASE(EEOP_DISTINCT)
		{
			/*
			 * IS DISTINCT FROM must evaluate arguments (already done into
			 * fcinfo->args) to determine whether they are NULL; if either is
			 * NULL then the result is determined.  If neither is NULL, then
			 * proceed to evaluate the comparison function, which is just the
			 * type's standard equality operator.  We need not care whether
			 * that function is strict.  Because the handling of nulls is
			 * different, we can't just reuse EEOP_FUNCEXPR.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* check function arguments for NULLness */
			if (fcinfo->args[0].isnull && fcinfo->args[1].isnull)
			{
				/* Both NULL? Then is not distinct... */
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			else if (fcinfo->args[0].isnull || fcinfo->args[1].isnull)
			{
				/* Only one is NULL? Then is distinct... */
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			else
			{
				/* Neither null, so apply the equality function */
				Datum		eqresult;

				fcinfo->isnull = false;
				eqresult = op->d.func.fn_addr(fcinfo);
				/* Must invert result of "="; safe to do even if null */
				*op->resvalue = BoolGetDatum(!DatumGetBool(eqresult));
				*op->resnull = fcinfo->isnull;
			}

			EEO_NEXT();
		}

		/* see EEOP_DISTINCT for comments, this is just inverted */
		EEO_CASE(EEOP_NOT_DISTINCT)
		{
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			if (fcinfo->args[0].isnull && fcinfo->args[1].isnull)
			{
				*op->resvalue = BoolGetDatum(true);
				*op->resnull = false;
			}
			else if (fcinfo->args[0].isnull || fcinfo->args[1].isnull)
			{
				*op->resvalue = BoolGetDatum(false);
				*op->resnull = false;
			}
			else
			{
				Datum		eqresult;

				fcinfo->isnull = false;
				eqresult = op->d.func.fn_addr(fcinfo);
				*op->resvalue = eqresult;
				*op->resnull = fcinfo->isnull;
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_NULLIF)
		{
			/*
			 * The arguments are already evaluated into fcinfo->args.
			 */
			FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

			/* if either argument is NULL they can't be equal */
			if (!fcinfo->args[0].isnull && !fcinfo->args[1].isnull)
			{
				Datum		result;

				fcinfo->isnull = false;
				result = op->d.func.fn_addr(fcinfo);

				/* if the arguments are equal return null */
				if (!fcinfo->isnull && DatumGetBool(result))
				{
					*op->resvalue = (Datum) 0;
					*op->resnull = true;

					EEO_NEXT();
				}
			}

			/* Arguments aren't equal, so return the first one */
			*op->resvalue = fcinfo->args[0].value;
			*op->resnull = fcinfo->args[0].isnull;

			EEO_NEXT();
		}

		EEO_CASE(EEOP_CURRENTOFEXPR)
		EEO_CASE(EEOP_SQLVALUEFUNCTION)
		EEO_CASE(EEOP_NEXTVALUEEXPR)
		{
			elog(ERROR, "EEOP_CURRENTOFEXPR/EEOP_SQLVALUEFUNCTION"
					"/EEOP_NEXTVALUEEXPR is not used in GammaDB.");
			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYEXPR)
		{
			/* too complex for an inline implementation */
			ExecEvalArrayExpr(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_ARRAYCOERCE)
		EEO_CASE(EEOP_ROW)
		EEO_CASE(EEOP_ROWCOMPARE_STEP)
		EEO_CASE(EEOP_ROWCOMPARE_FINAL)
		EEO_CASE(EEOP_MINMAX)
		EEO_CASE(EEOP_FIELDSELECT)
		EEO_CASE(EEOP_FIELDSTORE_DEFORM)
		EEO_CASE(EEOP_FIELDSTORE_FORM)
		EEO_CASE(EEOP_SBSREF_SUBSCRIPTS)
		EEO_CASE(EEOP_SBSREF_OLD)
		EEO_CASE(EEOP_SBSREF_ASSIGN)
		EEO_CASE(EEOP_SBSREF_FETCH)
		EEO_CASE(EEOP_CONVERT_ROWTYPE)
		EEO_CASE(EEOP_SCALARARRAYOP)
		EEO_CASE(EEOP_HASHED_SCALARARRAYOP)
		EEO_CASE(EEOP_DOMAIN_NOTNULL)
		EEO_CASE(EEOP_DOMAIN_CHECK)
		EEO_CASE(EEOP_XMLEXPR)
		{
			elog(ERROR, "EEOP_ARRAYCOERCE... is not used in GammaDB.");
			/* too complex for an inline implementation */
			ExecEvalArrayCoerce(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_AGGREF)
		{
			/*
			 * Returns a Datum whose value is the precomputed aggregate value
			 * found in the given expression context.
			 */
			int			aggno = op->d.aggref.aggno;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[aggno];
			*op->resnull = econtext->ecxt_aggnulls[aggno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_GROUPING_FUNC)
		{
			/* too complex/uncommon for an inline implementation */
			ExecEvalGroupingFunc(state, op);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_WINDOW_FUNC)
		{
			/*
			 * Like Aggref, just return a precomputed value from the econtext.
			 */
			WindowFuncExprState *wfunc = op->d.window_func.wfstate;

			Assert(econtext->ecxt_aggvalues != NULL);

			*op->resvalue = econtext->ecxt_aggvalues[wfunc->wfuncno];
			*op->resnull = econtext->ecxt_aggnulls[wfunc->wfuncno];

			EEO_NEXT();
		}

		EEO_CASE(EEOP_SUBPLAN)
		{
			/* too complex for an inline implementation */
			gamma_exec_eval_subplan(state, op, econtext);

			EEO_NEXT();
		}

		/* evaluate a strict aggregate deserialization function */
		EEO_CASE(EEOP_AGG_STRICT_DESERIALIZE)
		{
			/* Don't call a strict deserialization function with NULL input */
			if (op->d.agg_deserialize.fcinfo_data->args[0].isnull)
				EEO_JUMP(op->d.agg_deserialize.jumpnull);

			/* fallthrough */
		}

		/* evaluate aggregate deserialization function (non-strict portion) */
		EEO_CASE(EEOP_AGG_DESERIALIZE)
		{
			FunctionCallInfo fcinfo = op->d.agg_deserialize.fcinfo_data;
			AggState   *aggstate = castNode(AggState, state->parent);
			MemoryContext oldContext;

			/*
			 * We run the deserialization functions in per-input-tuple memory
			 * context.
			 */
			oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
			fcinfo->isnull = false;
			*op->resvalue = FunctionCallInvoke(fcinfo);
			*op->resnull = fcinfo->isnull;
			MemoryContextSwitchTo(oldContext);

			EEO_NEXT();
		}

		/*
		 * Check that a strict aggregate transition / combination function's
		 * input is not NULL.
		 */

		EEO_CASE(EEOP_AGG_STRICT_INPUT_CHECK_ARGS)
		{
			NullableDatum *args = op->d.agg_strict_input_check.args;
			int			nargs = op->d.agg_strict_input_check.nargs;

			for (int argno = 0; argno < nargs; argno++)
			{
				if (args[argno].isnull)
					EEO_JUMP(op->d.agg_strict_input_check.jumpnull);
			}
			EEO_NEXT();
		}

		EEO_CASE(EEOP_AGG_STRICT_INPUT_CHECK_NULLS)
		{
			bool	   *nulls = op->d.agg_strict_input_check.nulls;
			int			nargs = op->d.agg_strict_input_check.nargs;

			for (int argno = 0; argno < nargs; argno++)
			{
				if (nulls[argno])
					EEO_JUMP(op->d.agg_strict_input_check.jumpnull);
			}
			EEO_NEXT();
		}

		/*
		 * Check for a NULL pointer to the per-group states.
		 */

		EEO_CASE(EEOP_AGG_PLAIN_PERGROUP_NULLCHECK)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerGroup pergroup_allaggs =
			aggstate->all_pergroups[op->d.agg_plain_pergroup_nullcheck.setoff];

			if (pergroup_allaggs == NULL)
				EEO_JUMP(op->d.agg_plain_pergroup_nullcheck.jumpnull);

			EEO_NEXT();
		}

		/*
		 * Different types of aggregate transition functions are implemented
		 * as different types of steps, to avoid incurring unnecessary
		 * overhead.  There's a step type for each valid combination of having
		 * a by value / by reference transition type, [not] needing to the
		 * initialize the transition value for the first row in a group from
		 * input, and [not] strict transition function.
		 *
		 * Could optimize further by splitting off by-reference for
		 * fixed-length types, but currently that doesn't seem worth it.
		 */

		EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(pertrans->transtypeByVal);

			if (pergroup->noTransValue)
			{
				/* If transValue has not yet been initialized, do so now. */
				ExecAggInitGroup(aggstate, pertrans, pergroup,
								 op->d.agg_trans.aggcontext);
				/* copied trans value from input, done this round */
			}
			else if (likely(!pergroup->transValueIsNull))
			{
				/* invoke transition function, unless prevented by strictness */
				gamma_exec_agg_plain_trans_byval(aggstate, pertrans, pergroup,
									   op->d.agg_trans.aggcontext,
									   op->d.agg_trans.setno);
			}

			EEO_NEXT();
		}

		/* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
		EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(pertrans->transtypeByVal);

			if (likely(!pergroup->transValueIsNull))
				gamma_exec_agg_plain_trans_byval(aggstate, pertrans, pergroup,
									   op->d.agg_trans.aggcontext,
									   op->d.agg_trans.setno);

			EEO_NEXT();
		}

		/* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
		EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYVAL)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(pertrans->transtypeByVal);

			gamma_exec_agg_plain_trans_byval(aggstate, pertrans, pergroup,
								   op->d.agg_trans.aggcontext,
								   op->d.agg_trans.setno);

			EEO_NEXT();
		}

		/* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
		EEO_CASE(EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(!pertrans->transtypeByVal);

			if (pergroup->noTransValue)
				ExecAggInitGroup(aggstate, pertrans, pergroup,
								 op->d.agg_trans.aggcontext);
			else if (likely(!pergroup->transValueIsNull))
				gamma_exec_agg_plain_trans_byref(aggstate, pertrans, pergroup,
									   op->d.agg_trans.aggcontext,
									   op->d.agg_trans.setno);

			EEO_NEXT();
		}

		/* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
		EEO_CASE(EEOP_AGG_PLAIN_TRANS_STRICT_BYREF)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(!pertrans->transtypeByVal);

			if (likely(!pergroup->transValueIsNull))
				gamma_exec_agg_plain_trans_byref(aggstate, pertrans, pergroup,
									   op->d.agg_trans.aggcontext,
									   op->d.agg_trans.setno);
			EEO_NEXT();
		}

		/* see comments above EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL */
		EEO_CASE(EEOP_AGG_PLAIN_TRANS_BYREF)
		{
			AggState   *aggstate = castNode(AggState, state->parent);
			AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
			AggStatePerGroup pergroup =
			&aggstate->all_pergroups[op->d.agg_trans.setoff][op->d.agg_trans.transno];

			Assert(!pertrans->transtypeByVal);

			gamma_exec_agg_plain_trans_byref(aggstate, pertrans, pergroup,
								   op->d.agg_trans.aggcontext,
								   op->d.agg_trans.setno);

			EEO_NEXT();
		}

		/* process single-column ordered aggregate datum */
		EEO_CASE(EEOP_AGG_ORDERED_TRANS_DATUM)
		{
			/* too complex for an inline implementation */
			ExecEvalAggOrderedTransDatum(state, op, econtext);

			EEO_NEXT();
		}

		/* process multi-column ordered aggregate tuple */
		EEO_CASE(EEOP_AGG_ORDERED_TRANS_TUPLE)
		{
			/* too complex for an inline implementation */
			ExecEvalAggOrderedTransTuple(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_LAST)
		{
			/* unreachable */
			Assert(false);
			goto out;
		}
	}

out:
	*isnull = state->resnull;
	return state->resvalue;
}

/* implementation of transition function invocation for byval types */
static pg_attribute_always_inline void
gamma_exec_agg_plain_trans_byval(AggState *aggstate, AggStatePerTrans pertrans,
					   AggStatePerGroup pergroup,
					   ExprContext *aggcontext, int setno)
{
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	/* cf. select_current_set() */
	aggstate->curaggcontext = aggcontext;
	aggstate->current_set = setno;

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/* invoke transition function in per-tuple context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	fcinfo->args[0].value = pergroup->transValue;
	fcinfo->args[0].isnull = pergroup->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	pergroup->transValue = newVal;
	pergroup->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/* implementation of transition function invocation for byref types */
static pg_attribute_always_inline void
gamma_exec_agg_plain_trans_byref(AggState *aggstate, AggStatePerTrans pertrans,
					   AggStatePerGroup pergroup,
					   ExprContext *aggcontext, int setno)
{
	FunctionCallInfo fcinfo = pertrans->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	/* cf. select_current_set() */
	aggstate->curaggcontext = aggcontext;
	aggstate->current_set = setno;

	/* set up aggstate->curpertrans for AggGetAggref() */
	aggstate->curpertrans = pertrans;

	/* invoke transition function in per-tuple context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	fcinfo->args[0].value = pergroup->transValue;
	fcinfo->args[0].isnull = pergroup->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	/*
	 * For pass-by-ref datatype, must copy the new value into aggcontext and
	 * free the prior transValue.  But if transfn returned a pointer to its
	 * first input, we don't need to do anything.  Also, if transfn returned a
	 * pointer to a R/W expanded object that is already a child of the
	 * aggcontext, assume we can adopt that value without copying it.
	 *
	 * It's safe to compare newVal with pergroup->transValue without regard
	 * for either being NULL, because ExecAggTransReparent() takes care to set
	 * transValue to 0 when NULL. Otherwise we could end up accidentally not
	 * reparenting, when the transValue has the same numerical value as
	 * newValue, despite being NULL.  This is a somewhat hot path, making it
	 * undesirable to instead solve this with another branch for the common
	 * case of the transition function returning its (modified) input
	 * argument.
	 */
	if (DatumGetPointer(newVal) != DatumGetPointer(pergroup->transValue))
		newVal = ExecAggTransReparent(aggstate, pertrans,
									  newVal, fcinfo->isnull,
									  pergroup->transValue,
									  pergroup->transValueIsNull);

	pergroup->transValue = newVal;
	pergroup->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Hand off evaluation of a subplan to nodeSubplan.c
 */
static void
gamma_exec_eval_subplan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	int i;
	SubPlanState *sstate = op->d.subplan.sstate;
	GammaSubPlanState *gsstate = (GammaSubPlanState *) sstate;
	ExprContext *rcontext = gsstate->row_exprcontext;
	Datum value;
	bool isnull;
	bool *skip;

	TupleTableSlot *scanslot = econtext->ecxt_scantuple;
	TupleTableSlot *innerslot = econtext->ecxt_innertuple;
	TupleTableSlot *outerslot = econtext->ecxt_outertuple;

	/* could potentially be nested, so make sure there's enough stack */
	check_stack_depth();

	if (!gsstate->init_slot)
	{
		TupleDesc rowdesc;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(rcontext->ecxt_per_tuple_memory);

		if (scanslot != NULL)
		{
			rowdesc = CreateTupleDescCopy(scanslot->tts_tupleDescriptor);
			rowdesc = de_vec_tupledesc(rowdesc);
			rcontext->ecxt_scantuple = MakeTupleTableSlot(rowdesc, &TTSOpsVirtual);
		}

		if (innerslot != NULL)
		{
			rowdesc = CreateTupleDescCopy(innerslot->tts_tupleDescriptor);
			rowdesc = de_vec_tupledesc(rowdesc);
			rcontext->ecxt_innertuple = MakeTupleTableSlot(rowdesc, &TTSOpsVirtual);
		}

		if (outerslot != NULL)
		{
			rowdesc = CreateTupleDescCopy(outerslot->tts_tupleDescriptor);
			rowdesc = de_vec_tupledesc(rowdesc);
			rcontext->ecxt_outertuple = MakeTupleTableSlot(rowdesc, &TTSOpsVirtual);
		}

		skip = (bool *)palloc(sizeof(bool) * VECTOR_SIZE);
		*op->resvalue = (Datum)buildvdatum(gsstate->typeoid, VECTOR_SIZE, skip);
		MemoryContextSwitchTo(oldcontext);

		gsstate->init_slot = true;
	}

	for (i = 0; i < VECTOR_SIZE; i++)
	{
		isnull = false;

		if (!tts_vector_slot_get_skip(scanslot, i))
			tts_vector_slot_copy_one_row(rcontext->ecxt_scantuple, scanslot, i);

		if (!tts_vector_slot_get_skip(innerslot, i))
			tts_vector_slot_copy_one_row(rcontext->ecxt_outertuple, innerslot, i);

		if (!tts_vector_slot_get_skip(outerslot, i))
			tts_vector_slot_copy_one_row(rcontext->ecxt_outertuple, outerslot, i);

		value = ExecSubPlan(sstate, rcontext, &isnull);

		if (isnull)
		{
			VDATUM_SET_ISNULL(((vdatum *)*op->resvalue), i, true);
		}
		else
		{
			VDATUM_SET_DATUM(((vdatum *)*op->resvalue), i, value);
			VDATUM_SET_ISNULL(((vdatum *)*op->resvalue), i, false);
		}
	}
}

ProjectionInfo *
gamma_exec_build_proj_info(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						PlanState *parent,
						TupleDesc inputDesc)
{
	ProjectionInfo *projInfo = makeNode(ProjectionInfo);
	ExprState  *state;
	ExprEvalStep scratch = {0};
	ListCell   *lc;

	projInfo->pi_exprContext = econtext;
	/* We embed ExprState into ProjectionInfo instead of doing extra palloc */
	projInfo->pi_state.type = T_ExprState;
	state = &projInfo->pi_state;
	state->expr = (Expr *) targetList;
	state->parent = parent;
	state->ext_params = NULL;

	state->resultslot = slot;

	/* Insert setup steps as needed */
	gamma_exec_expr_setup_steps(state, (Node *) targetList);

	/* Now compile each tlist column */
	foreach(lc, targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *variable = NULL;
		AttrNumber	attnum = 0;
		bool		isSafeVar = false;

		/*
		 * If tlist expression is a safe non-system Var, use the fast-path
		 * ASSIGN_*_VAR opcodes.  "Safe" means that we don't need to apply
		 * CheckVarSlotCompatibility() during plan startup.  If a source slot
		 * was provided, we make the equivalent tests here; if a slot was not
		 * provided, we assume that no check is needed because we're dealing
		 * with a non-relation-scan-level expression.
		 */
		if (tle->expr != NULL &&
			IsA(tle->expr, Var) &&
			((Var *) tle->expr)->varattno > 0)
		{
			/* Non-system Var, but how safe is it? */
			variable = (Var *) tle->expr;
			attnum = variable->varattno;

			if (inputDesc == NULL)
				isSafeVar = true;	/* can't check, just assume OK */
			else if (attnum <= inputDesc->natts)
			{
				Form_pg_attribute attr = TupleDescAttr(inputDesc, attnum - 1);

				/*
				 * If user attribute is dropped or has a type mismatch, don't
				 * use ASSIGN_*_VAR.  Instead let the normal expression
				 * machinery handle it (which'll possibly error out).
				 */
				if (!attr->attisdropped && variable->vartype == attr->atttypid)
				{
					isSafeVar = true;
				}
			}
		}

		if (isSafeVar)
		{
			/* Fast-path: just generate an EEOP_ASSIGN_*_VAR step */
			switch (variable->varno)
			{
				case INNER_VAR:
					/* get the tuple from the inner node */
					scratch.opcode = EEOP_ASSIGN_INNER_VAR;
					break;

				case OUTER_VAR:
					/* get the tuple from the outer node */
					scratch.opcode = EEOP_ASSIGN_OUTER_VAR;
					break;

					/* INDEX_VAR is handled by default case */

				default:
					/* get the tuple from the relation being scanned */
					scratch.opcode = EEOP_ASSIGN_SCAN_VAR;
					break;
			}

			scratch.d.assign_var.attnum = attnum - 1;
			scratch.d.assign_var.resultnum = tle->resno - 1;
			gamma_expr_eval_push_step(state, &scratch);
		}
		else
		{
			/*
			 * Otherwise, compile the column expression normally.
			 *
			 * We can't tell the expression to evaluate directly into the
			 * result slot, as the result slot (and the exprstate for that
			 * matter) can change between executions.  We instead evaluate
			 * into the ExprState's resvalue/resnull and then move.
			 */
			gamma_exec_init_expr_rec(tle->expr, state,
							&state->resvalue, &state->resnull);

			/*
			 * Column might be referenced multiple times in upper nodes, so
			 * force value to R/O - but only if it could be an expanded datum.
			 */
			if (get_typlen(exprType((Node *) tle->expr)) == -1)
				scratch.opcode = EEOP_ASSIGN_TMP_MAKE_RO;
			else
				scratch.opcode = EEOP_ASSIGN_TMP;
			scratch.d.assign_tmp.resultnum = tle->resno - 1;
			gamma_expr_eval_push_step(state, &scratch);
		}
	}

	scratch.opcode = EEOP_DONE;
	gamma_expr_eval_push_step(state, &scratch);

	gamma_exec_ready_expr(state);

	return projInfo;
}
