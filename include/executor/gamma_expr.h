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

#ifndef GAMMA_EXPR_H
#define GAMMA_EXPR_H

#include "executor/execExpr.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"

typedef struct ExprSetupInfo
{
	/* Highest attribute numbers fetched from inner/outer/scan tuple slots: */
	AttrNumber	last_inner;
	AttrNumber	last_outer;
	AttrNumber	last_scan;
	/* MULTIEXPR SubPlan nodes appearing in the expression: */
	List	   *multiexpr_subplans;
} ExprSetupInfo;

extern void gamma_exec_init_expr_rec(Expr *node, ExprState *state,
				Datum *resv, bool *resnull);
extern void gamma_expr_eval_push_step(ExprState *es, const ExprEvalStep *s);
extern void gamma_exec_ready_expr(ExprState *state);
extern void gamma_exec_ready_interp_expr(ExprState *state);
extern ExprState * gamma_exec_init_expr(Expr *node, PlanState *parent);
extern bool gamma_expr_setup_walker(Node *node, ExprSetupInfo *info);
extern void gamma_exec_push_expr_setup_steps(ExprState *state, ExprSetupInfo *info);

extern ProjectionInfo * gamma_exec_build_proj_info(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						PlanState *parent,
						TupleDesc inputDesc);

#endif   /* GAMMA_EXPR_H */
