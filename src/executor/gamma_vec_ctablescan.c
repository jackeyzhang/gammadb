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

#include "access/relscan.h"
#include "access/heapam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

#include "nodes/extensible.h"
#include "executor/nodeCustom.h"
#include "utils/memutils.h"


#include "executor/gamma_vec_ctablescan.h"
#include "executor/gamma_vec_tablescan.h"
#include "executor/vec_exec_scan.h"
#include "executor/vector_tuple_slot.h"
#include "storage/gamma_scankeys.h"
#include "storage/ctable_vec_am.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"


TupleTableSlot *
vec_ctablescan_access_seqnext(ScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	Plan *plan;
	Bitmapset *bms_proj = NULL;

	/* vectorized */
	SeqScanState	*state = (SeqScanState *)node;
	VecSeqScanState *vstate = (VecSeqScanState *)node;
	CTableScanDesc vscandesc;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = state->ss.ss_currentScanDesc;
	estate = state->ss.ps.state;
	direction = estate->es_direction;
	slot = state->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		uint32      flags = 0;
#if PG_VERSION_NUM >= 170000
		flags = SO_TYPE_SEQSCAN |
							SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;
#endif
		scandesc = vec_ctable_beginscan(state->ss.ss_currentRelation,
								  estate->es_snapshot,
								  0, NULL, NULL, flags);
		state->ss.ss_currentScanDesc = scandesc;
	}

	vscandesc = (CTableScanDesc) scandesc;
	if (vscandesc->cvscan != NULL && vscandesc->cvscan->bms_proj == NULL)
	{
		plan = node->ps.plan;
		pull_varattnos((Node *)plan->targetlist,
						((Scan *)plan)->scanrelid, &bms_proj);
		pull_varattnos((Node *)plan->qual, ((Scan *)plan)->scanrelid, &bms_proj);

		vscandesc->cvscan->bms_proj = bms_proj;

		gamma_sk_set_scankeys(vscandesc->cvscan, state);
	}

	/* return the last batch. */
	if (vstate->scan_over)
	{
		ExecClearTuple(slot);
		return slot;
	}

	ExecClearTuple(slot);


	/* fetch a batch of rows and fill them into VectorTupleSlot */
	if (!vec_ctable_getnextslot(scandesc, direction, slot))
	{
		/* scan finish, but we still need to emit current vslot */
		vstate->scan_over = true;
	}

	return slot;
}

bool
vec_ctablescan_access_recheck(ScanState *node, TupleTableSlot *slot)
{
	return true;
}



