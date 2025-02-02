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

#include "storage/gamma_buffer.h"
#include "storage/gamma_dsm.h"
#include "storage/gamma_toc.h"

void
gamma_buffer_startup(void)
{
	gamma_buffer_dsm_startup();
}

bool
gamma_buffer_add_cv(Oid relid, Oid rgid, int attno,
		uint32 dim, char *data, Size values_nbytes, bool *nulls, Size isnull_nbytes)
{
	volatile gamma_toc *toc = gamma_buffer_dsm_toc();
	gamma_toc_entry *entry;
	char *entry_addr = NULL;

	Size nbytes;
	char *lookup_values;
	bool *lookup_isnull;
	Size lookup_v_nbytes;
	Size lookup_n_nbytes;
	uint32 lookup_dim;

	Size align_v_nbytes;
	Size align_n_nbytes;

	gamma_toc_lock_acquire_x((gamma_toc *)toc);

	align_v_nbytes = BUFFERALIGN(values_nbytes);
	align_n_nbytes = BUFFERALIGN(isnull_nbytes);
	nbytes = align_v_nbytes + align_n_nbytes;

	/* check if the other session have been insert the ColumnVector */
	if (gamma_toc_lookup((gamma_toc *)toc, relid, rgid, attno, &lookup_dim,
				&lookup_values, &lookup_v_nbytes,
				&lookup_isnull, &lookup_n_nbytes))
	{
		Assert(values_nbytes == lookup_v_nbytes);
		Assert(isnull_nbytes == lookup_n_nbytes);
		gamma_toc_lock_release((gamma_toc *)toc);
		return true;
	}

	/* initialize the toc entry */
	entry = gamma_toc_alloc((gamma_toc *)toc, nbytes);
	if (entry == NULL)
	{
		gamma_toc_lock_release((gamma_toc *)toc);
		return false;
	}

	entry_addr = gamma_toc_addr((gamma_toc *)toc, entry);
	memcpy(entry_addr, data, values_nbytes);
	if (nulls != NULL)
		memcpy(entry_addr + align_v_nbytes, nulls, isnull_nbytes);

	entry->relid = relid;
	entry->rgid = rgid;
	entry->attno = attno;
	entry->dim = dim;
	entry->values_nbytes = values_nbytes;
	entry->isnull_nbytes = isnull_nbytes;

	gamma_toc_lock_release((gamma_toc *)toc);

	return true;
}

bool
gamma_buffer_get_cv(Oid relid, Oid rgid, int16 attno, uint32 *dim,
			char **data, Size *values_nbytes, bool **nulls, Size *isnull_nbytes)
{
	bool result = false;
	gamma_toc *toc = gamma_buffer_dsm_toc();
	gamma_toc_lock_acquire_s(toc);
	result = gamma_toc_lookup(toc, relid, rgid, attno, dim, data, values_nbytes,
							nulls, isnull_nbytes);
	gamma_toc_lock_release(toc);
	return result;
}

void
gamma_buffer_invalid_rel(Oid relid)
{
	gamma_toc *toc = gamma_buffer_dsm_toc();
	gamma_toc_lock_acquire_x(toc);
	gamma_toc_invalid_rel(toc, relid);
	gamma_toc_lock_release(toc);
}
