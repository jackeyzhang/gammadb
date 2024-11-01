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

#ifndef GAMMA_TOC_H
#define GAMMA_TOC_H


#include "postgres.h"
#include "port/atomics.h"

typedef struct gamma_toc_entry
{
	Oid relid;
	Oid rgid;
	int16 attno;
	int16 flags;			/* for memory align, nouse now */
	Size nbytes;			/* toc memory size */
	Size values_offset;		/* Offset, in bytes, from TOC start */
	Size values_nbytes;		/* values array size (not aligned) */
	Size isnull_nbytes;		/* nulls array size (not aligned) */
	Size dim;
	pg_atomic_uint32 state;
} gamma_toc_entry;

typedef struct gamma_toc gamma_toc;

#define GAMMA_TOC_MAGIC (20101030)

extern gamma_toc *gamma_toc_create(uint64 magic, void *address, Size nbytes);
extern gamma_toc *gamma_toc_attach(uint64 magic, void *address);
extern bool gamma_toc_insert(gamma_toc *toc, Oid relid, Oid rgid, int16 attno,
				uint32 dim, char *data, Size values_nbytes,
				bool *nulls, Size isnull_nbytes);
extern bool gamma_toc_lookup(gamma_toc *toc, Oid relid, Oid rgid, int16 attno,
				uint32 *dim, char **data, Size *values_nbytes,
				bool **nulls, Size *isnull_nbytes);

extern void gamma_toc_invalid_rel(gamma_toc *toc, Oid relid);
extern void gamma_toc_invalid_rg(gamma_toc *toc, Oid relid, uint32 rgid);
extern void gamma_toc_invalid_cv(gamma_toc *toc, Oid relid, uint32 rgid, int16 attno);

extern void gamma_toc_lock_acquire_x(gamma_toc *toc);
extern void gamma_toc_lock_acquire_s(gamma_toc *toc);
extern void gamma_toc_lock_release(gamma_toc *toc);

extern gamma_toc_entry* gamma_toc_alloc(gamma_toc *toc, Size nbytes);
extern char * gamma_toc_addr(gamma_toc *toc, gamma_toc_entry *entry);

#endif

