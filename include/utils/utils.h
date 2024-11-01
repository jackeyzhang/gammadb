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

#ifndef UTILS_H
#define UTILS_H

#include "postgres.h"

#include "access/tupdesc.h"
#include "common/hashfn.h"
#include "nodes/execnodes.h"

#include "vdatum/vvarlena.h"


extern Oid en_vec_type(Oid ntype);
extern Oid de_vec_type(Oid vtype);
extern Oid en_vec_tupdesc_attr(TupleDesc tupdesc, int i);
extern TupleDesc de_vec_tupledesc(TupleDesc tupdesc);
extern TupleDesc en_vec_tupledesc(TupleDesc tupdesc);

extern List* gamma_pull_vars_of_level(Node *node, int levelsup);
extern uint32 gamma_hash_bytes(const char *k, int32 length);
extern uint32 fnv1a_hash_int32(int k);

extern bool is_vec_type(Oid ntype);

#define PROC_OID_HASH_INT2		449
#define PROC_OID_HASH_INT4		450
#define PROC_OID_HASH_INT8		949
#define PROC_OID_HASH_FLOAT4	451
#define PROC_OID_HASH_FLOAT8	452
#define PROC_OID_HASH_OID		453
#define PROC_OID_HASH_NUMERIC	432
#define PROC_OID_HASH_TEXT		400

static inline uint32
gamma_hash_datum(FmgrInfo *hash_fcinfo, Datum value, Oid collid)
{
	uint32 hkey;

	Assert(hash_fcinfo != NULL);

	switch (hash_fcinfo->fn_oid)
	{
		case PROC_OID_HASH_OID:
		case PROC_OID_HASH_INT2:
		case PROC_OID_HASH_INT4:
			{
				hkey = DatumGetUInt32(value);
				break;
			}
		case PROC_OID_HASH_INT8:
			{
				uint64 ikey64 = DatumGetUInt64(value);
				uint32 ikey32;
				ikey32 = (uint32)ikey64;
				hkey = ikey32 ^ (ikey64 >> 32);
				break;
			}
		case PROC_OID_HASH_FLOAT4:
			{
				float4 key = DatumGetFloat4(value);
				if (key == (float4) 0)
				{
					hkey = 0;
					break;
				}

				hkey = *(uint32 *)&key;
				break;
			}
		case PROC_OID_HASH_FLOAT8:
			{
				float8 key = DatumGetFloat4(value);
				uint64 ikey64;// = *(uint64)&key;
				uint32 ikey32;
				if (key == (float4) 0)
				{
					hkey = 0;
					break;
				}

				ikey64 = *(uint64 *) &key;
				ikey32 = (uint32)ikey64;
				hkey = ikey32 ^ (ikey64 >> 32);
				break;
			}
			//case PROC_OID_HASH_NUMERIC:
			//	{
			//		Numeric key = DatumGetNumeric(VDATUM_DATUM(vec_value, j));
			//		hkey = gamma_hash_numeric(key);
			//		break;
			//	}
		case PROC_OID_HASH_TEXT:
			{
				text *key = 
					(text *)DatumGetPointer(value);
				hkey = gamma_hash_text(key, collid);
				break;
			}
		default:
			{
				hkey = DatumGetUInt32(FunctionCall1Coll(hash_fcinfo,
							collid, value));
			}
	}

	return hkey;
}
#endif
