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

#include <assert.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>

#include "postgres.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "access/tupmacs.h"
#include "common/hashfn.h"

#include "storage/gamma_cv.h"
#include "utils/utils.h"
#include "utils/vdatum/vdatum.h"


PG_FUNCTION_INFO_V1(gamma_count_distinct_transition);
PG_FUNCTION_INFO_V1(gamma_count_distinct_serial);
PG_FUNCTION_INFO_V1(gamma_count_distinct_deserial);
PG_FUNCTION_INFO_V1(gamma_count_distinct_combine);
PG_FUNCTION_INFO_V1(gamma_count_distinct_final);

typedef struct DistinctAggHashEntry
{
	Datum		key;
	uint32		hash;			/* Hash value (cached) */
	char		status;
} DistinctAggHashEntry;


#define SH_PREFIX distinct_agg
#define SH_ELEMENT_TYPE DistinctAggHashEntry
#define SH_KEY_TYPE Datum
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

static uint32 distinct_agg_hash_method(struct distinct_agg_hash *tb,
							   const Datum key);
static bool distinct_agg_equal_method(struct distinct_agg_hash *tb,
							  const Datum params1,
							  const Datum params2);

typedef struct DistinctAggState
{
	distinct_agg_hash *tb;
	FmgrInfo hash_funcs;
	FmgrInfo eq_funcs;

	/* for entry copy */
	Oid typeoid;
	int16 typlen;
	bool typbyval;
	char typalign;
	Oid collid;
	uint64 processed;
} DistinctAggState;

#define SH_PREFIX distinct_agg
#define SH_ELEMENT_TYPE DistinctAggHashEntry
#define SH_KEY_TYPE Datum
#define SH_KEY key
#define SH_HASH_KEY(tb, key) distinct_agg_hash_method(tb, key)
#define SH_EQUAL(tb, a, b) distinct_agg_equal_method(tb, a, b)
#define SH_SCOPE static inline
#define SH_STORE_HASH
#define SH_MANUAL_GROW
#define SH_GET_HASH(tb, a) a->hash
#define SH_DEFINE
#include "lib/simplehash.h"

#define SH_STORE_HASH
static DistinctAggHashEntry *
gamma_distinct_agg_insert_hash(struct distinct_agg_hash *tb,
								Datum key,
								uint32 hash, bool *found)
{
	uint32		startelem;
	uint32		curelem;
	DistinctAggHashEntry *data;

	/* perform insert, start bucket search at optimal location */
	data = tb->data;
	startelem = distinct_agg_initial_bucket(tb, hash);
	curelem = startelem;
	while (true)
	{
		DistinctAggHashEntry *entry = &data[curelem];

		/* any empty bucket can directly be used */
		if (entry->status == 0/*TODO: EMPTY*/)
		{
			tb->members++;
			entry->key = key;
#ifdef SH_STORE_HASH
			entry->hash = hash;
#endif
			entry->status = 1;/*TODO:SH_STATUS_IN_USE*/;
			*found = false;
			return entry;
		}

		if (entry->hash == hash && distinct_agg_equal_method(tb, entry->key, key))
		{
			Assert(entry->status == 1/*TODO:SH_STATUS_IN_USE*/);
			*found = true;
			return entry;
		}

		curelem = distinct_agg_next(tb, curelem, startelem);
	}
}

#undef SH_STORE_HASH
static void
gamma_distinct_agg_grow(struct distinct_agg_hash *tb, Size incr)
{
	DistinctAggState *dastate = (DistinctAggState *) tb->private_data;
	uint64 newsize = 0;
	uint32 members = tb->members + incr;

	if (unlikely(members >= tb->grow_threshold))
	{
        if (unlikely(tb->size == PG_UINT32_MAX))
			elog(LOG, "hash table size exceeded");

		newsize = (tb->size + incr) * 2;
	}

	if (unlikely(tb->size - members <  tb->size / 3))
		newsize = (tb->size + incr) * 2;

	if (unlikely(newsize > 0))
		newsize = newsize > dastate->processed / 5 ? newsize : dastate->processed/5;

	if (unlikely(newsize > 0))
		distinct_agg_grow(tb, newsize);
}

static uint32 distinct_agg_hash_method(struct distinct_agg_hash *tb,
							   const Datum key)
{
	Assert(false);
	return 0;
}

static bool distinct_agg_equal_method(struct distinct_agg_hash *tb,
							  const Datum params1,
							  const Datum params2)
{
	DistinctAggState *dastate = (DistinctAggState *) tb->private_data;
	return DatumGetBool(FunctionCall2Coll(&dastate->eq_funcs, dastate->collid,
				params1, params2));
}

Datum
gamma_count_distinct_transition(PG_FUNCTION_ARGS)
{
	Oid collid = PG_GET_COLLATION();
	vdatum *vec_value;
	MemoryContext	oldcontext;
	MemoryContext	aggcontext;
	DistinctAggState *dastate;
	uint32 hashkey;
	Datum value;

	int j;
	uint32		hkey = 0;
	DistinctAggHashEntry *daentry;
	bool found = false;

	/* Figure out which context we want the result in */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "gamma_count_distinct_transition error.");

	if (PG_ARGISNULL(1) && PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else if (PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	else if (PG_ARGISNULL(0))
	{
		Oid type;
		Oid eq_proc;
		TypeCacheEntry *typentry;
		vec_value = (vdatum *) PG_GETARG_POINTER(1);

		/* get non-vec type */
		type = de_vec_type(vec_value->elemtype);
		if (type == InvalidOid)
			type = vec_value->elemtype;

		typentry = lookup_type_cache(type, TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

		oldcontext = MemoryContextSwitchTo(aggcontext);
		dastate = (DistinctAggState *) palloc0(sizeof(DistinctAggState));
		fmgr_info_cxt(typentry->hash_proc, &dastate->hash_funcs, aggcontext);
		eq_proc = get_opcode(typentry->eq_opr);
		fmgr_info_cxt(eq_proc, &dastate->eq_funcs, aggcontext);
		dastate->tb = distinct_agg_create(aggcontext, 1024, dastate);
		dastate->typlen = typentry->typlen;
		dastate->typbyval = typentry->typbyval;
		dastate->typalign = typentry->typalign;
		dastate->typeoid = type;
		dastate->collid = collid;
		dastate->processed = 0;
	}
	else
	{
		oldcontext = MemoryContextSwitchTo(aggcontext);
		vec_value = (vdatum *) PG_GETARG_POINTER(1);
		dastate = (DistinctAggState *) PG_GETARG_POINTER(0);
		dastate->processed += vec_value->dim;
	}

	gamma_distinct_agg_grow(dastate->tb, VECTOR_SIZE);
	if (vec_value->indexarr != NULL)
	{
		int16 k;
		j = 0;
		while ((k = vec_value->indexarr[j]) != -1)
		{
			if (unlikely(j >= VECTOR_SIZE))
				break;

			/* skip null values */
			if (VDATUM_ISNULL(vec_value, k))
			{
				j++;
				continue;
			}

			value = VDATUM_DATUM(vec_value, k);
			hashkey = 0;

			/* 
			 * GAMMA NOTE: The initial value of hashkeys is 0, 
			 * and the result of the 
			 * '(hashkeys[k] << 1) | ((hashkeys[k] & 0x80000000) ? 1 : 0)'
			 * operation is also guaranteed to be 0, so this step can be 
			 * ignored.
			 */
			/* hashkeys[k] =
			 *	(hashkeys[k] << 1) | ((hashkeys[k] & 0x80000000) ? 1 : 0);*/
			hkey = gamma_hash_datum(&dastate->hash_funcs, value, collid);
			hashkey ^= hkey;
			hashkey = murmurhash32(hashkey);

			found = false;

			daentry = gamma_distinct_agg_insert_hash(dastate->tb,
					value, hashkey, &found);
			if (found)
			{
				j++;
				continue;
			}

			daentry->key = value;
			j++;
		}
	}
	else
	{
		for (j = 0; j < vec_value->dim; j++)
		{
			if (vec_value->skipref != NULL && vec_value->skipref[j])
				continue;

			/* skip null values */
			if (VDATUM_ISNULL(vec_value, j))
				continue;

			value = VDATUM_DATUM(vec_value, j);
			hashkey = 0;

			/* rotate hashkey left 1 bit at each step */
			/* hashkeys[j] =
			 * (hashkeys[j] << 1) | ((hashkeys[j] & 0x80000000) ? 1 : 0);*/
			hkey = gamma_hash_datum(&dastate->hash_funcs,
					value, collid);

			hashkey ^= hkey;
			hashkey = murmurhash32(hashkey);
			found = false;
			daentry = gamma_distinct_agg_insert_hash(dastate->tb,
					value, hashkey, &found);
			if (found)
				continue;

			//TODO: need copy?
			daentry->key = value;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(dastate);
}

Datum
gamma_count_distinct_serial(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	DistinctAggState *dastate = (DistinctAggState *) PG_GETARG_POINTER(0);
	StringInfo serial_data;

	bool datumbyval = dastate->typbyval;
	char datumalign = dastate->typalign;
	int16 datumlen = dastate->typlen;
	uint32 dim = dastate->tb->members;
	uint32 meta_len = 0;

	distinct_agg_iterator i;
	DistinctAggHashEntry *entry;
	bytea *result;
	char *begin;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "gamma_count_distinct_transition error.");

	oldcontext = MemoryContextSwitchTo(aggcontext);
	serial_data = makeStringInfo();
	if (datumbyval && datumlen > 0)
	{
		enlargeStringInfo(serial_data, datumlen * dim);
	}

	distinct_agg_start_iterate(dastate->tb, &i);
	while ((entry = distinct_agg_iterate(dastate->tb, &i)) != NULL)
	{
		Datum datum = entry->key;
		Datum datum_detoast  = datum;
		//bool isnull = false;
		uint32 data_len;
		uint32 data_align_len;

		char *data_cur_ptr;
#if 0
		/* detoast datum, get the real data */
		if (!isnull && datumlen == -1 && VARATT_IS_EXTENDED(datum))
		{
			struct varlena *detoast_value =
							(struct varlena *) DatumGetPointer(datum);
			datum_detoast = PointerGetDatum(detoast_attr(detoast_value));
		}
#endif

		/* get the datum length */
		if (datumlen > 0 && datumbyval)
		{
			data_len = sizeof(Datum);
			data_align_len = sizeof(Datum);
		}
		else
		{
			data_len = att_addlength_datum(0, datumlen, datum_detoast);
			data_align_len = att_align_nominal(data_len, datumalign);
		}

		enlargeStringInfo(serial_data, data_align_len);

		data_cur_ptr = serial_data->data + serial_data->len;
		memset(data_cur_ptr, 0, data_align_len);

		/* serial data */
		Assert (!isnull);
		if (datumlen > 0)
		{
			if (datumbyval)
			{
				/* data type is fix length, treat it as Datum */
				gamma_store_att_byval(data_cur_ptr, datum_detoast, data_len);
			}
			else
			{
				memcpy(data_cur_ptr, DatumGetPointer(datum_detoast), data_len);
			}
		}
		else
		{
			Assert(!datumbyval);
			memcpy(data_cur_ptr, DatumGetPointer(datum_detoast), data_len);
		}

		serial_data->len += data_align_len;

		/* free the detoast memorys */
		if (datum != datum_detoast)
			pfree(DatumGetPointer(datum_detoast));
	}

	meta_len = sizeof(Oid) + sizeof(bool) +sizeof(uint16) + sizeof(char) +
				sizeof(uint32) + sizeof(Oid) + sizeof(uint64) + sizeof(uint64);
	result = (bytea *) palloc(VARHDRSZ + serial_data->len + meta_len);
	SET_VARSIZE(result, VARHDRSZ + serial_data->len + meta_len);
	begin = VARDATA(result);
	memcpy(begin, &dastate->typeoid, sizeof(Oid));
	begin += sizeof(Oid);
	memcpy(begin, &datumbyval, sizeof(bool));
	begin += sizeof(bool);
	memcpy(begin, &datumlen, sizeof(int16));
	begin += sizeof(int16);
	memcpy(begin, &datumalign, sizeof(char));
	begin += sizeof(char);
	memcpy(begin, &dim, sizeof(uint32));
	begin += sizeof(uint32);
	memcpy(begin, &dastate->collid, sizeof(Oid));
	begin += sizeof(Oid);
	memcpy(begin, &dastate->tb->size, sizeof(uint64));
	begin += sizeof(uint64);
	memcpy(begin, &dastate->processed, sizeof(uint64));
	begin += sizeof(uint64);

	memcpy(begin, serial_data->data, serial_data->len);

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_BYTEA_P(result);
}

Datum
gamma_count_distinct_deserial(PG_FUNCTION_ARGS)
{
	uint32 i = 0;

	MemoryContext aggcontext;
	MemoryContext oldcontext;
	bytea *data = (bytea *) PG_GETARG_POINTER(0);
	DistinctAggState *dastate;
	uint32 len = VARSIZE_ANY_EXHDR(data);
	char *pos;
	char *begin = VARDATA_ANY(data);

	Oid datumtype;
	bool datumbyval;
	char datumalign;
	int16 datumlen;
	Oid datumcollid;
	uint64 tbsize;
	uint64 processed;
	uint32 count;
	Datum value;
	uint32 offset = 0;
	uint32 meta_len = sizeof(Oid) + sizeof(bool) +sizeof(uint16) + sizeof(char) +
				sizeof(uint32) + sizeof(Oid) + sizeof(uint64) + sizeof(uint64);

	Oid eq_proc;
	TypeCacheEntry *typentry;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "gamma_count_distinct_transition error.");

	memcpy((char *)&datumtype, begin, sizeof(Oid));
	begin += sizeof(Oid);
	memcpy((char *)&datumbyval, begin, sizeof(bool));
	begin += sizeof(bool);
	memcpy((char *)&datumlen, begin, sizeof(int16));
	begin += sizeof(int16);
	memcpy((char *)&datumalign, begin, sizeof(char));
	begin += sizeof(char);
	memcpy((char *)&count, begin, sizeof(uint32));
	begin += sizeof(uint32);
	memcpy((char *)&datumcollid, begin, sizeof(Oid));
	begin += sizeof(Oid);
	memcpy((char *)&tbsize, begin, sizeof(uint64));
	begin += sizeof(uint64);
	memcpy((char *)&processed, begin, sizeof(uint64));
	begin += sizeof(uint64);
	pos = begin;

	len = len - meta_len;

	typentry = lookup_type_cache(datumtype,
								TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

	oldcontext = MemoryContextSwitchTo(aggcontext);
	dastate = (DistinctAggState *) palloc0(sizeof(DistinctAggState));
	fmgr_info_cxt(typentry->hash_proc, &dastate->hash_funcs, aggcontext);
	eq_proc = get_opcode(typentry->eq_opr);
	fmgr_info_cxt(eq_proc, &dastate->eq_funcs, aggcontext);
	dastate->tb = distinct_agg_create(aggcontext, tbsize, dastate);
	dastate->typlen = typentry->typlen;
	dastate->typbyval = typentry->typbyval;
	dastate->typalign = typentry->typalign;
	dastate->collid = datumcollid;
	dastate->processed = processed;

	gamma_distinct_agg_grow(dastate->tb, VECTOR_SIZE);

	for (i = 0; i < count; i++)
	{
		uint32 hashkey = 0;
		uint32 hkey = 0;
		bool found = false;
		DistinctAggHashEntry *daentry;

		pos = begin + offset;
		
		if (datumbyval && datumlen > 0)
		{
			value = *(Datum *) pos;
			offset += sizeof(Datum);
		}
		else
		{
			value = fetch_att(pos, datumbyval, datumlen);
			offset = att_addlength_datum(offset, datumlen, value);
			offset = att_align_nominal(offset, datumalign);
		}

		if (offset > len)
		{
			ereport(ERROR,
					(errmsg("offset: %d, data length: %d", offset, len)));
		}

		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
		hkey = gamma_hash_datum(&dastate->hash_funcs, value, datumcollid);
		hashkey ^= hkey;
		hashkey = murmurhash32(hashkey);
		daentry = gamma_distinct_agg_insert_hash(dastate->tb, value, hashkey, &found);
		Assert(!found);
		daentry->key = value;
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(dastate);
}

Datum
gamma_count_distinct_combine(PG_FUNCTION_ARGS)
{
	DistinctAggState *dastate1;
	DistinctAggState *dastate2;

	distinct_agg_iterator i;
	DistinctAggHashEntry *entry;

	MemoryContext	oldcontext;
	MemoryContext	aggcontext;

	Oid collid;
	dastate1 = PG_ARGISNULL(0) ? NULL : (DistinctAggState *) PG_GETARG_POINTER(0);
	dastate2 = PG_ARGISNULL(1) ? NULL : (DistinctAggState *) PG_GETARG_POINTER(1);

	if (dastate2 == NULL && dastate1 == NULL)
	{
		PG_RETURN_NULL();
	}
	else if (dastate1 == NULL)
	{
		PG_RETURN_POINTER(dastate2);
	}
	else if (dastate2 == NULL)
	{
		PG_RETURN_POINTER(dastate1);
	}

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "gamma_count_distinct_transition error.");

	collid = dastate2->collid;

	oldcontext = MemoryContextSwitchTo(aggcontext);

	/* set processed tuple count */
	dastate1->processed += dastate2->processed;

	gamma_distinct_agg_grow(dastate1->tb, VECTOR_SIZE);
	distinct_agg_start_iterate(dastate2->tb, &i);
	while ((entry = distinct_agg_iterate(dastate2->tb, &i)) != NULL)
	{
		uint32 hashkey = 0;
		uint32 hkey = 0;
		bool found = false;
		DistinctAggHashEntry *daentry;
		Datum value = entry->key;

		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
		hkey = gamma_hash_datum(&dastate1->hash_funcs, value, collid);
		hashkey ^= hkey;
		hashkey = murmurhash32(hashkey);
		daentry = gamma_distinct_agg_insert_hash(dastate1->tb, value, hashkey, &found);
		Assert(!found);
		daentry->key = value;
	}

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_POINTER(dastate1);
}

Datum sort_array[10000];

Datum
gamma_count_distinct_final(PG_FUNCTION_ARGS)
{
	DistinctAggState *dastate;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	dastate = (DistinctAggState *) PG_GETARG_POINTER(0);
#if 0
	{
		int i,j,k;
		distinct_agg_iterator it;
		DistinctAggHashEntry *entry;
		distinct_agg_start_iterate(dastate->tb, &it);
		while ((entry = distinct_agg_iterate(dastate->tb, &it)) != NULL)
		{
			sort_array[i++] = entry->key;
			elog(WARNING, "%d:%lu", i, entry->key);
		}

		for (j = 0; j < i; j++)
		{
			for (k = 0; k < j; k++)
			{
				if (sort_array[k] < sort_array[j])
				{
					Datum temp = sort_array[k];
					sort_array[k] = sort_array[j];
					sort_array[j] = temp;
				}

			}
		}

		for (j = 0; j < i - 1; j++)
		{
			if (sort_array[j] == sort_array[j+1])
				elog(ERROR, "WRONG");
		}
	}
#endif
	PG_RETURN_INT64(dastate->tb->members);
}
