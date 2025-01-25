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

#include "catalog/namespace.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/hsearch.h"

#include "utils/utils.h"
#include "utils/vdatum/vvarlena.h"

typedef struct
{
	List	   *vars;
	int			sublevels_up;
} pull_vars_context;

/* Map between the vectorized types and non-vectorized types */
typedef struct mapVecNormal
{
	Oid min;
	Oid max;
	Oid *arr;
} mapVecNormal;

static bool g_n2v_init = false;
static bool g_v2n_init = false;
static mapVecNormal hashMapN2V;
static mapVecNormal hashMapV2N;

#define BUILTIN_TYPE_NUM 13
#define TYPE_HASH_TABLE_SIZE 64
const char *typenames[] = { "any", "int2", "int4", "int8", "float4", "float8",
							"bool", "text", "date", "bpchar", "timestamp",
							"numeric", "tid"};
const char *vdatumnames[] = { "vany", "vint2", "vint4", "vint8", "vfloat4",
							"vfloat8", "vbool", "vtext", "vdate", "vbpchar",
							"vtimestamp", "vnumeric", "vtid"};

static bool gamma_pull_vars_walker(Node *node, pull_vars_context *context);

static inline uint32
gamma_vec_type_hash(const void *typid, Size size)
{
	return murmurhash32(*(uint32*)typid);
}

bool
is_vec_type(Oid vtype)
{
	Assert(g_n2v_init);
	if (vtype < hashMapV2N.min || vtype > hashMapV2N.max)
		return false;
	return (hashMapV2N.arr[vtype - hashMapV2N.min] != InvalidOid);
}

/*
 * map non-vectorized type to vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid
en_vec_type(Oid ntype)
{
	/* construct the hash table */
	if(unlikely(!g_n2v_init))
	{
		Oid min,max;
		int len;
		int i;
		Oid norOid[BUILTIN_TYPE_NUM];
		Oid vecOid[BUILTIN_TYPE_NUM];
		MemoryContext oldcontext;

		/* insert supported built-in type and vdatums */
		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vecOid[i] = TypenameGetTypid(vdatumnames[i]);
			norOid[i] = TypenameGetTypid(typenames[i]);
			
			if (i == 0)
			{
				min = max = norOid[0];
			}
			else
			{
				if (min > norOid[i])
					min = norOid[i];

				if (max < norOid[i])
					max = norOid[i];
			}
		}

		len = max - min + 1;
		hashMapN2V.min = min;
		hashMapN2V.max = max;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		hashMapN2V.arr = (Oid *) palloc0(sizeof(Oid) * len);
		MemoryContextSwitchTo(oldcontext);

		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			hashMapN2V.arr[norOid[i] - min] = vecOid[i];
		}	

		g_n2v_init = true;
	}

	if (ntype < hashMapN2V.min || ntype > hashMapN2V.max)
		return InvalidOid;

	/* find the vectorized type in hash table */
	return hashMapN2V.arr[ntype - hashMapN2V.min];
}



/*
 * map vectorized type to non-vectorized type.
 * To scan the PG_TYPE is inefficient, so we create a hashtable to map
 * the vectorized type and non-vectorized types.
 */
Oid
de_vec_type(Oid vtype)
{
	if(unlikely(!g_v2n_init))
	{
		Oid min,max;
		int len;
		int i;
		Oid norOid[BUILTIN_TYPE_NUM];
		Oid vecOid[BUILTIN_TYPE_NUM];
		MemoryContext oldcontext;

		/* insert supported built-in type and vdatums */
		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			vecOid[i] = TypenameGetTypid(vdatumnames[i]);
			norOid[i] = TypenameGetTypid(typenames[i]);
			
			if (i == 0)
			{
				min = max = vecOid[0];
			}
			else
			{
				if (min > vecOid[i])
					min = vecOid[i];

				if (max < vecOid[i])
					max = vecOid[i];
			}
		}

		len = max - min + 1;
		hashMapV2N.min = min;
		hashMapV2N.max = max;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		hashMapV2N.arr = (Oid *) palloc0(sizeof(Oid) * len);
		MemoryContextSwitchTo(oldcontext);

		for (i = 0; i < BUILTIN_TYPE_NUM; i++)
		{
			hashMapV2N.arr[vecOid[i] - min] = norOid[i];
		}	

		g_v2n_init = true;
	}

	if (vtype < hashMapV2N.min || vtype > hashMapV2N.max)
		return InvalidOid;

	return hashMapV2N.arr[vtype - hashMapV2N.min];
}

Oid
en_vec_tupdesc_attr(TupleDesc tupdesc, int i)
{
	Form_pg_attribute att = &(tupdesc->attrs[i]);
	return en_vec_type(att->atttypid);
}

TupleDesc
de_vec_tupledesc(TupleDesc tupdesc)
{
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = &(tupdesc->attrs[i]);
		Oid typid = de_vec_type(attr->atttypid);
		if (typid != InvalidOid)
			attr->atttypid = typid;
	}

	return tupdesc;
}

TupleDesc
en_vec_tupledesc(TupleDesc tupdesc)
{
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = &(tupdesc->attrs[i]);
		Oid typid = en_vec_type(attr->atttypid);
		if (typid != InvalidOid)
			attr->atttypid = typid;
	}

	return tupdesc;
}

List *
gamma_pull_vars_of_level(Node *node, int levelsup)
{
	pull_vars_context context;

	context.vars = NIL;
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									gamma_pull_vars_walker,
									(void *) &context,
									0);

	return context.vars;
}

static bool
gamma_pull_vars_walker(Node *node, pull_vars_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *ri = (RestrictInfo *) node;
		return expression_tree_walker((Node *)ri->clause,
									gamma_pull_vars_walker,
									(void *) context);

	}

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, var);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		if (phv->phlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, phv);
		/* we don't want to look into the contained expression */
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node, gamma_pull_vars_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, gamma_pull_vars_walker,
								  (void *) context);
}

//DJB2
uint32
gamma_hash_bytes(const char *k, int32 length)
{
	unsigned int hash = 5381;  
	int c;  

	for (int i = 0; i < length; i++) {  
		c = k[i];  
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */  
	}  

	return hash;  
}  

#if 0
// FNV-1a
#define FNV_PRIME 1099511628211  
#define FNV_OFFSET_BASIS 0x811c9dc5  
uint32
gamma_hash_bytes(const char *k, int length) {  
	uint32_t hash = FNV_OFFSET_BASIS;  

	for (int i = 0; i < length; i++)
	{  
		hash ^= (uint8_t)k[i];
		hash *= FNV_PRIME; 
	}  

	return hash;  
}
#endif
#define FNV_OFFSET_BASIS 2166136261U  
#define FNV_PRIME 16777619U  
  
//FNV-1a-int 
uint32
fnv1a_hash_int32(int k)
{
	uint32 hash = FNV_OFFSET_BASIS;
	uint8_t *bytes = (uint8_t*)&k;
	for (int i = 0; i < sizeof(int); i++)
	{
		hash ^= bytes[i];
		hash *= FNV_PRIME;
	}  

	return hash;
}  


