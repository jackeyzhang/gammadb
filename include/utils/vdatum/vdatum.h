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

#ifndef __VDATUM_H___
#define __VDATUM_H___

#include "postgres.h"
#include "fmgr.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

typedef int16 int2;
typedef int32 int4;

#define VECTOR_SIZE 1024
typedef struct vdatum {
	Oid	 elemtype;
	int	 dim;

	bool ref;

	/* not ref */
	bool	isnull[VECTOR_SIZE];
	Datum   values[VECTOR_SIZE];

	/* be ref */
	bool *ref_isnull;
	Datum *ref_values;

	/* for filter */
	bool	*skipref;

	/* use in aggregation */
	short	*indexarr;
}vdatum;

#define VDATUM_DATUM(d, i) ((d)->ref ? (d)->ref_values[i] : (d)->values[i])
#define VDATUM_ISNULL(d, i) \
	((d)->ref ? \
				((d)->ref_isnull == NULL? false : (d)->ref_isnull[i]) : \
				(d)->isnull[i])

#define VDATUM_SET_DATUM(d, i, v) \
	{ \
		if ((d)->ref) \
		{ \
			(d)->ref_values[i] = (v); \
		} \
		else \
		{ \
			(d)->values[i] = (v); \
		} \
	}

#define VDATUM_SET_ISNULL(d, i, b) \
	{ \
		if ((d)->ref) \
		{ \
			(d)->ref_isnull[i] = (b); \
		} \
		else \
		{ \
			(d)->isnull[i] = (b); \
		} \
	}

#define VDATUM_IS_REF(d) ((d)->ref)
#define VDATUM_REF_ISNULL(d) ((d)->ref_isnull)
#define VDATUM_REF_VALUES(d) ((d)->ref_values)

#define VDATUM_ARR_ISNULL(d) ((d)->isnull)
#define VDATUM_ARR_VALUES(d) ((d)->values)

#define CANARYSIZE  sizeof(char)
#define VDATUMHEADERSZ (sizeof(vdatum))
#define VDATUMSZ(dim) (sizeof(Datum) * dim)
#define ISNULLSZ(dim) (sizeof(bool) * dim)
#define VDATUMSIZE(dim) (VDATUMHEADERSZ + VDATUMSZ(dim) + CANARYSIZE + ISNULLSZ(dim))
#define CANARYOFFSET(vdatum) ((char*)((unsigned char*)vdatum + VDATUMHEADERSZ + VDATUMSZ(dim)))
#define ISNULLOFFSET(vdatum) ((bool*)((unsigned char*)vdatum + VDATUMHEADERSZ + VDATUMSZ(vdatum->dim) + CANARYSIZE))
#define VDATUM_STURCTURE(type) typedef struct vdatum v##type;

#define FUNCTION_BUILD_HEADER(type) \
v##type* buildv##type(int dim, bool *skip);

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_HEADER(type1, type2, opsym, opstr) \
extern Datum v##type1##v##type2##opstr(PG_FUNCTION_ARGS);

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
extern Datum v##type##const_type##opstr(PG_FUNCTION_ARGS);
#define __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
extern Datum \
const_type##v##type##opstr(PG_FUNCTION_ARGS);
/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_HEADER(type1, type2, cmpsym, cmpstr) \
extern Datum v##type1##v##type2##cmpstr(PG_FUNCTION_ARGS);

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
extern Datum v##type##const_type##cmpstr(PG_FUNCTION_ARGS);

#define _FUNCTION_OP_HEADER(type1, type2) \
	__FUNCTION_OP_HEADER(type1, type2, +, pl)  \
	__FUNCTION_OP_HEADER(type1, type2, -, mi)  \
	__FUNCTION_OP_HEADER(type1, type2, *, mul) \
	__FUNCTION_OP_HEADER(type1, type2, /, div)

#define _FUNCTION_OP_CONST_HEADER(type, const_type, CONST_ARG_MACRO) \
	__FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, +, pl)  \
	__FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, -, mi)  \
	__FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, *, mul) \
	__FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, /, div) \
	__FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, +, pl)  \
	__FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, -, mi)  \
	__FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, *, mul) \
	__FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, /, div)

#define _FUNCTION_CMP_HEADER(type1, type2) \
	__FUNCTION_CMP_HEADER(type1, type2, ==, eq) \
	__FUNCTION_CMP_HEADER(type1, type2, !=, ne) \
	__FUNCTION_CMP_HEADER(type1, type2, >, gt) \
	__FUNCTION_CMP_HEADER(type1, type2, >=, ge) \
	__FUNCTION_CMP_HEADER(type1, type2, <, lt) \
	__FUNCTION_CMP_HEADER(type1, type2, <=, le)

#define _FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO) \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, ==, eq)  \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, !=, ne)  \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  >, gt) \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, >=, ge) \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  <, lt) \
	__FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, <=, le) 

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define FUNCTION_IN_HEADER(type, typeoid) \
extern Datum v##type##in(PG_FUNCTION_ARGS);

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define FUNCTION_OUT_HEADER(type, typeoid) \
extern Datum v##type##out(PG_FUNCTION_ARGS);

#define FUNCTION_OP_HEADER(type) \
	_FUNCTION_OP_HEADER(type, int2) \
	_FUNCTION_OP_HEADER(type, int4) \
	_FUNCTION_OP_HEADER(type, int8) \
	_FUNCTION_OP_HEADER(type, float4) \
	_FUNCTION_OP_HEADER(type, float8)

#define FUNCTION_OP_CONST_HEADER(type) \
	_FUNCTION_OP_CONST_HEADER(type, int2, PG_GETARG_INT16) \
	_FUNCTION_OP_CONST_HEADER(type, int4, PG_GETARG_INT32) \
	_FUNCTION_OP_CONST_HEADER(type, int8, PG_GETARG_INT64) \
	_FUNCTION_OP_CONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
	_FUNCTION_OP_CONST_HEADER(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_CMP_HEADER(type1) \
	_FUNCTION_CMP_HEADER(type1, int2) \
	_FUNCTION_CMP_HEADER(type1, int4) \
	_FUNCTION_CMP_HEADER(type1, int8) \
	_FUNCTION_CMP_HEADER(type1, float4) \
	_FUNCTION_CMP_HEADER(type1, float8)

#define FUNCTION_CMP_RCONST_HEADER(type) \
	_FUNCTION_CMP_RCONST_HEADER(type, int2, PG_GETARG_INT16) \
	_FUNCTION_CMP_RCONST_HEADER(type, int4, PG_GETARG_INT32) \
	_FUNCTION_CMP_RCONST_HEADER(type, int8, PG_GETARG_INT64) \
	_FUNCTION_CMP_RCONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
	_FUNCTION_CMP_RCONST_HEADER(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_OP_ALL_HEADER(type) \
	FUNCTION_OP_HEADER(type) \
	FUNCTION_OP_CONST_HEADER(type) \
	FUNCTION_CMP_HEADER(type) \
	FUNCTION_CMP_RCONST_HEADER(type) \

#define TYPE_HEADER(type,oid) \
	VDATUM_STURCTURE(type) \
	FUNCTION_BUILD_HEADER(type) \
	FUNCTION_OP_ALL_HEADER(type) \
	FUNCTION_IN_HEADER(type, typeoid) \
	FUNCTION_OUT_HEADER(type, typeoid) \

TYPE_HEADER(int2, INT2OID)
TYPE_HEADER(int4, INT4OID)
TYPE_HEADER(int8, INT8OID)
TYPE_HEADER(float4, FLOAT4OID)
TYPE_HEADER(float8, FLOAT8OID)
//TYPE_HEADER(bool, BOOLOID)
TYPE_HEADER(text, TEXTOID)
TYPE_HEADER(date, DATEOID)
TYPE_HEADER(bpchar, BPCHAROID)

extern vdatum* buildvdatum(Oid elemtype, int dim, bool *skip);
extern void destroyvdatum(vdatum** vt);
extern vdatum *copyvdatum(vdatum *src, bool *skip);
extern void clearvdatum(vdatum *vt);

typedef struct vdatum vbool;
vbool* buildvbool(int dim, bool *skip);
#endif
