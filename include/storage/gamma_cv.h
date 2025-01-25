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

#ifndef GAMMA_CV_H
#define GAMMA_CV_H

#include "lib/stringinfo.h"
#include "utils/relcache.h"

/* 60 * 1024 */
#define GAMMA_COLUMN_VECTOR_SIZE (61440)

#define GAMMA_CV_FLAGS_REF			(1)
#define GAMMA_CV_FLAGS_NON_NULL		(1 << 1)

typedef struct ColumnVector {
	Oid rgid;
	int32 attno;
	Oid elemtype;
	int16 elemlen;
	bool elembyval;
	char elemalign;

	int	 dim;
	int flags;

	/* reference from RowGroup */
	bool *delbitmap;
	
	/* cache or ref */
	bool *isnull;//[GAMMA_COLUMN_VECTOR_SIZE];
	Datum *values;//[GAMMA_COLUMN_VECTOR_SIZE];
} ColumnVector;

#define CVIsRef(cv) (cv->flags & GAMMA_CV_FLAGS_REF)
#define CVIsNonNull(cv) (cv->flags & GAMMA_CV_FLAGS_NON_NULL)

#define CVSetRef(cv) (cv->flags |= GAMMA_CV_FLAGS_REF)
#define CVSetNonNull(cv) (cv->flags |= GAMMA_CV_FLAGS_NON_NULL)

extern ColumnVector* gamma_cv_build(Form_pg_attribute attr, int dim);
extern void gamma_cv_serialize(ColumnVector *cv, StringInfo serial_data);
extern void gamma_cv_fill_data(ColumnVector *cv, char *data, uint32 length,
					bool *nulls, uint32 count);

#define gamma_store_att_byval(T,newdatum,attlen) \
	do { \
		switch (attlen) \
		{ \
			case sizeof(char): \
			case sizeof(int16): \
			case sizeof(int32): \
			case sizeof(Datum): \
				*(Datum *) (T) = (newdatum); \
				break; \
			default: \
				elog(ERROR, "unsupported byval length: %d", \
							 (int) (attlen)); \
				break; \
		} \
	} while (0)

#endif /*GAMMA_CV_H*/
