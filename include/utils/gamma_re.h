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

#ifndef _GAMMA_RE_
#define _GAMMA_RE_

#ifdef _AVX2_
#include "postgres.h"

#include <immintrin.h>  

typedef struct StringContext
{  
	const char *pattern;  
	const char *pattern_end;  
	Size pattern_len;  
	uint8_t first;  
	uint8_t last;  
	__m256i vfirst;  
	__m256i vlast;  
} StringContext;  

extern int32 cstring_init_pattern(StringContext *context,
									const char *pattern, Size len);  
extern int32 cstring_is_substring(const StringContext *context, const char *text,
									uint32 text_len, bool *res);
extern int32 cstring_equal(const StringContext *context, const char *text,
									const char *text_end, bool *res);
#endif
#endif /* _GAMMA_RE_ */
