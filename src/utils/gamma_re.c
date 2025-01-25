#ifdef _AVX2_
#include <stdint.h>  
#include <stddef.h>  
#include <stdbool.h>  
#include <immintrin.h>  
#include <string.h>  
#include <stdio.h>  

#include "utils/gamma_re.h"

#define AVX2_SIZE sizeof(__m256i)  
#define GAMMA_RE_SUCCESS 0  
#define GAMMA_RE_INVALID_ARGUMENT -1  

int32
cstring_init_pattern(StringContext *context, const char *pattern, Size len)
{  
	int32 ret = GAMMA_RE_SUCCESS;  
	if (pattern == NULL || len == 0)
	{  
		ret = GAMMA_RE_INVALID_ARGUMENT;  
		elog(WARNING, "invalid argument. pattern is null.");  
	}
	else
	{  
		context->pattern = pattern;  
		context->pattern_end = pattern + len;  
		context->pattern_len = len;  
		context->first = *pattern;  
		context->vfirst = _mm256_set1_epi8(context->first);  
		if (len >= 2)
		{  
			context->last = *(context->pattern_end - 1);  
			context->vlast = _mm256_set1_epi8(context->last);  
		}  
	}  
	return ret;  
}

static bool
memequal_opt_sse(const char *p1, const char *p2, Size len)
{  
	Size cnt;
	Size i;

	if (len == 0)
		return true;  

	if (len % 16 != 0)
	{  
		if (!memcmp(p1, p2, len % 16))
		{  
			p1 += len % 16;  
			p2 += len % 16;  
			len -= len % 16;  
		}
		else
		{  
			return false;  
		}  
	}  
	
	cnt = len / 16;  
	for (i = 0; i < cnt; ++i) {  
		if (_mm_movemask_epi8(_mm_cmpeq_epi8(  
						_mm_loadu_si128((__m128i *)(p1 + i * 16)),  
						_mm_loadu_si128((__m128i *)(p2 + i * 16)))) != 0xFFFF) {  
			return false;  
		}  
	}  
	return true;  
}  

int32
cstring_is_substring(const StringContext *context, const char *text,
				uint32 text_len, bool *res)
{  
	int32 ret = GAMMA_RE_SUCCESS;  
	const char *text_cur = text;  
	const char *text_end = text + text_len;  
	*res = false;  
	if (context->pattern == NULL || context->pattern_len == 0)
	{  
		ret = GAMMA_RE_INVALID_ARGUMENT;  
		elog(WARNING, "invalid argument. pattern_ is null.");  
	}
	else if (text == text_end)
	{
		/* do nothing */	
	}
	else if (context->pattern_len == 1)
	{  
		const char *avx_end = text + ((text_end - text) & ~(AVX2_SIZE - 1));  
		for (; text_cur < avx_end; text_cur += AVX2_SIZE)
		{  
			__m256i first_block = _mm256_loadu_si256((__m256i *)text_cur);  
			__m256i first_cmp = _mm256_cmpeq_epi8(first_block, context->vfirst);  
			uint32 mask = _mm256_movemask_epi8(first_cmp);  
			if (mask != 0)
			{  
				*res = true;  
				break;  
			}  
		}  
	} 
	else
	{
		const char *avx_end = text +
			((text_end - (text + context->pattern_len - 1)) & ~(AVX2_SIZE - 1));  

		for (; !*res && text_cur < avx_end; text_cur += AVX2_SIZE)
		{  
			const char *last_cur = text_cur + context->pattern_len - 1;  
			__m256i first_block = _mm256_loadu_si256((__m256i *)text_cur);  
			__m256i last_block = _mm256_loadu_si256((__m256i *)last_cur);  
			__m256i first_cmp = _mm256_cmpeq_epi8(first_block, context->vfirst);  
			__m256i last_cmp = _mm256_cmpeq_epi8(last_block, context->vlast);  
			uint32 mask = _mm256_movemask_epi8(
										_mm256_and_si256(first_cmp, last_cmp));  

			while (mask != 0)
			{  
				int32 offset = __builtin_ctz(mask);  
				if (context->pattern_len == 2 ||  
					memequal_opt_sse(text_cur + offset + 1,
										context->pattern + 1,
										context->pattern_len - 2))
				{  
					*res = true;  
					break;  
				}  

				mask &= (mask - 1);  
			}  
		}  
	}  

	if (!*res && text_end - text_cur >= context->pattern_len)
	{
		int32 tl = text_end - text_cur;
		int32 ml = tl - context->pattern_len;

		__m256i first_block = _mm256_loadu_si256((__m256i *)text_cur);  
		__m256i first_cmp = _mm256_cmpeq_epi8(first_block, context->vfirst);  
		uint32 mask = _mm256_movemask_epi8(first_cmp);  
		Assert(tl < AVX2_SIZE);

		while (mask != 0 && ml >= 0)
		{  
			int32 offset = __builtin_ctz(mask);  
			if (context->pattern_len == 2 ||  
					memequal_opt_sse(text_cur + offset + 1,
						context->pattern + 1,
						context->pattern_len - 2))
			{  
				*res = true;  
				break;  
			}  

			mask &= (mask - 1);
			ml--;
		}  
	}  

	return ret;  
}  

int32
cstring_equal(const StringContext *context, const char *text,
				const char *text_end, bool *res)
{  
	int32 ret = GAMMA_RE_SUCCESS;  
	if (context->pattern == NULL || context->pattern_len == 0)
	{  
		ret = GAMMA_RE_INVALID_ARGUMENT;  
		elog(WARNING, "invalid argument. pattern_ is null.");  
	}
	else if (context->pattern_len != text_end - text)
	{  
		*res = false;  
	}
	else
	{  
		*res = memequal_opt_sse(text, context->pattern, context->pattern_len);  
	}
	
	return ret;  
}

#if 0
int32 main() {  
	StringContext context;  
	const char *pattern = "hello";  
	int32 init_result = init_string_context(&context, pattern, strlen(pattern));  
	if (init_result != GAMMA_RE_SUCCESS) {  
		fprint32f(stderr, "Failed to initialize string context\n");  
		return 1;  
	}  

	const char *text = "hello world";  
	const char *text_end = text + strlen(text);  
	bool res;  
	int32 is_sub_result = is_substring(&context, text, text_end, &res);  
	if (is_sub_result == GAMMA_RE_SUCCESS) {  
		print32f("Is substring: %s\n", res ? "yes" : "no");  
	} else {  
		fprint32f(stderr, "Failed to check substring\n");  
	}  

	int32 equal_result = equal(&context, text, text_end, &res);  
	if (equal_result == GAMMA_RE_SUCCESS) {  
		print32f("Is equal: %s\n", res ? "yes" : "no");  
	} else {  
		fprint32f(stderr, "Failed to check equal\n");  
	}
}
#endif
#endif
