#ifndef _RL_UTIL_H
#define _RL_UTIL_H

#include <string.h>
#include <limits.h>
#include "../deps/utilfromredis.h"

#ifdef DEBUG
int expect_fail();
extern int test_mode;
void *rl_malloc(size_t size);
void rl_free(void *ptr);
#else
#define rl_malloc malloc
#define rl_free free
#endif

#define RL_MALLOC(obj, size)\
	obj = rl_malloc(size);\
	if (!obj) {\
		retval = RL_OUT_OF_MEMORY;\
		goto cleanup;\
	}

#define RL_REALLOC(ptr, size)\
	tmp = realloc(ptr, size);\
	if (!tmp) {\
		retval = RL_OUT_OF_MEMORY;\
		goto cleanup;\
	}\
	ptr = tmp;

#define RL_CALL(func, expected, ...)\
	retval = func(__VA_ARGS__);\
	if (expected != retval) goto cleanup;

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

int get_4bytes(const unsigned char *p);
void put_4bytes(unsigned char *p, long v);
unsigned long long get_8bytes(const unsigned char *p);
void put_8bytes(unsigned char *p, unsigned long long v);
int long_cmp(void *v1, void *v2);
int sha1_cmp(void *v1, void *v2);
int double_cmp(void *v1, void *v2);
#ifdef DEBUG
int long_formatter(void *v2, char **formatted, int *size);
int sha1_formatter(void *v2, char **formatted, int *size);
int double_formatter(void *v2, char **formatted, int *size);
#endif
double get_double(const unsigned char *p);
void put_double(unsigned char *p, double v);
int sha1(const unsigned char *data, long datalen, unsigned char digest[20]);
unsigned long long mstime();

#endif
