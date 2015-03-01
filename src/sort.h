#ifndef _RL_SORT_H
#define _RL_SORT_H
#include "rlite.h"

typedef struct rliteSortObject {
	unsigned char *obj;
	long objlen;
	union {
		double score;
		struct {
			unsigned char *obj;
			long objlen;
		} cmpobj;
	} u;
} rliteSortObject;

int rl_sort(struct rlite *db, unsigned char *key, long keylen, unsigned char *sortby, long sortbylen, int dontsort, int inLuaScript, int alpha, int desc, long limit_start, long limit_count, int getc, unsigned char **getv, long *getvlen, unsigned char *storekey, long storekeylen, long *retobjc, unsigned char ***retobjv, long **retobjvlen);
#endif
