#ifndef _RL_TYPE_ZSET_H
#define _RL_TYPE_ZSET_H

#include "page_skiplist.h"

#define RL_TYPE_ZSET 'Z'

struct rlite;

typedef struct {
	double min;
	int minex;
	double max;
	int maxex;
} rl_zrangespec;

typedef struct rl_skiplist_iterator rl_zset_iterator;

int rl_zset_iterator_next(rl_zset_iterator *iterator, double *score, unsigned char **data, long *datalen);
int rl_zset_iterator_destroy(rl_zset_iterator *iterator);

int rl_zadd(struct rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen);
int rl_zcard(rlite *db, unsigned char *key, long keylen, long *card);
int rl_zcount(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long *count);
int rl_zrange(rlite *db, unsigned char *key, long keylen, long start, long end, rl_zset_iterator **iterator);
int rl_zrank(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, long *rank);
int rl_zrem(rlite *db, unsigned char *key, long keylen, long members_size, unsigned char **members, long *members_len, long *changed);
int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, double *score);

#endif
