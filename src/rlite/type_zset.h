#ifndef _RL_TYPE_ZSET_H
#define _RL_TYPE_ZSET_H

#include "page_skiplist.h"

#define RL_TYPE_ZSET 'Z'

#define RL_ZSET_AGGREGATE_SUM 0
#define RL_ZSET_AGGREGATE_MIN 1
#define RL_ZSET_AGGREGATE_MAX 2

struct rlite;

typedef struct {
	double min;
	int minex;
	double max;
	int maxex;
} rl_zrangespec;

typedef struct rl_skiplist_iterator rl_zset_iterator;

int rl_zset_iterator_next(rl_zset_iterator *iterator, long *page, double *score, unsigned char **data, long *datalen);
int rl_zset_iterator_destroy(rl_zset_iterator *iterator);

int rl_zadd(struct rlite *db, const unsigned char *key, long keylen, double score, unsigned char *data, long datalen);
int rl_zcard(struct rlite *db, const unsigned char *key, long keylen, long *card);
int rl_zcount(struct rlite *db, const unsigned char *key, long keylen, rl_zrangespec *range, long *count);
int rl_zincrby(struct rlite *db, const unsigned char *key, long keylen, double score, unsigned char *data, long datalen, double *newscore);
int rl_zinterstore(struct rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *weights, int aggregate);
int rl_zlexcount(struct rlite *db, const unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long *lexcount);
int rl_zrange(struct rlite *db, const unsigned char *key, long keylen, long start, long end, rl_zset_iterator **iterator);
int rl_zrangebylex(struct rlite *db, const unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long offset, long count, rl_zset_iterator **iterator);
int rl_zrangebyscore(struct rlite *db, const unsigned char *key, long keylen, rl_zrangespec *range, long offset, long count, rl_zset_iterator **iterator);
int rl_zrank(struct rlite *db, const unsigned char *key, long keylen, unsigned char *data, long datalen, long *rank);
int rl_zrevrange(struct rlite *db, const unsigned char *key, long keylen, long start, long end, rl_zset_iterator **iterator);
int rl_zrevrangebylex(struct rlite *db, const unsigned char *key, long keylen, unsigned char *max, long maxlen, unsigned char *min, long minlen, long offset, long count, rl_zset_iterator **iterator);
int rl_zrevrangebyscore(struct rlite *db, const unsigned char *key, long keylen, rl_zrangespec *range, long offset, long count, rl_zset_iterator **iterator);
int rl_zrevrank(struct rlite *db, const unsigned char *key, long keylen, unsigned char *data, long datalen, long *revrank);
int rl_zrem(struct rlite *db, const unsigned char *key, long keylen, long members_size, unsigned char **members, long *members_len, long *changed);
int rl_zremrangebylex(struct rlite *db, const unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long *changed);
int rl_zremrangebyrank(struct rlite *db, const unsigned char *key, long keylen, long start, long end, long *changed);
int rl_zremrangebyscore(struct rlite *db, const unsigned char *key, long keylen, rl_zrangespec *range, long *changed);
int rl_zscore(struct rlite *db, const unsigned char *key, long keylen, unsigned char *data, long datalen, double *score);
int rl_zunionstore(struct rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *weights, int aggregate);

int rl_zset_pages(struct rlite *db, long page, short *pages);
int rl_zset_delete(struct rlite *db, long value_page);

#endif
