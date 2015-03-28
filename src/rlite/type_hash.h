#ifndef _RL_TYPE_HASH_H
#define _RL_TYPE_HASH_H

#include "page_btree.h"

#define RL_TYPE_HASH 'H'

struct rlite;

typedef rl_btree_iterator rl_hash_iterator;

int rl_hash_iterator_next(rl_hash_iterator *iterator, unsigned char **field, long *fieldlen, unsigned char **member, long *memberlen);
int rl_hash_iterator_destroy(rl_hash_iterator *iterator);

int rl_hset(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char *data, long datalen, long *added, int update);
int rl_hget(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char **data, long *datalen);
int rl_hexists(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen);
int rl_hdel(struct rlite *db, const unsigned char *key, long keylen, long fieldsc, unsigned char **fields, long *fieldslen, long *delcount);
int rl_hgetall(struct rlite *db, rl_hash_iterator **iterator, const unsigned char *key, long keylen);
int rl_hlen(struct rlite *db, const unsigned char *key, long keylen, long *len);
int rl_hmget(struct rlite *db, const unsigned char *key, long keylen, int fieldc, unsigned char **fields, long *fieldslen, unsigned char ***_data, long **_datalen);
int rl_hmset(struct rlite *db, const unsigned char *key, long keylen, int fieldc, unsigned char **fields, long *fieldslen, unsigned char **datas, long *dataslen);
int rl_hincrby(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, long increment, long *newvalue);
int rl_hincrbyfloat(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, double increment, double *newvalue);

int rl_hash_pages(struct rlite *db, long page, short *pages);
int rl_hash_delete(struct rlite *db, long value_page);

#endif
