#ifndef _RL_TYPE_HASH_H
#define _RL_TYPE_HASH_H

#define RL_TYPE_HASH 'H'

struct rlite;

typedef struct rl_btree_iterator rl_hash_iterator;

int rl_hash_iterator_next(rl_hash_iterator *iterator, unsigned char *key, long keylen, unsigned char **data, long *datalen);
int rl_hash_iterator_destroy(rl_hash_iterator *iterator);

int rl_hset(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char *data, long datalen, long *added);
int rl_hget(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char **data, long *datalen);
int rl_hexists(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen);

int rl_hash_pages(struct rlite *db, long page, short *pages);
int rl_hash_delete(struct rlite *db, const unsigned char *key, long keylen);

#endif
