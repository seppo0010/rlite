#ifndef _RL_TYPE_SET_H
#define _RL_TYPE_SET_H

#include "page_btree.h"

#define RL_TYPE_SET 'S'

struct rlite;

typedef rl_btree_iterator rl_set_iterator;

// int rl_set_iterator_next(rl_set_iterator *iterator, unsigned char **member, long *memberlen);
// int rl_set_iterator_destroy(rl_set_iterator *iterator);

int rl_sadd(struct rlite *db, const unsigned char *key, long keylen, int memberc, unsigned char **members, long *memberslen, long *added);
int rl_sismember(struct rlite *db, const unsigned char *key, long keylen, unsigned char *data, long datalen);
int rl_scard(struct rlite *db, const unsigned char *key, long keylen, long *card);

int rl_set_pages(struct rlite *db, long page, short *pages);
int rl_set_delete(struct rlite *db, const unsigned char *key, long keylen);

#endif
