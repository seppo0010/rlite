#ifndef _RL_TYPE_LIST_H
#define _RL_TYPE_LIST_H

#include "page_list.h"

#define RL_TYPE_LIST 'L'

struct rlite;

typedef rl_list_iterator rl_llist_iterator;

int rl_lpush(struct rlite *db, const unsigned char *key, long keylen, int valuec, unsigned char **values, long *valueslen, long *size);
int rl_llen(struct rlite *db, const unsigned char *key, long keylen, long *len);

int rl_llist_pages(struct rlite *db, long page, short *pages);
int rl_llist_delete(struct rlite *db, const unsigned char *key, long keylen);

#endif
