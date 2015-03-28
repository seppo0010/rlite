#ifndef _RL_TYPE_LIST_H
#define _RL_TYPE_LIST_H

#include "page_list.h"

#define RL_TYPE_LIST 'L'

struct rlite;

int rl_push(struct rlite *db, const unsigned char *key, long keylen, int create, int left, int valuec, unsigned char **values, long *valueslen, long *size);
int rl_llen(struct rlite *db, const unsigned char *key, long keylen, long *len);
int rl_pop(struct rlite *db, const unsigned char *key, long keylen, unsigned char **value, long *valuelen, int left);
int rl_lindex(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char **value, long *valuelen);
int rl_linsert(struct rlite *db, const unsigned char *key, long keylen, int after, unsigned char *pivot, long pivotlen, unsigned char *value, long valuelen, long *size);
int rl_lrange(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, long *size, unsigned char ***values, long **valueslen);
int rl_lrem(struct rlite *db, const unsigned char *key, long keylen, int direction, long maxcount, unsigned char *value, long valuelen, long *count);
int rl_lset(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char *value, long valuelen);
int rl_ltrim(struct rlite *db, const unsigned char *key, long keylen, long start, long stop);

int rl_llist_pages(struct rlite *db, long page, short *pages);
int rl_llist_delete(struct rlite *db, long value_page);

#endif
