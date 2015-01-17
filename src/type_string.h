#ifndef _RL_TYPE_STRING_H
#define _RL_TYPE_STRING_H

#include "page_multi_string.h"

#define RL_TYPE_STRING 'T'

struct rlite;

int rl_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen, int nx, unsigned long long expires);
int rl_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char **value, long *valuelen);
int rl_append(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen, long *newlength);
int rl_getrange(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, unsigned char **value, long *valuelen);
int rl_setrange(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char *value, long valuelen, long *newlength);
int rl_incr(struct rlite *db, const unsigned char *key, long keylen, long long increment, long long *newvalue);
int rl_incrbyfloat(struct rlite *db, const unsigned char *key, long keylen, double increment, double *newvalue);
int rl_getbit(struct rlite *db, const unsigned char *key, long keylen, long offset, int *value);
int rl_setbit(struct rlite *db, const unsigned char *key, long keylen, long bitoffset, int on, int *previousvalue);
int rl_bitop(struct rlite *db, int op, const unsigned char *dest, long destlen, unsigned long keylen, const unsigned char **keys, long *keyslen);
int rl_bitcount(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, long *bitcount);
int rl_bitpos(struct rlite *db, const unsigned char *key, long keylen, int bit, long start, long stop, int end_given, long *position);

int rl_string_pages(struct rlite *db, long page, short *pages);
int rl_string_delete(struct rlite *db, long value_page);

#endif
