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

int rl_string_pages(struct rlite *db, long page, short *pages);
int rl_string_delete(struct rlite *db, long value_page);

#endif
