#ifndef _RL_TYPE_STRING_H
#define _RL_TYPE_STRING_H

#include "page_multi_string.h"

#define RL_TYPE_STRING 'T'

struct rlite;

int rl_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen);
int rl_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char **value, long *valuelen);

int rl_string_pages(struct rlite *db, long page, short *pages);
int rl_string_delete(struct rlite *db, const unsigned char *key, long keylen);

#endif
