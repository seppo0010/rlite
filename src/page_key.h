#ifndef _RL_OBJ_KEY_H
#define _RL_OBJ_KEY_H

struct rlite;

int rl_key_get_or_create(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long *page);
int rl_key_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires);
int rl_key_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long page, unsigned long long expires);
int rl_key_delete(struct rlite *db, const unsigned char *key, long keylen);

#endif
