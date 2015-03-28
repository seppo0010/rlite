#ifndef _RL_OBJ_KEY_H
#define _RL_OBJ_KEY_H

struct rlite;
struct watched_key;

typedef struct {
	char identifier;
	const char *name;
	int (*delete)(struct rlite *db, long value_page);
} rl_type;

extern rl_type types[];

int rl_key_get_or_create(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long *page, long *version);
int rl_key_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires, long *version);
int rl_check_watched_keys(struct rlite *db, int watched_count, struct watched_key** keys);
int rl_key_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long page, unsigned long long expires, long version);
int rl_key_delete(struct rlite *db, const unsigned char *key, long keylen);
int rl_key_expires(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires);
int rl_key_delete_value(struct rlite *db, unsigned char identifier, long value_page);
int rl_key_delete_with_value(struct rlite *db, const unsigned char *key, long keylen);
int rl_watch(struct rlite *db, struct watched_key** _watched_key, const unsigned char *key, long keylen);

#endif
