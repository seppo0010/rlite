#ifndef _RL_OBJ_KEY_H
#define _RL_OBJ_KEY_H

struct rlite;

int rl_obj_key_get(struct rlite *db, unsigned char *key, long keylen, unsigned char *type, long *page);
int rl_obj_key_set(struct rlite *db, unsigned char *key, long keylen, unsigned char type, long page);
int rl_obj_key_delete(struct rlite *db, unsigned char *key, long keylen);

int key_cmp(void *v1, void *v2);

#endif
