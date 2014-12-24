#ifndef _RL_PAGE_LONG_H
#define _RL_PAGE_LONG_H

struct rlite;

int rl_long_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_long_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_long_destroy(struct rlite *db, void *obj);
int rl_long_create(struct rlite *db, long value, long *number);
int rl_long_get(struct rlite *db, long *value, long number);
int rl_long_set(struct rlite *db, long value, long number);

#endif

