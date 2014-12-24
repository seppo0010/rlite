#ifndef _RL_PAGE_STRING_H
#define _RL_PAGE_STRING_H

struct rlite;

int rl_string_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_string_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_string_destroy(struct rlite *db, void *obj);
int rl_string_create(struct rlite *db, unsigned char **data, long *number);
int rl_string_get(struct rlite *db, unsigned char **_data, long number);

#endif
