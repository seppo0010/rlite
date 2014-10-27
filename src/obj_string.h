#ifndef _RL_OBJ_STRING_H
#define _RL_OBJ_STRING_H

struct rlite;

int rl_obj_string_cmp(struct rlite *db, long p1, long p2, int *cmp);
int rl_obj_string_get(struct rlite *db, long number, unsigned char **data, long *size);
int rl_obj_string_set(struct rlite *db, long *number, unsigned char *data, long size);

#endif
