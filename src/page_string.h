#ifndef _PAGE_STRING_H
#define _PAGE_STRING_H

struct rlite;

int rl_serialize_string(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_string(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_destroy_string(struct rlite *db, void *obj);
int rl_string_create(struct rlite *db, unsigned char **data, long *number);
int rl_string_get(rlite *db, unsigned char **_data, long number);

#endif
