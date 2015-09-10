#ifndef _RL_OBJ_STRING_H
#define _RL_OBJ_STRING_H

struct rlite;

int rl_normalize_string_range(long totalsize, long *start, long *stop);
int rl_multi_string_cmp(struct rlite *db, long p1, long p2, int *cmp);
int rl_multi_string_cmp_str(struct rlite *db, long p1, unsigned char *str, long len, int *cmp);
int rl_multi_string_getrange(struct rlite *db, long number, unsigned char **_data, long *size, long start, long stop);
int rl_multi_string_get(struct rlite *db, long number, unsigned char **data, long *size);
int rl_multi_string_setrange(struct rlite *db, long number, const unsigned char *data, long size, long offset, long *newlength);
int rl_multi_string_set(struct rlite *db, long *number, const unsigned char *data, long size);
int rl_multi_string_append(struct rlite *db, long number, const unsigned char *data, long datasize, long *newlength);
int rl_multi_string_sha1(struct rlite *db, unsigned char data[20], long number);
int rl_multi_string_pages(struct rlite *db, long page, short *pages);
int rl_multi_string_delete(struct rlite *db, long page);
int rl_multi_string_cpyrange(struct rlite *db, long number, unsigned char *data, long *size, long start, long stop);
int rl_multi_string_cpy(struct rlite *db, long number, unsigned char *data, long *size);

#endif
