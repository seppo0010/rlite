#ifndef _RL_UTIL_H
#define _RL_UTIL_H

void *memmove_dbg(void *dest, void *src, size_t n, int flag);
int get_4bytes(const unsigned char *p);
void put_4bytes(unsigned char *p, long v);
int long_cmp(void *v1, void *v2);
int long_formatter(void *v2, char **formatted, int *size);
int md5_cmp(void *v1, void *v2);
int md5_formatter(void *v2, char **formatted, int *size);
int double_cmp(void *v1, void *v2);
int double_formatter(void *v2, char **formatted, int *size);
double get_double(const unsigned char *p);
void put_double(unsigned char *p, double v);
int md5(const unsigned char *data, long datalen, unsigned char digest[16]);

#endif
