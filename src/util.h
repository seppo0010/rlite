#ifndef _RL_UTIL_H
#define _RL_UTIL_H

#ifdef __GNUC__
#  define UNUSED(x) UNUSED_ ## x __attribute__((__unused__))
#else
#  define UNUSED(x) UNUSED_ ## x
#endif

void *memmove_dbg(void *dest, void *src, size_t n, int flag);
int get_4bytes(const unsigned char *p);
void put_4bytes(unsigned char *p, long v);
int long_cmp(void *v1, void *v2);
int sha1_cmp(void *v1, void *v2);
int double_cmp(void *v1, void *v2);
#ifdef DEBUG
int long_formatter(void *v2, char **formatted, int *size);
int sha1_formatter(void *v2, char **formatted, int *size);
int double_formatter(void *v2, char **formatted, int *size);
#endif
double get_double(const unsigned char *p);
void put_double(unsigned char *p, double v);
int sha1(const unsigned char *data, long datalen, unsigned char digest[20]);

#endif
