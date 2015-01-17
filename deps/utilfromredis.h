#include <stdlib.h>

#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3

int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
void bitop(int op, unsigned long numkeys, unsigned char **objects, unsigned long *objectslen, unsigned char **result, long *resultlen);
size_t redisPopcount(void *s, long count);
long bitpos(void *s, unsigned long count, int bit);
