#include <stdlib.h>

#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3

int rl_stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
void rl_internal_bitop(int op, unsigned long numkeys, unsigned char **objects, unsigned long *objectslen, unsigned char **result, long *resultlen);
size_t rl_redisPopcount(void *s, long count);
long rl_internal_bitpos(void *s, unsigned long count, int bit);
