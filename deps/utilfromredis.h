int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
void bitop(int op, unsigned long numkeys, unsigned char **objects, unsigned long *objectslen, unsigned char **result, long *resultlen);

#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3
