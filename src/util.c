#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "status.h"

#ifdef DEBUG
void *memmove_dbg(void *dest, void *src, size_t n, int flag)
{
	void *data = malloc(n);
	memmove(data, src, n);
	memset(src, flag, n);
	void *retval = memmove(dest, data, n);
	free(data);
	return retval;
}
#else
void *memmove_dbg(void *dest, void *src, size_t n, int flag)
{
	flag = flag; // avoid warning
	return memmove(dest, src, n);
}
#endif


int get_4bytes(const unsigned char *p)
{
	return (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}


void put_4bytes(unsigned char *p, long v)
{
	p[0] = (unsigned char)(v >> 24);
	p[1] = (unsigned char)(v >> 16);
	p[2] = (unsigned char)(v >> 8);
	p[3] = (unsigned char)v;
}

int long_cmp(void *v1, void *v2)
{
	long a = *((long *)v1), b = *((long *)v2);
	if (a == b) {
		return 0;
	}
	return a > b ? 1 : -1;
}

int long_formatter(void *v2, char **formatted, int *size)
{
	*formatted = malloc(sizeof(char) * 22);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	*size = snprintf(*formatted, 22, "%ld", *(long *)v2);
	return RL_OK;
}

int md5_cmp(void *v1, void *v2)
{
	return memcmp(v1, v2, sizeof(unsigned char) * 16);
}

int md5_formatter(void *v2, char **formatted, int *size)
{
	unsigned char *data = (unsigned char *)v2;
	static const char *hex_lookup = "0123456789ABCDEF";
	*formatted = malloc(sizeof(char) * 32);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	int i;
	for (i = 0; i < 16; i++) {
		*formatted[i * 2] = hex_lookup[data[i] & 0x0F];
		*formatted[i * 2 + 1] = hex_lookup[(data[i] / 0x0F) & 0x0F];
	}
	*size = 32;
	return RL_OK;
}
