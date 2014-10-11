#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
