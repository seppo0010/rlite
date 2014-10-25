#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <openssl/md5.h>
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


void put_4bytes(unsigned char *p, long v)
{
	p[0] = (unsigned char)(v >> 24);
	p[1] = (unsigned char)(v >> 16);
	p[2] = (unsigned char)(v >> 8);
	p[3] = (unsigned char)v;
}

int get_4bytes(const unsigned char *p)
{
	return (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}

unsigned long long get_8bytes(const unsigned char *p)
{
	unsigned long long retval = 0;
	retval |= (unsigned long long)p[0] << 56;
	retval |= (unsigned long long)p[1] << 48;
	retval |= (unsigned long long)p[2] << 40;
	retval |= (unsigned long long)p[3] << 32;
	retval |= (unsigned long long)p[4] << 24;
	retval |= (unsigned long long)p[5] << 16;
	retval |= (unsigned long long)p[6] << 8;
	retval |= (unsigned long long)p[7];
	return retval;
}

void put_8bytes(unsigned char *p, unsigned long long v)
{
	p[0] = (unsigned char)(v >> 56);
	p[1] = (unsigned char)(v >> 48);
	p[2] = (unsigned char)(v >> 40);
	p[3] = (unsigned char)(v >> 32);
	p[4] = (unsigned char)(v >> 24);
	p[5] = (unsigned char)(v >> 16);
	p[6] = (unsigned char)(v >> 8);
	p[7] = (unsigned char)v;
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

int double_cmp(void *v1, void *v2)
{
	double a = *((double *)v1), b = *((double *)v2);
	if (a == b) {
		return 0;
	}
	return a > b ? 1 : -1;
}

int double_formatter(void *v2, char **formatted, int *size)
{
	*formatted = malloc(sizeof(char) * 22);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	*size = snprintf(*formatted, 22, "%lf", *(double *)v2);
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

// Code for serialize/deserialize double comes from
// http://beej.us/guide/bgnet/output/html/singlepage/bgnet.html#serialization

#define pack754_32(f) (pack754((f), 32, 8))
#define pack754_64(f) (pack754((f), 64, 11))
#define unpack754_32(i) (unpack754((i), 32, 8))
#define unpack754_64(i) (unpack754((i), 64, 11))

unsigned long long pack754(long double f, unsigned bits, unsigned expbits)
{
	long double fnorm;
	int shift;
	long long sign, exp, significand;
	unsigned significandbits = bits - expbits - 1; // -1 for sign bit

	if (f == 0.0) {
		return 0;    // get this special case out of the way
	}

	// check sign and begin normalization
	if (f < 0) {
		sign = 1;
		fnorm = -f;
	}
	else {
		sign = 0;
		fnorm = f;
	}

	// get the normalized form of f and track the exponent
	shift = 0;
	while(fnorm >= 2.0) {
		fnorm /= 2.0;
		shift++;
	}
	while(fnorm < 1.0) {
		fnorm *= 2.0;
		shift--;
	}
	fnorm = fnorm - 1.0;

	// calculate the binary form (non-float) of the significand data
	significand = fnorm * ((1LL << significandbits) + 0.5f);

	// get the biased exponent
	exp = shift + ((1 << (expbits - 1)) - 1); // shift + bias

	// return the final answer
	return (sign << (bits - 1)) | (exp << (bits - expbits - 1)) | significand;
}

long double unpack754(unsigned long long i, unsigned bits, unsigned expbits)
{
	long double result;
	long long shift;
	unsigned bias;
	unsigned significandbits = bits - expbits - 1; // -1 for sign bit

	if (i == 0) {
		return 0.0;
	}

	// pull the significand
	result = (i & ((1LL << significandbits) - 1)); // mask
	result /= (1LL << significandbits); // convert back to float
	result += 1.0f; // add the one back on

	// deal with the exponent
	bias = (1 << (expbits - 1)) - 1;
	shift = ((i >> significandbits) & ((1LL << expbits) - 1)) - bias;
	while(shift > 0) {
		result *= 2.0;
		shift--;
	}
	while(shift < 0) {
		result /= 2.0;
		shift++;
	}

	// sign it
	result *= (i >> (bits - 1)) & 1 ? -1.0 : 1.0;

	return result;
}

double get_double(const unsigned char *p)
{
	unsigned long long ull = get_8bytes(p);
	return unpack754_64(ull);
}

void put_double(unsigned char *p, double v)
{
	unsigned long long ull = pack754_64(v);
	put_8bytes(p, ull);
}

int md5(const unsigned char *data, long datalen, unsigned char digest[16])
{
	MD5_CTX md5;
	MD5_Init(&md5);
	MD5_Update(&md5, data, datalen);
	MD5_Final(digest, &md5);
	return RL_OK;
}
