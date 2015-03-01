#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "sha1.h"
#ifdef RL_DEBUG
#include <unistd.h>
#include <sys/stat.h>
#include <execinfo.h>
#include <valgrind/valgrind.h>
#endif
#include "status.h"
#include "util.h"
#include <sys/time.h>

int _sha1_formatter(unsigned char *data, char formatted[40])
{
	static const char *hex_lookup = "0123456789ABCDEF";
	int i;
	for (i = 0; i < 20; i++) {
		formatted[i * 2] = hex_lookup[data[i] & 0x0F];
		formatted[i * 2 + 1] = hex_lookup[(data[i] / 0x0F) & 0x0F];
	}
	return RL_OK;
}

#ifdef RL_DEBUG

int test_mode = 0;
int failed = 0;

int expect_fail()
{
	return failed;
}

void *rl_malloc(size_t size)
{
	if (test_mode == 0) {
		return malloc(size);
	}
	unsigned char digest[20];
	char hexdigest[41];
	int j, nptrs;
	void *r;
#define SIZE 1000
	void *buffer[SIZE];
	char **strings;
	nptrs = backtrace(buffer, SIZE);
	strings = backtrace_symbols(buffer, nptrs);
	if (strings == NULL) {
		return NULL;
	}

	SHA1_CTX sha;
	SHA1Init(&sha);
	for (j = 0; j < nptrs; j++) {
		SHA1Update(&sha, (unsigned char *)strings[j], strlen(strings[j]));
	}
	SHA1Final(digest, &sha);

	_sha1_formatter(digest, hexdigest);

	const char *dir = "malloc-debug";
	char subdir[100];
	mkdir(dir, 0777);
	long i = strlen(dir);
	memcpy(subdir, dir, strlen(dir));
	subdir[i] = '/';
	subdir[i + 1] = hexdigest[0];
	subdir[i + 2] = hexdigest[1];
	subdir[i + 3] = 0;
	mkdir(subdir, 0777);
	subdir[i + 3] = '/';

	for (j = 0; j < 38; j++) {
		subdir[i + 4 + j] = hexdigest[2 + j];
	}
	subdir[42] = 0;

	if (access(subdir, F_OK) == 0) {
		r = malloc(size);
	}
	else {
		hexdigest[40] = 0;
		fprintf(stderr, "Failing malloc on %s\n", hexdigest);

		FILE *fp = fopen(subdir, "w");
		for (j = 0; j < nptrs; j++) {
			fwrite(strings[j], 1, strlen(strings[j]), fp);
			fwrite("\n", 1, 1, fp);
		}
		fclose(fp);
		failed = 1;
		fprintf(stderr, "Simulating OOM\n");
		r = NULL;
	}
	free(strings);
	return r;
}

void rl_free(void *ptr)
{
	free(ptr);
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

int sha1_cmp(void *v1, void *v2)
{
	return memcmp(v1, v2, sizeof(unsigned char) * 20);
}

#ifdef RL_DEBUG
int long_formatter(void *v2, char **formatted, int *size)
{
	*formatted = rl_malloc(sizeof(char) * 22);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	*size = snprintf(*formatted, 22, "%ld", *(long *)v2);
	return RL_OK;
}

int sha1_formatter(void *v2, char **formatted, int *size)
{
	unsigned char *data = (unsigned char *)v2;
	*formatted = rl_malloc(sizeof(char) * 40);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	_sha1_formatter(data, *formatted);
	if (size) {
		*size = 40;
	}
	return RL_OK;
}

#endif

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

	// This doesn't look very formal, but the values for +inf and -inf were
	// calculated using the formula described in
	// http://stackoverflow.com/a/3421923/551548
	if ((double)f == (double) - INFINITY) {
		return 140737360872672;
	}

	if ((double)f == (double)INFINITY) {
		return 140737151196432;
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
	if (i == 140737360872672) {
		return -INFINITY;
	}

	if (i == 140737151196432) {
		return INFINITY;
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

int sha1(const unsigned char *data, long datalen, unsigned char digest[20])
{
	SHA1_CTX sha;
	SHA1Init(&sha);
	SHA1Update(&sha, data, datalen);
	SHA1Final(digest, &sha);
	return RL_OK;
}

unsigned long long rl_mstime()
{
	struct timeval tp;
	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

double rl_strtod(unsigned char *_str, long strlen, unsigned char **_eptr) {
	double d;
	char str[40];
	char *eptr;
	if (strlen > 39) {
		strlen = 39;
	}
	memcpy(str, _str, strlen);
	str[strlen] = 0;
	d = strtod(str, &eptr);
	if (_eptr) {
		*_eptr = _str + (eptr - str);
	}
	return d;
}
