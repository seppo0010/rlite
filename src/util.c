#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "rlite/sha1.h"
#ifdef RL_DEBUG
#include <unistd.h>
#include <sys/stat.h>
#include <execinfo.h>
#include <valgrind/valgrind.h>
#endif
#include "rlite/status.h"
#include "rlite/util.h"
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
size_t test_mode_counter;
int failed = 0;

int expect_fail()
{
	return failed;
}

void *rl_malloc(size_t size)
{
	return rl_realloc(NULL, size);
}

void *rl_realloc(void *ptr, size_t size)
{
	if (test_mode == 0) {
		return realloc(ptr, size);
	}
	int nptrs;
#define SIZE 1000
	void *buffer[SIZE];
	char **strings;
	nptrs = backtrace(buffer, SIZE);
	strings = backtrace_symbols(buffer, nptrs);
	if (strings == NULL) {
		exit(1);
	}

	int i;
	for (i = 1; i < nptrs; i++) {
		if (--test_mode_counter == 0) {
			test_mode = 0;
			// for (i = 1; i < nptrs; i++) { fprintf(stderr, "%s\n", strings[i]); }
			free(strings);
			return NULL;
		}
	}
	free(strings);
	return realloc(ptr, size);
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

#endif

int sha1_formatter(void *v2, char **formatted, int *size)
{
	unsigned char *data = (unsigned char *)v2;
	*formatted = rl_malloc(sizeof(char) * 41);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	(*formatted)[40] = 0;
	_sha1_formatter(data, *formatted);
	if (size) {
		*size = 40;
	}
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

char *rl_get_filename_with_suffix(const char *filename, char *suffix) {
	int retval = RL_OK;
	char *new_path = NULL;
	size_t i, last_slash = 0, filenamelen = strlen(filename);
	size_t suffixlen = strlen(suffix);
	// Adding "." to the beginning of the filename, and null termination
	RL_MALLOC(new_path, sizeof(char) * (filenamelen + suffixlen + 2));
	for (i = filenamelen - 1; i > 0; i--) {
		if (filename[i] == '/') {
			last_slash = i + 1;
			break;
		}
	}
	memcpy(new_path, filename, last_slash);
	new_path[last_slash] = '.';
	memcpy(&new_path[last_slash + 1], &filename[last_slash], filenamelen - last_slash);
	memcpy(&new_path[filenamelen + 1], suffix, suffixlen + 1);
cleanup:
	return new_path;
}
