#include <stdlib.h>
#include <errno.h>
#include <math.h>
#include <ctype.h>
#include "rlite.h"
#include "page_multi_string.h"
#include "type_string.h"
#include "util.h"

static int rl_string_get_objects(rlite *db, const unsigned char *key, long keylen, long *_page_number, unsigned long long *expires)
{
	long page_number;
	int retval;
	unsigned char type;
	retval = rl_key_get(db, key, keylen, &type, NULL, &page_number, expires);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	if (type != RL_TYPE_STRING) {
		retval = RL_WRONG_TYPE;
		goto cleanup;
	}
	if (_page_number) {
		*_page_number = page_number;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen, int nx, unsigned long long expires)
{
	int retval;
	long page_number;
	int exists;
	unsigned char type;
	long value_page;
	retval = rl_key_get(db, key, keylen, &type, NULL, &value_page, NULL);
	if (retval == RL_OK && type != RL_TYPE_STRING) {
		retval = RL_WRONG_TYPE;
		goto cleanup;
	}
	exists = retval == RL_FOUND;
	if (exists) {
		if (nx) {
			retval = RL_FOUND;
			goto cleanup;
		}
		else {
			RL_CALL(rl_string_delete, RL_OK, db, value_page);
			RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
		}
	}
	RL_CALL(rl_multi_string_set, RL_OK, db, &page_number, value, valuelen);
	RL_CALL(rl_key_set, RL_OK, db, key, keylen, RL_TYPE_STRING, page_number, expires);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char **value, long *valuelen)
{
	long page_number;
	int retval;
	RL_CALL(rl_string_get_objects, RL_OK, db, key, keylen, &page_number, NULL);
	RL_CALL(rl_multi_string_get, RL_OK, db, page_number, value, valuelen);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_append(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen, long *newlength)
{
	int retval;
	long page_number;
	RL_CALL2(rl_string_get_objects, RL_OK, RL_NOT_FOUND, db, key, keylen, &page_number, NULL);
	if (retval == RL_NOT_FOUND) {
		RL_CALL(rl_multi_string_set, RL_OK, db, &page_number, value, valuelen);
		RL_CALL(rl_key_set, RL_OK, db, key, keylen, RL_TYPE_STRING, page_number, 0);
		if (newlength) {
			*newlength = valuelen;
		}
	}
	else {
		RL_CALL(rl_multi_string_append, RL_OK, db, page_number, value, valuelen, newlength);
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_getrange(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, unsigned char **value, long *valuelen)
{
	long page_number;
	int retval;
	RL_CALL(rl_string_get_objects, RL_OK, db, key, keylen, &page_number, NULL);
	RL_CALL(rl_multi_string_getrange, RL_OK, db, page_number, value, valuelen, start, stop);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_setrange(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char *value, long valuelen, long *newlength)
{
	long page_number;
	int retval;
	RL_CALL2(rl_string_get_objects, RL_OK, RL_NOT_FOUND, db, key, keylen, &page_number, NULL);
	if (retval == RL_NOT_FOUND) {
		unsigned char *padding;
		RL_MALLOC(padding, sizeof(unsigned char) * index);
		memset(padding, 0, index);
		RL_CALL(rl_set, RL_OK, db, key, keylen, padding, index, 0, 0);
		rl_free(padding);
		RL_CALL(rl_append, RL_OK, db, key, keylen, value, valuelen, newlength);
	}
	else if (retval == RL_OK) {
		RL_CALL(rl_multi_string_setrange, RL_OK, db, page_number, value, valuelen, index, newlength);
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_incr(struct rlite *db, const unsigned char *key, long keylen, long long increment, long long *newvalue)
{
	long page_number;
	int retval;
	unsigned char *value;
	char *end;
	long valuelen;
	long long lvalue;
	unsigned long long expires;
	RL_CALL2(rl_string_get_objects, RL_OK, RL_NOT_FOUND, db, key, keylen, &page_number, &expires);
	if (retval == RL_NOT_FOUND) {
		RL_MALLOC(value, sizeof(unsigned char) * MAX_LLONG_DIGITS);
		valuelen = snprintf((char *)value, MAX_LLONG_DIGITS, "%lld", increment);
		if (newvalue) {
			*newvalue = increment;
		}
		retval = rl_set(db, key, keylen, value, valuelen, 1, 0);
		rl_free(value);
		goto cleanup;
	}
	RL_CALL(rl_multi_string_getrange, RL_OK, db, page_number, &value, &valuelen, 0, MAX_LLONG_DIGITS + 1);
	if (valuelen == MAX_LLONG_DIGITS + 1) {
		retval = RL_NAN;
		goto cleanup;
	}
	lvalue = strtoll((char *)value, &end, 10);
	if (isspace(((char *)value)[0]) || end[0] != '\0' || errno == ERANGE) {
		retval = RL_NAN;
		goto cleanup;
	}
	rl_free(value);
	if ((increment < 0 && lvalue < 0 && increment < (LLONG_MIN - lvalue)) ||
	        (increment > 0 && lvalue > 0 && increment > (LLONG_MAX - lvalue))) {
		retval = RL_OVERFLOW;
		goto cleanup;
	}
	lvalue += increment;
	if (newvalue) {
		*newvalue = lvalue;
	}
	RL_MALLOC(value, sizeof(unsigned char) * MAX_LLONG_DIGITS);
	valuelen = snprintf((char *)value, MAX_LLONG_DIGITS, "%lld", lvalue);
	RL_CALL(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, expires);
	rl_free(value);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_incrbyfloat(struct rlite *db, const unsigned char *key, long keylen, double increment, double *newvalue)
{
	long page_number;
	int retval;
	unsigned char *value;
	char *end;
	long valuelen;
	double dvalue;
	unsigned long long expires;
	RL_CALL2(rl_string_get_objects, RL_OK, RL_NOT_FOUND, db, key, keylen, &page_number, &expires);
	if (retval == RL_NOT_FOUND) {
		RL_MALLOC(value, sizeof(unsigned char) * MAX_DOUBLE_DIGITS);
		valuelen = snprintf((char *)value, MAX_DOUBLE_DIGITS, "%lf", increment);
		if (newvalue) {
			*newvalue = increment;
		}
		retval = rl_set(db, key, keylen, value, valuelen, 1, 0);
		rl_free(value);
		goto cleanup;
	}
	RL_CALL(rl_multi_string_getrange, RL_OK, db, page_number, &value, &valuelen, 0, MAX_DOUBLE_DIGITS + 1);
	if (valuelen == MAX_DOUBLE_DIGITS + 1) {
		retval = RL_NAN;
		goto cleanup;
	}
	dvalue = strtold((char *)value, &end);
	if (isspace(((char *)value)[0]) || end[0] != '\0' || errno == ERANGE || isnan(dvalue)) {
		retval = RL_NAN;
		goto cleanup;
	}
	rl_free(value);
	dvalue += increment;
	if (isinf(dvalue) || isnan(dvalue)) {
		retval = RL_OVERFLOW;
		goto cleanup;
	}
	if (newvalue) {
		*newvalue = dvalue;
	}
	RL_MALLOC(value, sizeof(unsigned char) * MAX_DOUBLE_DIGITS);
	valuelen = snprintf((char *)value, MAX_DOUBLE_DIGITS, "%lf", dvalue);
	RL_CALL(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, expires);
	rl_free(value);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_getbit(struct rlite *db, const unsigned char *key, long keylen, long bitoffset, int *value)
{
	int retval;
	unsigned char *rangevalue = NULL;
	long rangevaluelen, start;
	start = bitoffset >> 3;
	long bit = 7 - (bitoffset & 0x7);
	RL_CALL2(rl_getrange, RL_OK, RL_NOT_FOUND, db, key, keylen, start, start, &rangevalue, &rangevaluelen);
	if (retval == RL_NOT_FOUND || rangevaluelen == 0) {
		*value = 0;
		retval = RL_OK;
		goto cleanup;
	}
	*value = (rangevalue[0] & (1 << bit)) ? 1 : 0;
	retval = RL_OK;
cleanup:
	rl_free(rangevalue);
	return retval;
}

int rl_setbit(struct rlite *db, const unsigned char *key, long keylen, long bitoffset, int on, int *previousvalue)
{
	int retval;
	unsigned char *rangevalue = NULL;
	long rangevaluelen, start;
	char val;
	start = bitoffset >> 3;
	long bit = 7 - (bitoffset & 0x7);
	RL_CALL2(rl_getrange, RL_OK, RL_NOT_FOUND, db, key, keylen, start, start, &rangevalue, &rangevaluelen);
	if (retval == RL_NOT_FOUND || rangevaluelen == 0) {
		val = 0;
		val &= ~(1 << bit);
		val |= ((on & 0x1) << bit);
		retval = rl_setrange(db, key, keylen, start, (unsigned char *)&val, 1, NULL);
		if (previousvalue) {
			*previousvalue = 0;
		}
		goto cleanup;
	}

	int bitval = rangevalue[0] & (1 << bit);
	if (previousvalue) {
		*previousvalue = bitval ? 1 : 0;
	}

	rangevalue[0] &= ~(1 << bit);
	rangevalue[0] |= ((on & 0x1) << bit);
	retval = rl_setrange(db, key, keylen, start, rangevalue, 1, NULL);
cleanup:
	rl_free(rangevalue);
	return retval;
}

int rl_bitop(struct rlite *db, int op, const unsigned char *dest, long destlen, unsigned long keyc, const unsigned char **keys, long *keyslen)
{
	long ltmp;
	int retval;
	unsigned char **values = NULL, *result;
	unsigned long *valueslen = NULL, i;
	long resultlen;
	RL_MALLOC(valueslen, sizeof(unsigned long) * keyc);
	RL_MALLOC(values, sizeof(unsigned char *) * keyc);
	for (i = 0; i < keyc; i++) {
		RL_CALL2(rl_get, RL_OK, RL_NOT_FOUND, db, keys[i], keyslen[i], &values[i], &ltmp);
		if (retval == RL_NOT_FOUND) {
			values[i] = NULL;
			valueslen[i] = 0;
		}
		else if (retval == RL_OK) {
			valueslen[i] = ltmp;
		}
	}
	bitop(op, keyc, values, valueslen, &result, &resultlen);
	RL_CALL(rl_set, RL_OK, db, dest, destlen, result, resultlen, 0, 0);
	free(result);
	retval = RL_OK;
cleanup:
	if (values) {
		for (i = 0; i < keyc; i++) {
			rl_free(values[i]);
		}
	}
	rl_free(values);
	rl_free(valueslen);
	return retval;
}

int rl_bitcount(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, long *bitcount)
{
	int retval;
	unsigned char *value;
	long valuelen;
	RL_CALL(rl_getrange, RL_OK, db, key, keylen, start, stop, &value, &valuelen);
	*bitcount = (long)redisPopcount(value, valuelen);
	rl_free(value);
cleanup:
	return retval;
}

int rl_string_pages(struct rlite *db, long page, short *pages)
{
	return rl_multi_string_pages(db, page, pages);
}

int rl_string_delete(struct rlite *db, long value_page)
{
	return rl_multi_string_delete(db, value_page);
}
