#include <stdlib.h>
#include <errno.h>
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
	RL_CALL(rl_string_get_objects, RL_OK, db, key, keylen, &page_number, NULL);
	RL_CALL(rl_multi_string_setrange, RL_OK, db, page_number, value, valuelen, index, newlength);
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
	RL_MALLOC(value, sizeof(unsigned char *) * MAX_LLONG_DIGITS);
	valuelen = snprintf((char *)value, MAX_LLONG_DIGITS, "%lld", lvalue);
	RL_CALL(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, expires);
	rl_free(value);
	retval = RL_OK;
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
