#include <stdlib.h>
#include "rlite.h"
#include "page_multi_string.h"
#include "type_string.h"
#include "util.h"

static int rl_string_get_objects(rlite *db, const unsigned char *key, long keylen, long *_page_number)
{
	long page_number;
	int retval;
	unsigned char type;
	retval = rl_key_get(db, key, keylen, &type, NULL, &page_number, NULL);
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
	RL_CALL(rl_string_get_objects, RL_OK, db, key, keylen, &page_number);
	RL_CALL(rl_multi_string_get, RL_OK, db, page_number, value, valuelen);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_append(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen, long *newlength)
{
	int retval;
	long page_number;
	RL_CALL2(rl_string_get_objects, RL_OK, RL_NOT_FOUND, db, key, keylen, &page_number);
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
	RL_CALL(rl_string_get_objects, RL_OK, db, key, keylen, &page_number);
	RL_CALL(rl_multi_string_getrange, RL_OK, db, page_number, value, valuelen, start, stop);
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
