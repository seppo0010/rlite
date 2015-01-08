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

static int _rl_string_delete(rlite *db, const unsigned char *key, long keylen)
{
	int retval;
	long page_number;
	retval = rl_string_get_objects(db, key, keylen, &page_number);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
	}
	else if (retval == RL_OK) {
		retval = rl_multi_string_delete(db, page_number);
	}
	return retval;
}
int rl_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char *value, long valuelen)
{
	int retval;
	long page_number;
	RL_CALL2(rl_string_delete, RL_OK, RL_NOT_FOUND, db, key, keylen);
	RL_CALL(rl_multi_string_set, RL_OK, db, &page_number, value, valuelen);
	RL_CALL(rl_key_set, RL_OK, db, key, keylen, RL_TYPE_STRING, page_number, 0);
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

int rl_string_pages(struct rlite *db, long page, short *pages)
{
	return rl_multi_string_pages(db, page, pages);
}

int rl_string_delete(struct rlite *db, const unsigned char *key, long keylen)
{
	int retval;
	RL_CALL(_rl_string_delete, RL_OK, db, key, keylen);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	retval = RL_OK;
cleanup:
	return retval;
}
