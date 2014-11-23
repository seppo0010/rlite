#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "util.h"
#include "page_btree.h"
#include "page_key.h"
#include "page_multi_string.h"

int rl_key_set(rlite *db, const unsigned char *key, long keylen, unsigned char type, long value_page, unsigned long long expires)
{
	int called_add_element = 0;
	int retval;
	rl_key *key_obj = NULL;
	unsigned char *digest;
	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	rl_btree *btree;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree);
	RL_MALLOC(key_obj, sizeof(*key_obj))
	RL_CALL(rl_multi_string_set, RL_OK, db, &key_obj->string_page, key, keylen);
	key_obj->type = type;
	key_obj->value_page = value_page;
	key_obj->expires = expires;
	called_add_element = 1;
	retval = rl_btree_remove_element(db, btree, digest);
	if (retval != RL_OK && retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	RL_CALL(rl_btree_add_element, RL_OK, db, btree, digest, key_obj);
	retval = RL_OK;
cleanup:
	if (retval != RL_OK && !called_add_element) {
		rl_free(digest);
		rl_free(key_obj);
	}
	return retval;
}

int rl_key_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires)
{
	unsigned char digest[20];
	int retval;
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	rl_btree *btree;
	rl_key *key_obj;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree);
	void *tmp;
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		key_obj = tmp;
		if (key_obj->expires != 0 && key_obj->expires < mstime()) {
			// TODO: delete key and object
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
		else {
			if (type) {
				*type = key_obj->type;
			}
			if (string_page) {
				*string_page = key_obj->string_page;
			}
			if (value_page) {
				*value_page = key_obj->value_page;
			}
			if (expires) {
				*expires = key_obj->expires;
			}
		}
	}
cleanup:
	return retval;
}

int rl_key_get_or_create(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long *page)
{
	unsigned char existing_type;
	int retval = rl_key_get(db, key, keylen, &existing_type, NULL, page, NULL);
	if (retval == RL_FOUND) {
		if (existing_type != type) {
			return RL_WRONG_TYPE;
		}
	}
	else if (retval == RL_NOT_FOUND) {
		rl_alloc_page_number(db, page);
		RL_CALL(rl_key_set, RL_OK, db, key, keylen, type, *page, 0);
		retval = RL_NOT_FOUND;

	}
cleanup:
	return retval;
}

int rl_key_delete(struct rlite *db, const unsigned char *key, long keylen)
{
	int retval;
	void *tmp;
	unsigned char *digest;
	rl_btree *btree = NULL;
	rl_key *key_obj = NULL;
	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree);
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		key_obj = tmp;
		RL_CALL(rl_multi_string_delete, RL_OK, db, key_obj->string_page);
		RL_CALL(rl_btree_remove_element, RL_OK, db, btree, digest);
	}
cleanup:
	rl_free(digest);
	return retval;
}
