#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "util.h"
#include "page_btree.h"
#include "page_key.h"
#include "page_multi_string.h"

struct page_key {
	rlite *db;
	long keylen;
	unsigned char *key;
};

int rl_key_set(rlite *db, const unsigned char *key, long keylen, unsigned char type, long value_page)
{
	unsigned char *digest = malloc(sizeof(unsigned char) * 20);
	int retval = sha1(key, keylen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	rl_btree *btree;
	retval = rl_get_key_btree(db, &btree);
	if (retval != RL_OK) {
		goto cleanup;
	}
	rl_key *key_obj = malloc(sizeof(rl_key));
	if (!key_obj) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	retval = rl_multi_string_set(db, &key_obj->string_page, key, keylen);
	if (retval != RL_OK) {
		goto cleanup;
	}
	key_obj->type = type;
	key_obj->value_page = value_page;
	retval = rl_btree_add_element(db, btree, digest, key_obj);
	if (retval != RL_OK) {
		goto cleanup;
	}
cleanup:
	return retval;
}

int rl_key_get(rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page)
{
	unsigned char digest[20];
	int retval = sha1(key, keylen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	rl_btree *btree;
	retval = rl_get_key_btree(db, &btree);
	if (retval != RL_OK) {
		goto cleanup;
	}
	void *tmp;
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		rl_key *key = tmp;
		if (type) {
			*type = key->type;
		}
		if (string_page) {
			*string_page = key->string_page;
		}
		if (value_page) {
			*value_page = key->value_page;
		}
	}
cleanup:
	return retval;
}

int rl_key_get_or_create(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long *page)
{
	unsigned char existing_type;
	int retval = rl_key_get(db, key, keylen, &existing_type, NULL, page);
	if (retval == RL_FOUND) {
		if (existing_type != type) {
			return RL_WRONG_TYPE;
		}
	}
	else if (retval == RL_NOT_FOUND) {
		rl_alloc_page_number(db, page);
		retval = rl_key_set(db, key, keylen, type, *page);
		if (retval != RL_OK) {
			return retval;
		}
		retval = RL_NOT_FOUND;

	}
	return retval;
}

int rl_key_delete(struct rlite *db, const unsigned char *key, long keylen)
{
	unsigned char *digest = malloc(sizeof(unsigned char) * 20);
	int retval = sha1(key, keylen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	rl_btree *btree;
	retval = rl_get_key_btree(db, &btree);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_btree_remove_element(db, btree, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
cleanup:
	free(digest);
	return retval;
}
