#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "util.h"
#include "page_btree.h"
#include "page_key.h"
#include "page_multi_string.h"
#include "type_string.h"
#include "type_zset.h"
#include "type_hash.h"

#define TYPES_LENGTH 5
rl_type types[TYPES_LENGTH] = {
	{
		RL_TYPE_STRING,
		"string",
		rl_string_delete
	},
	{
		RL_TYPE_LIST,
		"list",
		rl_llist_delete
	},
	{
		RL_TYPE_SET,
		"set",
		rl_set_delete
	},
	{
		RL_TYPE_ZSET,
		"zset",
		rl_zset_delete
	},
	{
		RL_TYPE_HASH,
		"hash",
		rl_hash_delete
	},
};

static int get_type(char identifier, rl_type **type)
{
	long i;
	for (i = 0; i < TYPES_LENGTH; i++) {
		if (types[i].identifier == identifier) {
			*type = &types[i];
			return RL_OK;
		}
	}
	return RL_UNEXPECTED;
}

int rl_key_set(rlite *db, const unsigned char *key, long keylen, unsigned char type, long value_page, unsigned long long expires)
{
	int retval;
	rl_key *key_obj = NULL;
	unsigned char *digest;
	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	rl_btree *btree;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 1);
	RL_MALLOC(key_obj, sizeof(*key_obj))
	RL_CALL(rl_multi_string_set, RL_OK, db, &key_obj->string_page, key, keylen);
	key_obj->type = type;
	key_obj->value_page = value_page;
	key_obj->expires = expires;

	retval = rl_btree_update_element(db, btree, digest, key_obj);
	if (retval == RL_NOT_FOUND) {
		RL_CALL(rl_btree_add_element, RL_OK, db, btree, db->databases[db->selected_database], digest, key_obj);
	}
	else if (retval == RL_OK) {
		rl_free(digest);
	}
	else {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(digest);
		rl_free(key_obj);
	}
	return retval;
}

static int rl_key_get_ignore_expire(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires, int ignore_expire)
{
	unsigned char digest[20];
	int retval;
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	rl_btree *btree;
	rl_key *key_obj;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 0);
	void *tmp;
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		key_obj = tmp;
		if (ignore_expire == 0 && key_obj->expires != 0 && key_obj->expires <= rl_mstime()) {
			rl_key_delete_with_value(db, key, keylen);
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

int rl_key_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires)
{
	return rl_key_get_ignore_expire(db, key, keylen, type, string_page, value_page, expires, 0);
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
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 0);
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		key_obj = tmp;
		RL_CALL(rl_multi_string_delete, RL_OK, db, key_obj->string_page);
		retval = rl_btree_remove_element(db, btree, db->databases[db->selected_database], digest);
		if (retval == RL_DELETED) {
			db->databases[db->selected_database] = 0;
			retval = RL_OK;
		}
		else if (retval != RL_OK) {
			goto cleanup;
		}
	}
cleanup:
	rl_free(digest);
	return retval;
}

int rl_key_expires(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires)
{
	int retval;
	unsigned char type;
	long string_page, value_page;
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, &string_page, &value_page, NULL);
	RL_CALL(rl_key_set, RL_OK, db, key, keylen, type, value_page, expires);
cleanup:
	return retval;
}

int rl_key_delete_with_value(struct rlite *db, const unsigned char *key, long keylen)
{
	int retval;
	unsigned char identifier;
	rl_type *type;
	long value_page;
	unsigned long long expires;
	RL_CALL(rl_key_get_ignore_expire, RL_FOUND, db, key, keylen, &identifier, NULL, &value_page, &expires, 1);
	RL_CALL(get_type, RL_OK, identifier, &type);
	RL_CALL(type->delete, RL_OK, db, value_page);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	retval = expires <= rl_mstime() ? RL_NOT_FOUND : RL_OK;
cleanup:
	return retval;
}
