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

int rl_key_set(rlite *db, const unsigned char *key, long keylen, unsigned char type, long value_page, unsigned long long expires, long version)
{
	int retval;

	rl_key *key_obj = NULL;
	unsigned char *digest = NULL;
	RL_CALL2(rl_key_delete, RL_OK, RL_NOT_FOUND, db, key, keylen);
	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	rl_btree *btree;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 1);
	RL_MALLOC(key_obj, sizeof(*key_obj))
	RL_CALL(rl_multi_string_set, RL_OK, db, &key_obj->string_page, key, keylen);
	key_obj->type = type;
	key_obj->value_page = value_page;
	key_obj->expires = expires;
	// reserving version=0 for non existent keys
	if (version == 0) {
		version = 1;
	}
	key_obj->version = version;

	RL_CALL(rl_btree_add_element, RL_OK, db, btree, db->databases[db->selected_database], digest, key_obj);
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(digest);
		rl_free(key_obj);
	}
	return retval;
}

static int rl_key_get_hash_ignore_expire(struct rlite *db, unsigned char digest[20], unsigned char *type, long *string_page, long *value_page, unsigned long long *expires, long *version, int ignore_expire)
{
	int retval;
	rl_btree *btree;
	rl_key *key_obj;
	RL_CALL(rl_get_key_btree, RL_OK, db, &btree, 0);
	void *tmp = NULL;
	retval = rl_btree_find_score(db, btree, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		key_obj = tmp;
		if (ignore_expire == 0 && key_obj->expires != 0 && key_obj->expires <= rl_mstime()) {
			retval = RL_DELETED;
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
			if (version) {
				*version = key_obj->version;
			}
		}
	}
cleanup:
	return retval;
}

static int rl_key_get_ignore_expire(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires, long *version, int ignore_expire)
{
	unsigned char digest[20];
	int retval;
	RL_CALL(sha1, RL_OK, key, keylen, digest);
	RL_CALL2(rl_key_get_hash_ignore_expire, RL_FOUND, RL_DELETED, db, digest, type, string_page, value_page, expires, version, ignore_expire);
	if (retval == RL_DELETED) {
		rl_key_delete_with_value(db, key, keylen);
		if (version) {
			*version = 0;
		}
		retval = RL_NOT_FOUND;
	}
cleanup:
	return retval;
}

static int rl_key_sha_check_version(struct rlite *db, struct watched_key* key) {
	int retval;
	long version;
	int selected_database = db->selected_database;
	// if the key has expired, the version is still valid, according to
	// https://code.google.com/p/redis/issues/detail?id=270
	// it seems to be relevant to redis being stateful and single process
	// I don't think it is possible to replicate exactly the behavior, but
	// this is pretty close.
	RL_CALL2(rl_key_get_hash_ignore_expire, RL_FOUND, RL_NOT_FOUND, db, key->digest, NULL, NULL, NULL, NULL, &version, 1);
	if (retval == RL_NOT_FOUND) {
		version = 0;
	}
	if (version != key->version) {
		retval = RL_OUTDATED;
	} else {
		retval = RL_OK;
	}
cleanup:
	db->selected_database = selected_database;
	return retval;
}

int rl_check_watched_keys(struct rlite *db, int watched_count, struct watched_key** keys)
{
	int i, retval = RL_OK;

	for (i = 0; i < watched_count; i++) {
		RL_CALL(rl_key_sha_check_version, RL_OK, db, keys[i]);
	}
cleanup:
	return retval;
}

int rl_watch(struct rlite *db, struct watched_key** _watched_key, const unsigned char *key, long keylen) {
	int retval;
	struct watched_key* wkey = NULL;
	RL_MALLOC(wkey, sizeof(struct watched_key));
	wkey->database = db->selected_database;

	RL_CALL(sha1, RL_OK, key, keylen, wkey->digest);
	RL_CALL2(rl_key_get_hash_ignore_expire, RL_FOUND, RL_NOT_FOUND, db, wkey->digest, NULL, NULL, NULL, NULL, &wkey->version, 1);
	if (retval == RL_NOT_FOUND) {
		wkey->version = 0;
	}

	*_watched_key = wkey;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(wkey);
	}
	return retval;
}

int rl_key_get(struct rlite *db, const unsigned char *key, long keylen, unsigned char *type, long *string_page, long *value_page, unsigned long long *expires, long *version)
{
	return rl_key_get_ignore_expire(db, key, keylen, type, string_page, value_page, expires, version, 0);
}

int rl_key_get_or_create(struct rlite *db, const unsigned char *key, long keylen, unsigned char type, long *page, long *version)
{
	unsigned char existing_type;
	int retval = rl_key_get(db, key, keylen, &existing_type, NULL, page, NULL, version);
	long _version;
	if (retval == RL_FOUND) {
		if (existing_type != type) {
			return RL_WRONG_TYPE;
		}
	}
	else if (retval == RL_NOT_FOUND) {
		_version = rand();
		if (version) {
			*version = _version;
		}
		rl_alloc_page_number(db, page);
		RL_CALL(rl_key_set, RL_OK, db, key, keylen, type, *page, 0, _version);
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
	long string_page, value_page, version;
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, &string_page, &value_page, NULL, &version);
	RL_CALL(rl_key_set, RL_OK, db, key, keylen, type, value_page, expires, version + 1);
cleanup:
	return retval;
}

int rl_key_delete_value(struct rlite *db, unsigned char identifier, long value_page)
{
	int retval;
	rl_type *type;
	RL_CALL(get_type, RL_OK, identifier, &type);
	RL_CALL(type->delete, RL_OK, db, value_page);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_key_delete_with_value(struct rlite *db, const unsigned char *key, long keylen)
{
	int retval;
	unsigned char identifier;
	long value_page;
	unsigned long long expires;
	RL_CALL(rl_key_get_ignore_expire, RL_FOUND, db, key, keylen, &identifier, NULL, &value_page, &expires, NULL, 1);
	RL_CALL(rl_key_delete_value, RL_OK, db, identifier, value_page);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	retval = expires != 0 && expires <= rl_mstime() ? RL_NOT_FOUND : RL_OK;
cleanup:
	return retval;
}
