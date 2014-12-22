#include <math.h>
#include <stdlib.h>
#include "rlite.h"
#include "page_multi_string.h"
#include "type_hash.h"
#include "page_btree.h"
#include "util.h"

static int rl_hash_create(rlite *db, long btree_page, rl_btree **btree)
{
	rl_btree *hash = NULL;

	long max_node_size = (db->page_size - 8) / 24;
	int retval;
	RL_CALL(rl_btree_create, RL_OK, db, &hash, &rl_btree_type_hash_sha1_hashkey, max_node_size);
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_btree_hash_sha1_hashkey, btree_page, hash);

	if (btree) {
		*btree = hash;
	}
cleanup:
	return retval;
}

static int rl_hash_read(rlite *db, long hash_page_number, rl_btree **btree)
{
	void *tmp;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_hashkey, hash_page_number, &rl_btree_type_hash_sha1_hashkey, &tmp, 1);
	*btree = tmp;
	retval = RL_OK;
cleanup:
	return retval;
}

static int rl_hash_get_objects(rlite *db, const unsigned char *key, long keylen, long *_hash_page_number, rl_btree **btree, int create)
{
	long hash_page_number;
	int retval;
	if (create) {
		retval = rl_key_get_or_create(db, key, keylen, RL_TYPE_HASH, &hash_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_hash_create(db, hash_page_number, btree);
		}
		else {
			retval = rl_hash_read(db, hash_page_number, btree);
		}
	}
	else {
		unsigned char type;
		retval = rl_key_get(db, key, keylen, &type, NULL, &hash_page_number, NULL);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_HASH) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		retval = rl_hash_read(db, hash_page_number, btree);
	}
	if (_hash_page_number) {
		*_hash_page_number = hash_page_number;
	}
cleanup:
	return retval;
}

int rl_hset(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char *data, long datalen, long     *added)
{
	int retval;
	long hash_page_number;
	rl_btree *hash;
	unsigned char *digest = NULL;
	rl_hashkey *hashkey = NULL;
	RL_CALL(rl_hash_get_objects, RL_OK, db, key, keylen, &hash_page_number, &hash, 1);

	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, field, fieldlen, digest);
	RL_MALLOC(hashkey, sizeof(*hashkey));
	RL_CALL(rl_multi_string_set, RL_OK, db, &hashkey->string_page, field, fieldlen);
	RL_CALL(rl_multi_string_set, RL_OK, db, &hashkey->value_page, data, datalen);
	retval = rl_btree_update_element(db, hash, digest, hashkey);
	if (retval == RL_NOT_FOUND) {
		if (added) {
			*added = 1;
		}
		RL_CALL(rl_btree_add_element, RL_OK, db, hash, hash_page_number, digest, hashkey);
	}
	else if (retval == RL_OK) {
		if (added) {
			*added = 0;
		}
		rl_free(digest);
		digest = NULL;
	}
	else {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(digest);
		rl_free(hashkey);
	}
	return retval;
}

int rl_hget(struct rlite *db, const unsigned char *key, long keylen, unsigned char *field, long fieldlen, unsigned char **data, long *datalen)
{
	int retval;
	long hash_page_number;
	rl_btree *hash;
	void *tmp;
	unsigned char *digest = NULL;
	rl_hashkey *hashkey;
	RL_CALL(rl_hash_get_objects, RL_OK, db, key, keylen, &hash_page_number, &hash, 1);

	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, field, fieldlen, digest);

	retval = rl_btree_find_score(db, hash, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		if (data || datalen) {
			hashkey = tmp;
			rl_multi_string_get(db, hashkey->value_page, data, datalen);
		}
	}
cleanup:
	rl_free(digest);
	return retval;
}

int rl_hash_pages(struct rlite *db, long page, short *pages)
{
	rl_btree *btree;
	rl_btree_iterator *iterator;
	int retval;
	void *tmp;
	rl_hashkey *hashkey;

	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_hashkey, page, &rl_btree_type_hash_sha1_hashkey, &tmp, 1);
	btree = tmp;

	RL_CALL(rl_btree_pages, RL_OK, db, btree, pages);

	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);
	while ((rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		hashkey = tmp;
		pages[hashkey->value_page] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, hashkey->value_page, pages);
		pages[hashkey->string_page] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, hashkey->string_page, pages);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_btree_iterator_destroy(iterator);
		}
	}
	return retval;
}
