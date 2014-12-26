#include <stdlib.h>
#include "rlite.h"
#include "page_multi_string.h"
#include "type_set.h"
#include "page_btree.h"
#include "util.h"

static int rl_set_create(rlite *db, long btree_page, rl_btree **btree)
{
	rl_btree *set = NULL;

	long max_node_size = (db->page_size - 8) / 24;
	int retval;
	RL_CALL(rl_btree_create, RL_OK, db, &set, &rl_btree_type_hash_sha1_long, max_node_size);
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_btree_hash_sha1_long, btree_page, set);

	if (btree) {
		*btree = set;
	}
cleanup:
	return retval;
}

static int rl_set_read(rlite *db, long set_page_number, rl_btree **btree)
{
	void *tmp;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_long, set_page_number, &rl_btree_type_hash_sha1_long, &tmp, 1);
	*btree = tmp;
	retval = RL_OK;
cleanup:
	return retval;
}

static int rl_set_get_objects(rlite *db, const unsigned char *key, long keylen, long *_set_page_number, rl_btree **btree, int create)
{
	long set_page_number;
	int retval;
	if (create) {
		retval = rl_key_get_or_create(db, key, keylen, RL_TYPE_SET, &set_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_set_create(db, set_page_number, btree);
		}
		else {
			retval = rl_set_read(db, set_page_number, btree);
		}
	}
	else {
		unsigned char type;
		retval = rl_key_get(db, key, keylen, &type, NULL, &set_page_number, NULL);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_SET) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		retval = rl_set_read(db, set_page_number, btree);
	}
	if (_set_page_number) {
		*_set_page_number = set_page_number;
	}
cleanup:
	return retval;
}
int rl_sadd(struct rlite *db, const unsigned char *key, long keylen, int memberc, unsigned char **members, long *memberslen, long *added)
{
	int i, retval;
	long set_page_number;
	rl_btree *set;
	unsigned char *digest = NULL;
	long *member = NULL;
	long count = 0;
	void *tmp;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 1);

	for (i = 0; i < memberc; i++) {
		RL_MALLOC(digest, sizeof(unsigned char) * 20);
		RL_CALL(sha1, RL_OK, members[i], memberslen[i], digest);

		retval = rl_btree_find_score(db, set, digest, &tmp, NULL, NULL);
		if (retval == RL_NOT_FOUND) {
			RL_MALLOC(member, sizeof(*member));
			RL_CALL(rl_multi_string_set, RL_OK, db, member, members[i], memberslen[i]);

			retval = rl_btree_add_element(db, set, set_page_number, digest, member);
			if (retval != RL_OK) {
				goto cleanup;
			}
			count++;
		} else if (retval == RL_FOUND) {
			rl_free(digest);
			digest = NULL;
		} else {
			goto cleanup;
		}
	}
	if (added) {
		*added = count;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(digest);
		rl_free(member);
	}
	return retval;
}

int rl_sismember(struct rlite *db, const unsigned char *key, long keylen, unsigned char *member, long memberlen)
{
	int retval;
	long set_page_number;
	rl_btree *set;
	unsigned char digest[20];
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 0);

	RL_CALL(sha1, RL_OK, member, memberlen, digest);

	retval = rl_btree_find_score(db, set, digest, NULL, NULL, NULL);
cleanup:
	return retval;
}

int rl_scard(struct rlite *db, const unsigned char *key, long keylen, long *card)
{
	int retval;
	long set_page_number;
	rl_btree *set;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 0);

	*card = set->number_of_elements;
cleanup:
	return retval;
}

int rl_set_pages(struct rlite *db, long page, short *pages)
{
	rl_btree *btree;
	rl_btree_iterator *iterator;
	int retval;
	void *tmp;
	long member;

	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_long, page, &rl_btree_type_hash_sha1_long, &tmp, 1);
	btree = tmp;

	RL_CALL(rl_btree_pages, RL_OK, db, btree, pages);

	RL_CALL(rl_btree_iterator_create, RL_OK, db, btree, &iterator);
	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		member = *(long *)tmp;
		pages[member] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, member, pages);
		rl_free(tmp);
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

int rl_set_delete(rlite *db, const unsigned char *key, long keylen)
{
	long hash_page_number;
	rl_btree *hash;
	rl_btree_iterator *iterator;
	long member;
	int retval;
	void *tmp;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &hash_page_number, &hash, 0);
	RL_CALL(rl_btree_iterator_create, RL_OK, db, hash, &iterator);
	while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
		member = *(long *)tmp;
		rl_multi_string_delete(db, member);
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(rl_btree_delete, RL_OK, db, hash);
	RL_CALL(rl_delete, RL_OK, db, hash_page_number);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	retval = RL_OK;
cleanup:
	return retval;
}
