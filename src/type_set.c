#include <stdlib.h>
#include "rlite/rlite.h"
#include "rlite/page_multi_string.h"
#include "rlite/type_set.h"
#include "rlite/page_btree.h"
#include "rlite/util.h"

static int rl_set_create(rlite *db, long btree_page, rl_btree **btree)
{
	rl_btree *set = NULL;

	int retval;
	RL_CALL(rl_btree_create, RL_OK, db, &set, &rl_btree_type_hash_sha1_long);
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
	if (btree) {
		*btree = tmp;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_set_get_objects(rlite *db, const unsigned char *key, long keylen, long *_set_page_number, rl_btree **btree, int update_version, int create)
{
	long set_page_number, version;
	int retval;
	unsigned long long expires = 0;
	if (create) {
		retval = rl_key_get_or_create(db, key, keylen, RL_TYPE_SET, &set_page_number, &version);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_set_create(db, set_page_number, btree);
			goto cleanup;
		}
		else {
			RL_CALL(rl_set_read, RL_OK, db, set_page_number, btree);
		}
	}
	else {
		unsigned char type;
		retval = rl_key_get(db, key, keylen, &type, NULL, &set_page_number, NULL, &version);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_SET) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		RL_CALL(rl_set_read, RL_OK, db, set_page_number, btree);
	}
	if (update_version) {
		RL_CALL(rl_key_set, RL_OK, db, key, keylen, RL_TYPE_SET, set_page_number, expires, version + 1);
	}
cleanup:
	if (_set_page_number) {
		*_set_page_number = set_page_number;
	}
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
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 1, 1);

	for (i = 0; i < memberc; i++) {
		RL_MALLOC(digest, sizeof(unsigned char) * 20);
		RL_CALL(sha1, RL_OK, members[i], memberslen[i], digest);

		retval = rl_btree_find_score(db, set, digest, &tmp, NULL, NULL);
		if (retval == RL_NOT_FOUND) {
			RL_MALLOC(member, sizeof(*member));
			RL_CALL(rl_multi_string_set, RL_OK, db, member, members[i], memberslen[i]);
			RL_CALL(rl_btree_add_element, RL_OK, db, set, set_page_number, digest, member);
			count++;
		}
		else if (retval == RL_FOUND) {
			rl_free(digest);
			digest = NULL;
		}
		else {
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

int rl_srem(struct rlite *db, const unsigned char *key, long keylen, int membersc, unsigned char **members, long *memberslen, long *delcount)
{
	int retval;
	long set_page_number;
	rl_btree *set;
	long member;
	void *tmp;
	long i;
	long deleted = 0;
	int keydeleted = 0;
	unsigned char digest[20];
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 1, 0);

	for (i = 0; i < membersc; i++) {
		RL_CALL(sha1, RL_OK, members[i], memberslen[i], digest);
		retval = rl_btree_find_score(db, set, digest, &tmp, NULL, NULL);
		if (retval == RL_FOUND) {
			deleted++;
			member = *(long *)tmp;
			rl_multi_string_delete(db, member);
			retval = rl_btree_remove_element(db, set, set_page_number, digest);
			if (retval != RL_OK && retval != RL_DELETED) {
				goto cleanup;
			}
			if (retval == RL_DELETED) {
				keydeleted = 1;
				break;
			}
		}
	}
	if (delcount) {
		*delcount = deleted;
	}
	if (keydeleted) {
		RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_sismember(struct rlite *db, const unsigned char *key, long keylen, unsigned char *member, long memberlen)
{
	int retval;
	long set_page_number;
	rl_btree *set;
	unsigned char digest[20];
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 0, 0);

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
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 0, 0);

	*card = set->number_of_elements;
cleanup:
	return retval;
}

int rl_smove(struct rlite *db, const unsigned char *source, long sourcelen, const unsigned char *destination, long destinationlen, unsigned char *member, long memberlen)
{
	rl_btree *source_hash, *target_hash;
	void *tmp;
	long target_page_number, source_page_number, *member_page_number;
	int retval;
	unsigned char *digest = NULL;
	// make sure the target key is a set or does not exist
	RL_CALL2(rl_set_get_objects, RL_OK, RL_NOT_FOUND, db, destination, destinationlen, NULL, NULL, 0, 0);

	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, member, memberlen, digest);
	RL_CALL(rl_set_get_objects, RL_OK, db, source, sourcelen, &source_page_number, &source_hash, 1, 0);
	retval = rl_btree_find_score(db, source_hash, digest, &tmp, NULL, NULL);
	if (retval == RL_FOUND) {
		rl_multi_string_delete(db, *(long *)tmp);
		retval = rl_btree_remove_element(db, source_hash, source_page_number, digest);
		if (retval == RL_DELETED) {
			RL_CALL(rl_key_delete, RL_OK, db, source, sourcelen);
		}
		else if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		goto cleanup;
	}
	RL_CALL(rl_set_get_objects, RL_OK, db, destination, destinationlen, &target_page_number, &target_hash, 1, 1);
	RL_MALLOC(member_page_number, sizeof(*member_page_number))
	RL_CALL(rl_multi_string_set, RL_OK, db, member_page_number, member, memberlen);
	RL_CALL(rl_btree_add_element, RL_OK, db, target_hash, target_page_number, digest, member_page_number);
cleanup:
	if (retval != RL_OK) {
		rl_free(digest);
	}
	return retval;
}

int rl_set_iterator_next(rl_set_iterator *iterator, long *_page, unsigned char **member, long *memberlen)
{
	void *tmp;
	long page;
	int retval = rl_btree_iterator_next(iterator, NULL, &tmp);
	if (retval == RL_OK) {
		page = *(long *)tmp;
		if (_page) {
			*_page = page;
		}
		rl_free(tmp);
		retval = rl_multi_string_get(iterator->db, page, member, memberlen);
		if (retval != RL_OK) {
			rl_set_iterator_destroy(iterator);
		}
	}
	return retval;
}

int rl_set_iterator_destroy(rl_set_iterator *iterator)
{
	return rl_btree_iterator_destroy(iterator);
}

int rl_smembers(struct rlite *db, rl_set_iterator **iterator, const unsigned char *key, long keylen)
{
	int retval;
	rl_btree *set;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, NULL, &set, 0, 0);
	RL_CALL(rl_btree_iterator_create, RL_OK, db, set, iterator);
cleanup:
	return retval;
}

static int contains(long size, long *elements, long element)
{
	long i;
	for (i = 0; i < size; i++) {
		if (element == elements[i]) {
			return 1;
		}
	}
	return 0;
}

int rl_srandmembers(struct rlite *db, const unsigned char *key, long keylen, int repeat, long *memberc, unsigned char ***_members, long **_memberslen)
{
	long i;
	int retval;
	long *member;
	long *used_members = NULL;
	rl_btree *set;
	unsigned char **members = NULL;
	long *memberslen = NULL;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, NULL, &set, 0, 0);
	if (!repeat) {
		if (*memberc > set->number_of_elements) {
			*memberc = set->number_of_elements;
		}
		RL_MALLOC(used_members, sizeof(long) * *memberc);
	}

	RL_MALLOC(members, sizeof(unsigned char *) * *memberc);
	RL_MALLOC(memberslen, sizeof(long) * *memberc);

	for (i = 0; i < *memberc; i++) {
		RL_CALL(rl_btree_random_element, RL_OK, db, set, NULL, (void **)&member);
		if (!repeat) {
			if (contains(i, used_members, *member)) {
				i--;
				continue;
			}
			else {
				used_members[i] = *member;
			}
		}
		RL_CALL(rl_multi_string_get, RL_OK, db, *member, &members[i], &memberslen[i]);
	}
	*_members = members;
	*_memberslen = memberslen;
cleanup:
	if (retval != RL_OK) {
		rl_free(members);
		rl_free(memberslen);
	}
	rl_free(used_members);
	return retval;
}

int rl_spop(struct rlite *db, const unsigned char *key, long keylen, unsigned char **member, long *memberlen)
{
	int retval;
	long set_page_number, *member_page;
	unsigned char *digest;
	rl_btree *set;
	RL_CALL(rl_set_get_objects, RL_OK, db, key, keylen, &set_page_number, &set, 1, 0);
	RL_CALL(rl_btree_random_element, RL_OK, db, set, (void **)&digest, (void **)&member_page);
	RL_CALL(rl_multi_string_get, RL_OK, db, *member_page, member, memberlen);
	rl_multi_string_delete(db, *member_page);
	retval = rl_btree_remove_element(db, set, set_page_number, digest);
	if (retval == RL_DELETED) {
		RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	}
	else if (retval != RL_OK) {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_sdiff(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen)
{
	int retval, found;
	rl_btree *source = NULL;
	rl_btree **sets = NULL;
	rl_btree_iterator *iterator;
	unsigned char **members = NULL, *digest;
	long *memberslen = NULL, i, member_page, setsc = 0;
	long membersc = 0;
	void *tmp;

	if (keyc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	RL_CALL(rl_set_get_objects, RL_OK, db, keys[0], keyslen[0], NULL, &source, 0, 0);
	RL_MALLOC(members, sizeof(unsigned char *) * source->number_of_elements);
	RL_MALLOC(memberslen, sizeof(long) * source->number_of_elements);
	RL_MALLOC(sets, sizeof(rl_btree *) * (keyc - 1));
	for (i = 1; i < keyc; i++) {
		retval = rl_set_get_objects(db, keys[i], keyslen[i], NULL, &sets[setsc], 0, 0);
		if (retval == RL_OK) {
			setsc++;
		}
		else if (retval != RL_NOT_FOUND) {
			goto cleanup;
		}
	}

	RL_CALL(rl_btree_iterator_create, RL_OK, db, source, &iterator);
	while ((retval = rl_btree_iterator_next(iterator, (void **)&digest, &tmp)) == RL_OK) {
		found = 0;
		for (i = 0; i < setsc; i++) {
			retval = rl_btree_find_score(db, sets[i], digest, NULL, NULL, NULL);
			if (retval == RL_FOUND) {
				found = 1;
				break;
			}
		}
		if (!found) {
			member_page = *(long *)tmp;
			RL_CALL(rl_multi_string_get, RL_OK, db, member_page, &members[membersc], &memberslen[membersc]);
			membersc++;
		}
		rl_free(digest);
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	if (membersc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	else if (membersc == source->number_of_elements) {
		*_members = members;
		*_memberslen = memberslen;
	}
	else {
		*_members = rl_realloc(members, sizeof(unsigned char *) * membersc);
		if (*_members == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*_memberslen = rl_realloc(memberslen, sizeof(long) * membersc);
		if (*_memberslen == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
	}

	retval = RL_OK;
cleanup:
	*_membersc = membersc;
	if (retval != RL_OK) {
		rl_free(members);
		rl_free(memberslen);
	}
	rl_free(sets);
	return retval;
}

int rl_sdiffstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added)
{
	int retval;
	unsigned char **members = NULL;
	long *memberslen = NULL, membersc = 0, i;

	retval = rl_key_delete_with_value(db, target, targetlen);
	if (retval != RL_NOT_FOUND && retval != RL_OK) {
		goto cleanup;
	}
	*added = 0;
	// this might be done more efficiently since we don't really need to have all members in memory
	// at the same time, but this is so easy to do...
	RL_CALL(rl_sdiff, RL_OK, db, keyc, keys, keyslen, &membersc, &members, &memberslen);
	if (membersc > 0) {
		RL_CALL(rl_sadd, RL_OK, db, target, targetlen, membersc, members, memberslen, added);
	}
cleanup:
	for (i = 0; i < membersc; i++) {
		rl_free(members[i]);
	}
	rl_free(members);
	rl_free(memberslen);
	return retval;
}

int rl_sinter(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen)
{
	int retval, found;
	rl_btree **sets = NULL;
	rl_btree_iterator *iterator;
	unsigned char **members = NULL, *digest;
	long *memberslen = NULL, i, member_page;
	long membersc = 0, maxmemberc = 0;
	void *tmp;

	if (keyc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	RL_MALLOC(sets, sizeof(rl_btree *) * keyc);
	for (i = 0; i < keyc; i++) {
		retval = rl_set_get_objects(db, keys[i], keyslen[i], NULL, &sets[i], 0, 0);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (i == 0 || sets[i]->number_of_elements < maxmemberc) {
			maxmemberc = sets[i]->number_of_elements;
			if (i != 0) {
				tmp = sets[i];
				sets[i] = sets[0];
				sets[0] = tmp;
			}
		}
	}

	if (maxmemberc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	RL_MALLOC(members, sizeof(unsigned char *) * maxmemberc);
	RL_MALLOC(memberslen, sizeof(long) * maxmemberc);

	RL_CALL(rl_btree_iterator_create, RL_OK, db, sets[0], &iterator);
	while ((retval = rl_btree_iterator_next(iterator, (void **)&digest, &tmp)) == RL_OK) {
		found = 1;
		for (i = 1; i < keyc; i++) {
			retval = rl_btree_find_score(db, sets[i], digest, NULL, NULL, NULL);
			if (retval == RL_NOT_FOUND) {
				found = 0;
				break;
			}
		}
		if (found) {
			member_page = *(long *)tmp;
			RL_CALL(rl_multi_string_get, RL_OK, db, member_page, &members[membersc], &memberslen[membersc]);
			membersc++;
		}
		rl_free(digest);
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	*_membersc = membersc;
	if (membersc == maxmemberc) {
		*_members = members;
		*_memberslen = memberslen;
	}
	else {
		*_members = rl_realloc(members, sizeof(unsigned char *) * membersc);
		if (*_members == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*_memberslen = rl_realloc(memberslen, sizeof(long) * membersc);
		if (*_memberslen == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
	}

	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		*_membersc = 0;
		rl_free(members);
		rl_free(memberslen);
	}
	rl_free(sets);
	return retval;
}

int rl_sinterstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added)
{
	int retval;
	unsigned char **members = NULL;
	long *memberslen = NULL, membersc = 0, i;

	retval = rl_key_delete_with_value(db, target, targetlen);
	if (retval != RL_NOT_FOUND && retval != RL_OK) {
		goto cleanup;
	}
	*added = 0;
	// this might be done more efficiently since we don't really need to have all members in memory
	// at the same time, but this is so easy to do...
	RL_CALL(rl_sinter, RL_OK, db, keyc, keys, keyslen, &membersc, &members, &memberslen);
	if (membersc > 0) {
		RL_CALL(rl_sadd, RL_OK, db, target, targetlen, membersc, members, memberslen, added);
	}
cleanup:
	for (i = 0; i < membersc; i++) {
		rl_free(members[i]);
	}
	rl_free(members);
	rl_free(memberslen);
	return retval;
}

int rl_sunion(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen)
{
	int retval, found;
	rl_btree **sets = NULL;
	rl_btree_iterator *iterator;
	unsigned char **members = NULL;
	long *memberslen = NULL, i, j, member_page;
	long membersc = 0, maxmemberc = 0;
	void *tmp;

	if (keyc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	RL_MALLOC(sets, sizeof(rl_btree *) * keyc);
	for (i = 0; i < keyc; i++) {
		retval = rl_set_get_objects(db, keys[i], keyslen[i], NULL, &sets[i], 0, 0);
		if (retval == RL_NOT_FOUND) {
			sets[i] = NULL;
		}
		else if (retval != RL_OK) {
			goto cleanup;
		}
		else {
			maxmemberc += sets[i]->number_of_elements;
		}
	}

	if (maxmemberc == 0) {
		*_membersc = 0;
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	RL_MALLOC(members, sizeof(unsigned char *) * maxmemberc);
	RL_MALLOC(memberslen, sizeof(long) * maxmemberc);

	for (i = 0; i < keyc; i++) {
		if (!sets[i]) {
			continue;
		}
		RL_CALL(rl_btree_iterator_create, RL_OK, db, sets[i], &iterator);

		while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
			member_page = *(long *)tmp;
			rl_free(tmp);
			RL_CALL(rl_multi_string_get, RL_OK, db, member_page, &members[membersc], &memberslen[membersc]);
			found = 0;
			for (j = 0; j < membersc; j++) {
				if (memberslen[membersc] == memberslen[j] && memcmp(members[membersc], members[j], memberslen[membersc]) == 0) {
					found = 1;
					break;
				}
			}
			if (found) {
				rl_free(members[membersc]);
			}
			else {
				membersc++;
			}
		}
		iterator = NULL;

		if (retval != RL_END) {
			goto cleanup;
		}
	}

	*_membersc = membersc;
	if (membersc == maxmemberc) {
		*_members = members;
		*_memberslen = memberslen;
	}
	else {
		*_members = rl_realloc(members, sizeof(unsigned char *) * membersc);
		if (*_members == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		*_memberslen = rl_realloc(memberslen, sizeof(long) * membersc);
		if (*_memberslen == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
	}

	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(members);
		rl_free(memberslen);
	}
	rl_free(sets);
	return retval;
}

int rl_sunionstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added)
{
	int retval;
	rl_btree *target_set = NULL;
	rl_btree *set = NULL;
	rl_btree_iterator *iterator;
	unsigned char *digest = NULL;
	unsigned char *member;
	long memberlen, i, member_page;
	long target_page_number;
	void *tmp;
	long *member_object = NULL;
	long count = 0;

	*added = 0;
	retval = rl_key_delete_with_value(db, target, targetlen);
	if (retval != RL_NOT_FOUND && retval != RL_OK) {
		goto cleanup;
	}
	if (keyc == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	RL_CALL(rl_set_get_objects, RL_OK, db, target, targetlen, &target_page_number, &target_set, 0, 1);

	for (i = 0; i < keyc; i++) {
		retval = rl_set_get_objects(db, keys[i], keyslen[i], NULL, &set, 0, 0);
		if (retval == RL_NOT_FOUND) {
			continue;
		}
		else if (retval != RL_OK) {
			goto cleanup;
		}

		RL_CALL(rl_btree_iterator_create, RL_OK, db, set, &iterator);
		while ((retval = rl_btree_iterator_next(iterator, (void **)&digest, &tmp)) == RL_OK) {
			member_page = *(long *)tmp;
			rl_free(tmp);

			retval = rl_btree_find_score(db, target_set, digest, &tmp, NULL, NULL);
			if (retval == RL_NOT_FOUND) {
				RL_CALL(rl_multi_string_get, RL_OK, db, member_page, &member, &memberlen);
				RL_MALLOC(member_object, sizeof(*member_object));
				RL_CALL(rl_multi_string_set, RL_OK, db, member_object, member, memberlen);

				retval = rl_btree_add_element(db, target_set, target_page_number, digest, member_object);
				if (retval != RL_OK) {
					goto cleanup;
				}
				count++;
				rl_free(member);
			}
			else if (retval == RL_FOUND) {
				rl_free(digest);
				digest = NULL;
			}
			else {
				goto cleanup;
			}
		}
		iterator = NULL;

		if (retval != RL_END) {
			goto cleanup;
		}
	}

	if (count == 0) {
		RL_CALL(rl_key_delete_with_value, RL_OK, db, target, targetlen);
	}
	*added = count;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_set_pages(struct rlite *db, long page, short *pages)
{
	rl_btree *btree;
	rl_btree_iterator *iterator = NULL;
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

int rl_set_delete(rlite *db, long value_page)
{
	rl_btree *hash;
	rl_btree_iterator *iterator;
	long member;
	int retval;
	void *tmp;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_long, value_page, &rl_btree_type_hash_sha1_long, &tmp, 1);
	hash = tmp;
	if (hash->number_of_elements) {
		RL_CALL2(rl_btree_iterator_create, RL_OK, RL_NOT_FOUND, db, hash, &iterator);
		if (retval == RL_OK) {
			while ((retval = rl_btree_iterator_next(iterator, NULL, &tmp)) == RL_OK) {
				member = *(long *)tmp;
				rl_multi_string_delete(db, member);
				rl_free(tmp);
			}
			iterator = NULL;

			if (retval != RL_END) {
				goto cleanup;
			}
		}
	}

	RL_CALL(rl_btree_delete, RL_OK, db, hash);
	RL_CALL(rl_delete, RL_OK, db, value_page);
	retval = RL_OK;
cleanup:
	return retval;
}
