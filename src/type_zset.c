#include <stdlib.h>
#include "rlite.h"
#include "page_key.h"
#include "obj_string.h"
#include "type_zset.h"
#include "page_btree.h"
#include "page_list.h"
#include "page_skiplist.h"
#include "util.h"

static int rl_zset_create(rlite *db, long levels_page_number, rl_btree **btree, long *btree_page, rl_skiplist **_skiplist, long *skiplist_page)
{
	rl_list *levels;
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page_number;
	long skiplist_page_number;

	long max_node_size = (db->page_size - 8) / 24;
	int retval = rl_btree_create(db, &scores, &rl_btree_type_hash_md5_double, max_node_size);
	if (retval != RL_OK) {
		goto cleanup;
	}
	scores_page_number = db->next_empty_page;
	retval = rl_write(db, &rl_data_type_btree_hash_md5_double, scores_page_number, scores);
	if (retval != RL_OK) {
		goto cleanup;
	}

	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}
	skiplist_page_number = db->next_empty_page;
	retval = rl_write(db, &rl_data_type_skiplist, skiplist_page_number, skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	retval = rl_list_create(db, &levels, &list_long);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_write(db, &rl_data_type_list_long, levels_page_number, levels);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long *scores_element = malloc(sizeof(long));
	*scores_element = scores_page_number;
	retval = rl_list_add_element(db, levels, scores_element, 0);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long *skiplist_element = malloc(sizeof(long));
	*skiplist_element = skiplist_page_number;
	retval = rl_list_add_element(db, levels, skiplist_element, 1);
	if (retval != RL_OK) {
		goto cleanup;
	}

	if (btree) {
		*btree = scores;
	}
	if (btree_page) {
		*btree_page = scores_page_number;
	}
	if (_skiplist) {
		*_skiplist = skiplist;
	}
	if (skiplist_page) {
		*skiplist_page = skiplist_page_number;
	}
cleanup:
	return retval;
}

static int rl_zset_read(rlite *db, long levels_page_number, rl_btree **btree, long *btree_page, rl_skiplist **skiplist, long *skiplist_page)
{
	void *tmp;
	long scores_page_number, skiplist_page_number;
	rl_list *levels;
	int retval = rl_read(db, &rl_data_type_list_long, levels_page_number, &list_long, &tmp);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	levels = tmp;
	if (btree || btree_page) {
		retval = rl_list_get_element(db, levels, &tmp, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		scores_page_number = *(long *)tmp;
		if (btree) {
			retval = rl_read(db, &rl_data_type_btree_hash_md5_double, scores_page_number, &rl_btree_type_hash_md5_double, &tmp);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			*btree = tmp;
		}
		if (btree_page) {
			*btree_page = scores_page_number;
		}
	}
	if (skiplist || skiplist_page) {
		retval = rl_list_get_element(db, levels, &tmp, 1);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		skiplist_page_number = *(long *)tmp;
		if (skiplist) {
			retval = rl_read(db, &rl_data_type_skiplist, skiplist_page_number, NULL, &tmp);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			*skiplist = tmp;
		}
		if (skiplist_page) {
			*skiplist_page = skiplist_page_number;
		}
	}
	retval = RL_OK;
cleanup:
	return retval;
}

static int rl_zset_get_objects(rlite *db, unsigned char *key, long keylen, rl_btree **btree, long *btree_page, rl_skiplist **skiplist, long *skiplist_page, int create)
{
	long levels_page_number;
	int retval;
	if (create) {
		retval = rl_key_get_or_create(db, key, keylen, RL_TYPE_ZSET, &levels_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_zset_create(db, levels_page_number, btree, btree_page, skiplist, skiplist_page);
		}
		else {
			retval = rl_zset_read(db, levels_page_number, btree, btree_page, skiplist, skiplist_page);
		}
	}
	else {
		unsigned char type;
		retval = rl_key_get(db, key, keylen, &type, &levels_page_number);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_ZSET) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		retval = rl_zset_read(db, levels_page_number, btree, btree_page, skiplist, skiplist_page);
	}
cleanup:
	return retval;
}

int rl_zadd(rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen)
{
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval = rl_zset_get_objects(db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	if (retval != RL_OK) {
		goto cleanup;
	}
	void *value_ptr = malloc(sizeof(double));
	*(double *)value_ptr = score;
	unsigned char *digest = malloc(sizeof(unsigned char) * 16);
	retval = md5(data, datalen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_btree_add_element(db, scores, digest, value_ptr);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_skiplist_add(db, skiplist, score, data, datalen);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_write(db, &rl_data_type_btree_hash_md5_double, scores_page, scores);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_write(db, &rl_data_type_skiplist, skiplist_page, skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}
cleanup:
	return retval;
}

static int rl_get_zscore(rlite *db, rl_btree *scores, unsigned char *data, long datalen, double *score)
{
	unsigned char *digest = NULL;
	int retval;
	digest = malloc(sizeof(unsigned char) * 16);
	if (digest == NULL) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	retval = md5(data, datalen, digest);
	if (retval != RL_OK) {
		goto cleanup;
	}
	void *value;
	retval = rl_btree_find_score(db, scores, digest, &value, NULL, NULL);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	*score = *(double *)value;
	retval = RL_FOUND;
cleanup:
	free(digest);
	return retval;
}

int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, double *score)
{
	rl_btree *scores;
	int retval = rl_zset_get_objects(db, key, keylen, &scores, NULL, NULL, NULL, 0);
	if (retval != RL_OK) {
		return retval;
	}
	return rl_get_zscore(db, scores, data, datalen, score);
}

int rl_zrank(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, long *rank)
{
	double score;
	rl_btree *scores;
	rl_skiplist *skiplist;
	int retval = rl_zset_get_objects(db, key, keylen, &scores, NULL, &skiplist, NULL, 0);
	if (retval != RL_OK) {
		return retval;
	}
	retval = rl_get_zscore(db, scores, data, datalen, &score);
	if (retval != RL_FOUND) {
		return retval;
	}
	retval = rl_skiplist_first_node(db, skiplist, score, data, datalen, NULL, rank);
	if (retval != RL_FOUND) {
		return retval;
	}
	return retval;
}
