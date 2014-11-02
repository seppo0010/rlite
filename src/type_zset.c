#include <stdlib.h>
#include "rlite.h"
#include "obj_key.h"
#include "obj_string.h"
#include "type_zset.h"
#include "page_btree.h"
#include "page_list.h"
#include "util.h"

static int rl_zset_create(rlite *db, long levels_page_number, rl_btree **btree)
{
	rl_list *levels;
	rl_btree *scores;
	long scores_page_number;
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
	retval = rl_list_create(db, &levels, &list_long);
	if (retval != RL_OK) {
		goto cleanup;
	}
	long *element = malloc(sizeof(long));
	*element = scores_page_number;
	retval = rl_list_add_element(db, levels, element, 0);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_write(db, &rl_data_type_list_long, levels_page_number, levels);
	if (retval != RL_OK) {
		goto cleanup;
	}
	if (btree) {
		*btree = scores;
	}
cleanup:
	return retval;
}

static int rl_zset_read(rlite *db, long levels_page_number, rl_btree **btree)
{
	void *tmp;
	long scores_page_number;
	rl_list *levels;
	int retval = rl_read(db, &rl_data_type_list_long, levels_page_number, &list_long, &tmp);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	levels = tmp;
	retval = rl_list_get_element(db, levels, &tmp, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	scores_page_number = *(long *)tmp;
	retval = rl_read(db, &rl_data_type_btree_hash_md5_double, scores_page_number, &rl_btree_type_hash_md5_double, &tmp);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	if (btree) {
		*btree = tmp;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

static int rl_zset_get_objects(rlite *db, unsigned char *key, long keylen, rl_btree **btree, int create)
{
	long levels_page_number;
	int retval;
	if (create) {
		retval = rl_obj_key_get_or_create(db, key, keylen, RL_TYPE_ZSET, &levels_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_zset_create(db, levels_page_number, btree);
		}
		else {
			retval = rl_zset_read(db, levels_page_number, btree);
		}
	}
	else {
		unsigned char type;
		retval = rl_obj_key_get(db, key, keylen, &type, &levels_page_number);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_ZSET) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		retval = rl_zset_read(db, levels_page_number, btree);
	}
cleanup:
	return retval;
}

int rl_zadd(rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen)
{
	rl_btree *scores;
	int retval = rl_zset_get_objects(db, key, keylen, &scores, 1);
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
cleanup:
	return retval;
}

int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, double *score)
{
	unsigned char *digest = NULL;
	rl_btree *scores;
	int retval = rl_zset_get_objects(db, key, keylen, &scores, 0);
	if (retval != RL_OK) {
		goto cleanup;
	}
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
