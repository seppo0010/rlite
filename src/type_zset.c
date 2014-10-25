#include <stdlib.h>
#include "rlite.h"
#include "obj_key.h"
#include "obj_string.h"
#include "type_zset.h"
#include "page_btree.h"
#include "page_list.h"
#include "util.h"

static int rl_zset_get_scores_btree(rlite *db, unsigned char *key, long keylen, rl_btree **btree, int create)
{
	long scores_page_number, levels_page_number;
	rl_list *levels;
	rl_btree *scores = NULL;
	int retval;
	void *tmp;
	if (create) {
		retval = rl_obj_key_get_or_create(db, key, keylen, RL_TYPE_ZSET, &levels_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			long max_node_size = (db->page_size - 8) / 24;
			retval = rl_btree_create(db, &scores, &hash_md5_double, max_node_size);
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
		}
		else {
			retval = rl_read(db, &rl_data_type_list_long, levels_page_number, &list_long, &tmp);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			levels = tmp;
			retval = rl_list_get_element(db, levels, &tmp, 0);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			scores_page_number = *(long *)tmp;
			retval = rl_read(db, &rl_data_type_btree_hash_md5_double, scores_page_number, &hash_md5_double, &tmp);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			scores = tmp;
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
		retval = rl_read(db, &rl_data_type_list_long, levels_page_number, &list_long, &tmp);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		levels = tmp;
		retval = rl_list_get_element(db, levels, &tmp, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		scores_page_number = *(long *)tmp;
		retval = rl_read(db, &rl_data_type_btree_hash_md5_double, scores_page_number, &hash_md5_double, &tmp);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		scores = tmp;
	}
	if (scores == NULL) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	*btree = scores;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zadd(rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen)
{
	rl_btree *scores;
	int retval = rl_zset_get_scores_btree(db, key, keylen, &scores, 1);
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
	int retval = rl_zset_get_scores_btree(db, key, keylen, &scores, 0);
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
