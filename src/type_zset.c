#include <math.h>
#include <stdlib.h>
#include "rlite.h"
#include "page_key.h"
#include "page_multi_string.h"
#include "type_zset.h"
#include "page_btree.h"
#include "page_list.h"
#include "page_skiplist.h"
#include "util.h"

static int update_zset(rlite *db, rl_btree *scores, long scores_page, rl_skiplist *skiplist, long skiplist_page)
{
	int retval;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_btree_hash_sha1_double, scores_page, scores);
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);
cleanup:
	return retval;
}

static int rl_zset_create(rlite *db, long levels_page_number, rl_btree **btree, long *btree_page, rl_skiplist **_skiplist, long *skiplist_page)
{
	rl_list *levels;
	rl_btree *scores = NULL;
	rl_skiplist *skiplist = NULL;
	long scores_page_number;
	long skiplist_page_number;

	long max_node_size = (db->page_size - 8) / 24;
	int retval;
	RL_CALL(rl_btree_create, RL_OK, db, &scores, &rl_btree_type_hash_sha1_double, max_node_size);
	scores_page_number = db->next_empty_page;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_btree_hash_sha1_double, scores_page_number, scores);
	RL_CALL(rl_skiplist_create, RL_OK, db, &skiplist);
	skiplist_page_number = db->next_empty_page;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page_number, skiplist);
	RL_CALL(rl_list_create, RL_OK, db, &levels, &list_long);
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_list_long, levels_page_number, levels);

	long *scores_element;
	RL_MALLOC(scores_element, sizeof(long));
	*scores_element = scores_page_number;
	RL_CALL(rl_list_add_element, RL_OK, db, levels, scores_element, 0);

	long *skiplist_element;
	RL_MALLOC(skiplist_element, sizeof(long))
	*skiplist_element = skiplist_page_number;
	RL_CALL(rl_list_add_element, RL_OK, db, levels, skiplist_element, 1);

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
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, levels_page_number, &list_long, &tmp, 1);
	levels = tmp;
	if (btree || btree_page) {
		RL_CALL(rl_list_get_element, RL_FOUND, db, levels, &tmp, 0);
		scores_page_number = *(long *)tmp;
		if (btree) {
			RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_sha1_double, scores_page_number, &rl_btree_type_hash_sha1_double, &tmp, 1);
			*btree = tmp;
		}
		if (btree_page) {
			*btree_page = scores_page_number;
		}
	}
	if (skiplist || skiplist_page) {
		RL_CALL(rl_list_get_element, RL_FOUND, db, levels, &tmp, 1);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		skiplist_page_number = *(long *)tmp;
		if (skiplist) {
			RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_skiplist, skiplist_page_number, NULL, &tmp, 1);
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
		retval = rl_key_get(db, key, keylen, &type, NULL, &levels_page_number);
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

static int add_member(rlite *db, rl_btree *scores, rl_skiplist *skiplist, double score, unsigned char *member, long memberlen)
{
	int retval;
	unsigned char *digest = NULL;
	void *value_ptr;
	RL_MALLOC(value_ptr, sizeof(double));
	digest = rl_malloc(sizeof(unsigned char) * 20);
	if (!digest) {
		rl_free(value_ptr);
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	*(double *)value_ptr = score;
	retval = sha1(member, memberlen, digest);
	if (retval != RL_OK) {
		rl_free(value_ptr);
		rl_free(digest);
		goto cleanup;
	}
	RL_CALL(rl_btree_add_element, RL_OK, db, scores, digest, value_ptr);

	retval = rl_skiplist_add(db, skiplist, score, member, memberlen);
	if (retval != RL_OK) {
		// This failure is critical. The btree already has the element, but
		// the skiplist failed to add it. If left as is, it would be in an
		// inconsistent state. Dropping all the transaction in progress.
		rl_discard(db);
		goto cleanup;
	}
cleanup:
	return retval;
}

int rl_zadd(rlite *db, unsigned char *key, long keylen, double score, unsigned char *member, long memberlen)
{
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	RL_CALL(add_member, RL_OK, db, scores, skiplist, score, member, memberlen);
	RL_CALL(update_zset, RL_OK, db, scores, scores_page, skiplist, skiplist_page);
cleanup:
	return retval;
}

static int rl_get_zscore(rlite *db, rl_btree *scores, unsigned char *member, long memberlen, double *score)
{
	unsigned char *digest = NULL;
	int retval;
	RL_MALLOC(digest, sizeof(unsigned char) * 20);
	RL_CALL(sha1, RL_OK, member, memberlen, digest);
	void *value;
	RL_CALL(rl_btree_find_score, RL_FOUND, db, scores, digest, &value, NULL, NULL);
	*score = *(double *)value;
	retval = RL_FOUND;
cleanup:
	rl_free(digest);
	return retval;
}

int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *member, long memberlen, double *score)
{
	rl_btree *scores;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, NULL, NULL, NULL, 0);
	RL_CALL(rl_get_zscore, RL_FOUND, db, scores, member, memberlen, score);
cleanup:
	return retval;
}

int rl_zrank(rlite *db, unsigned char *key, long keylen, unsigned char *member, long memberlen, long *rank)
{
	double score;
	rl_btree *scores;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, NULL, &skiplist, NULL, 0);
	RL_CALL(rl_get_zscore, RL_FOUND, db, scores, member, memberlen, &score);
	RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, score, RL_SKIPLIST_INCLUDE_SCORE, member, memberlen, NULL, rank);
cleanup:
	return retval;
}

int rl_zrevrank(rlite *db, unsigned char *key, long keylen, unsigned char *member, long memberlen, long *revrank)
{
	double score;
	rl_btree *scores;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, NULL, &skiplist, NULL, 0);
	RL_CALL(rl_get_zscore, RL_FOUND, db, scores, member, memberlen, &score);
	RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, score, RL_SKIPLIST_INCLUDE_SCORE, member, memberlen, NULL, revrank);
	*revrank = skiplist->size - (*revrank) - 1;
cleanup:
	return retval;
}

int rl_zcard(rlite *db, unsigned char *key, long keylen, long *card)
{
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	*card = skiplist->size;
cleanup:
	return RL_OK;
}

int rl_zcount(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long *count)
{
	rl_skiplist *skiplist;
	long maxrank, minrank;
	int retval;
	if (range->max < range->min) {
		*count = 0;
		retval = RL_OK;
		goto cleanup;
	}

	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);

	retval = rl_skiplist_first_node(db, skiplist, range->max, range->maxex ? RL_SKIPLIST_BEFORE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, NULL, &maxrank);
	if (retval == RL_NOT_FOUND) {
		maxrank = skiplist->size - 1;
	}
	else if (retval != RL_FOUND) {
		goto cleanup;
	}
	retval = rl_skiplist_first_node(db, skiplist, range->min, range->minex ? RL_SKIPLIST_EXCLUDE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, NULL, &minrank);
	if (retval == RL_NOT_FOUND) {
		minrank = skiplist->size;
	}
	else if (retval != RL_FOUND) {
		goto cleanup;
	}
	else if (minrank < 0) {
		minrank = 0;
	}

	*count = maxrank - minrank + 1;
	retval = RL_OK;
cleanup:
	return retval;
}

static int _rl_zrange(rlite *db, rl_skiplist *skiplist, long start, long end, int direction, rl_zset_iterator **iterator)
{
	int retval = RL_OK;
	long size, node_page;
	long card = skiplist->size;

	if (start < 0) {
		start += card;
		if (start < 0) {
			start = 0;
		}
	}
	if (end < 0) {
		end += card;
	}
	if (start > end || start >= card) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	if (end >= card) {
		end = card - 1;
	}

	size = end - start + 1;

	RL_CALL(rl_skiplist_node_by_rank, RL_OK, db, skiplist, direction > 0 ? start : end, NULL, &node_page);
	RL_CALL(rl_skiplist_iterator_create, RL_OK, db, iterator, skiplist, node_page, direction, size);
cleanup:
	return retval;
}

static int _rl_zrangebyscore(rlite *db, rl_skiplist *skiplist, rl_zrangespec *range, long *_start, long *_end)
{
	long start, end;
	rl_skiplist_node *node;
	int retval;
	RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, range->min, range->minex ? RL_SKIPLIST_EXCLUDE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, NULL, &start);
	retval = rl_skiplist_first_node(db, skiplist, range->max, range->maxex ? RL_SKIPLIST_BEFORE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, &node, &end);
	if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
		goto cleanup;
	}

	if (retval == RL_FOUND && end == 0 && (range->maxex || node->score > range->max)) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	if (end < start) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	*_start = start;
	*_end = end;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zrangebyscore(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long offset, long count, rl_zset_iterator **iterator)
{
	long start, end;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(_rl_zrangebyscore, RL_OK, db, skiplist, range, &start, &end);

	start += offset;

	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, iterator);
	if (count > 0 && (*iterator)->size > count) {
		(*iterator)->size = count;
	}
cleanup:
	return retval;
}

int rl_zrevrangebyscore(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long offset, long count, rl_zset_iterator **iterator)
{
	long start, end;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(_rl_zrangebyscore, RL_OK, db, skiplist, range, &start, &end);

	end -= offset;

	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, -1, iterator);
	if (count > 0 && (*iterator)->size > count) {
		(*iterator)->size = count;
	}
cleanup:
	return retval;
}

static int lex_get_range(rlite *db, unsigned char *min, long minlen, unsigned char *max, long maxlen, rl_skiplist *skiplist, long *_start, long *_end)
{
	if (min[0] != '-' && min[0] != '(' && min[0] != '[') {
		return RL_UNEXPECTED;
	}

	if (max[0] != '+' && max[0] != '(' && max[0] != '[') {
		return RL_UNEXPECTED;
	}

	rl_skiplist_node *node;
	double score;
	int retval;
	RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, -INFINITY, RL_SKIPLIST_EXCLUDE_SCORE, NULL, 0, &node, NULL);
	score = node->score;

	long start, end;
	if (min[0] == '-') {
		start = 0;
	}
	else {
		RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, score, min[0] == '(' ? RL_SKIPLIST_EXCLUDE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, &min[1], minlen - 1, &node, &start);
	}

	if (max[0] == '+') {
		end = -1;
	}
	else {
		RL_CALL(rl_skiplist_first_node, RL_FOUND, db, skiplist, score, max[0] == '(' ? RL_SKIPLIST_BEFORE_SCORE : RL_SKIPLIST_INCLUDE_SCORE, &max[1], maxlen - 1, NULL, &end);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (end < 0) {
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
	}

	if (_start) {
		*_start = start;
	}
	if (_end) {
		*_end = end;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zlexcount(rlite *db, unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long *lexcount)
{
	long start, end;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(lex_get_range, RL_OK, db, min, minlen, max, maxlen, skiplist, &start, &end);
	if (end < 0) {
		end += skiplist->size;
	}

	*lexcount = end - start + 1;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zrevrangebylex(rlite *db, unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long offset, long count, rl_zset_iterator **iterator)
{
	long start, end;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(lex_get_range, RL_OK, db, min, minlen, max, maxlen, skiplist, &start, &end);

	end -= offset;

	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, -1, iterator);
	if (count > 0 && (*iterator)->size > count) {
		(*iterator)->size = count;
	}
cleanup:
	return retval;
}

int rl_zrangebylex(rlite *db, unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long offset, long count, rl_zset_iterator **iterator)
{
	long start, end;
	rl_skiplist *skiplist;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(lex_get_range, RL_OK, db, min, minlen, max, maxlen, skiplist, &start, &end);

	start += offset;

	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, iterator);
	if (count > 0 && (*iterator)->size > count) {
		(*iterator)->size = count;
	}
cleanup:
	return retval;
}

int rl_zrevrange(rlite *db, unsigned char *key, long keylen, long start, long end, rl_zset_iterator **iterator)
{
	rl_skiplist *skiplist;

	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, -1, iterator);
cleanup:
	return retval;
}

int rl_zrange(rlite *db, unsigned char *key, long keylen, long start, long end, rl_zset_iterator **iterator)
{
	rl_skiplist *skiplist;

	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, NULL, NULL, &skiplist, NULL, 0);
	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, iterator);
cleanup:
	return retval;
}

int rl_zset_iterator_next(rl_zset_iterator *iterator, double *score, unsigned char **member, long *memberlen)
{
	if ((!member && memberlen) || (member && !memberlen)) {
		fprintf(stderr, "Expected to receive either member and memberlen or neither\n");
		return RL_UNEXPECTED;
	}

	rl_skiplist_node *node;
	int retval;
	RL_CALL(rl_skiplist_iterator_next, RL_OK, iterator, &node);

	if (member) {
		retval = rl_multi_string_get(iterator->db, node->value, member, memberlen);
		if (retval != RL_OK) {
			rl_skiplist_iterator_destroy(iterator->db, iterator);
			goto cleanup;
		}
	}

	if (score) {
		*score = node->score;
	}
cleanup:
	return retval;
}

int rl_zset_iterator_destroy(rl_zset_iterator *iterator)
{
	return rl_skiplist_iterator_destroy(iterator->db, iterator);
}

static int delete_zset(rlite *db, unsigned char *key, long keylen, rl_skiplist *skiplist, long skiplist_page, rl_btree *scores, long scores_page)
{
	int retval;
	if (skiplist->size == 0) {
		RL_CALL(rl_delete, RL_OK, db, skiplist_page);
		RL_CALL(rl_skiplist_destroy, RL_OK, db, skiplist);
		RL_CALL(rl_delete, RL_OK, db, scores_page);
		RL_CALL(rl_btree_destroy, RL_OK, db, scores);
		RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	}
	else {
		retval = RL_NOT_IMPLEMENTED;
	}
cleanup:
	return retval;
}

static int remove_member_score_sha1(rlite *db, rl_btree *scores, rl_skiplist *skiplist, unsigned char *member, long member_len, double score, unsigned char digest[20])
{
	int retval;
	RL_CALL(rl_btree_remove_element, RL_OK, db, scores, digest);
	RL_CALL(rl_skiplist_delete, RL_OK, db, skiplist, score, member, member_len);
	retval = RL_OK;
cleanup:
	return retval;
}

static int remove_member_score(rlite *db, rl_btree *scores, rl_skiplist *skiplist, unsigned char *member, long member_len, double score)
{
	unsigned char digest[20];
	int retval;
	RL_CALL(sha1, RL_OK, member, member_len, digest);
	RL_CALL(remove_member_score_sha1, RL_OK, db, scores, skiplist, member, member_len, score, digest);
cleanup:
	return retval;
}

static int remove_member(rlite *db, rl_btree *scores, rl_skiplist *skiplist, unsigned char *member, long member_len)
{
	double score;
	void *tmp;
	unsigned char digest[20];
	int retval;
	RL_CALL(sha1, RL_OK, member, member_len, digest);
	retval = rl_btree_find_score(db, scores, digest, &tmp, NULL, NULL);
	if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	if (retval == RL_FOUND) {
		score = *(double *)tmp;
		RL_CALL(remove_member_score_sha1, RL_OK, db, scores, skiplist, member, member_len, score, digest);
	}
cleanup:
	return retval;
}
static int skiplist_changed(rlite *db, unsigned char *key, long keylen, rl_skiplist *skiplist, long skiplist_page, rl_btree *scores, long scores_page, long changed)
{
	int retval;
	if (skiplist->size == 0) {
		RL_CALL(delete_zset, RL_OK, db, key, keylen, skiplist, skiplist_page, scores, scores_page);
	}
	else if (changed) {
		RL_CALL(update_zset, RL_OK, db, scores, scores_page, skiplist, skiplist_page);
	}

	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zrem(rlite *db, unsigned char *key, long keylen, long members_size, unsigned char **members, long *members_len, long *changed)
{
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	long i;
	long _changed = 0;
	for (i = 0; i < members_size; i++) {
		retval = remove_member(db, scores, skiplist, members[i], members_len[i]);
		if (retval != RL_OK && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_OK) {
			_changed++;
		}
	}

	RL_CALL(skiplist_changed, RL_OK, db, key, keylen, skiplist, skiplist_page, scores, scores_page, _changed);

	*changed = _changed;
	retval = RL_OK;
cleanup:
	return retval;
}

static int _zremiterator(rlite *db, unsigned char *key, long keylen, rl_zset_iterator *iterator, rl_btree *scores, long scores_page, rl_skiplist *skiplist, long skiplist_page, long *changed)
{
	long _changed = 0;
	double score;
	unsigned char *member;
	long memberlen;
	int retval;
	while ((retval = rl_zset_iterator_next(iterator, &score, &member, &memberlen)) == RL_OK) {
		retval = remove_member_score(db, scores, skiplist, member, memberlen, score);
		rl_free(member);
		if (retval != RL_OK) {
			rl_zset_iterator_destroy(iterator);
			goto cleanup;
		}
		_changed++;
	}

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(skiplist_changed, RL_OK, db, key, keylen, skiplist, skiplist_page, scores, scores_page, _changed);

	*changed = _changed;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zremrangebyrank(rlite *db, unsigned char *key, long keylen, long start, long end, long *changed)
{
	rl_zset_iterator *iterator;
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, &iterator);
	RL_CALL(_zremiterator, RL_OK, db, key, keylen, iterator, scores, scores_page, skiplist, skiplist_page, changed);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zremrangebyscore(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long *changed)
{
	rl_zset_iterator *iterator;
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);

	long start, end;
	RL_CALL(_rl_zrangebyscore, RL_OK, db, skiplist, range, &start, &end);
	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, &iterator);
	RL_CALL(_zremiterator, RL_OK, db, key, keylen, iterator, scores, scores_page, skiplist, skiplist_page, changed);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_zremrangebylex(rlite *db, unsigned char *key, long keylen, unsigned char *min, long minlen, unsigned char *max, long maxlen, long *changed)
{
	rl_zset_iterator *iterator;
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page, start, end;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	retval = lex_get_range(db, min, minlen, max, maxlen, skiplist, &start, &end);
	if (retval == RL_NOT_FOUND) {
		*changed = 0;
		retval = RL_OK;
		goto cleanup;
	}
	if (retval != RL_OK) {
		goto cleanup;
	}

	RL_CALL(_rl_zrange, RL_OK, db, skiplist, start, end, 1, &iterator);
	RL_CALL(_zremiterator, RL_OK, db, key, keylen, iterator, scores, scores_page, skiplist, skiplist_page, changed);
	retval = RL_OK;
cleanup:
	return retval;
}



static int incrby(rlite *db, rl_btree *scores, rl_skiplist *skiplist, unsigned char *member, long memberlen, double score, double *newscore)
{
	double existing_score = 0.0;
	int retval = rl_get_zscore(db, scores, member, memberlen, &existing_score);
	if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
		goto cleanup;
	}
	else if (retval == RL_FOUND) {
		score += existing_score;
		RL_CALL(remove_member, RL_OK, db, scores, skiplist, member, memberlen);
	}

	RL_CALL(add_member, RL_OK, db, scores, skiplist, score, member, memberlen);
	if (newscore) {
		*newscore = score;
	}
cleanup:
	return retval;
}
int rl_zincrby(rlite *db, unsigned char *key, long keylen, double score, unsigned char *member, long memberlen, double *newscore)
{
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, key, keylen, &scores, &scores_page, &skiplist, &skiplist_page, 1);
	RL_CALL(incrby, RL_OK, db, scores, skiplist, member, memberlen, score, newscore);
	RL_CALL(update_zset, RL_OK, db, scores, scores_page, skiplist, skiplist_page);
cleanup:
	return retval;
}

int rl_zinterstore(rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *_weights, int aggregate)
{
	unsigned char *member = NULL;
	rl_btree **btrees = NULL;
	rl_skiplist **skiplists = NULL;
	double weight = 1.0, weight_tmp;
	double *weights = NULL;
	rl_skiplist_node *node;
	rl_skiplist_iterator *iterator = NULL;
	int retval;

	if (keys_size > 1) {
		RL_MALLOC(btrees, sizeof(rl_btree *) * (keys_size - 1));
		RL_MALLOC(skiplists, sizeof(rl_btree *) * (keys_size - 1));
	}
	else {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_btree *btree, *btree_tmp;
	rl_skiplist *skiplist = NULL, *skiplist_tmp;
	long i;
	// key in position 0 is the target key
	// we'll store a pivot skiplist in btree/skiplist and the others in btrees/skiplists
	RL_MALLOC(weights, sizeof(double) * (keys_size - 2));
	for (i = 1; i < keys_size; i++) {
		weight_tmp = _weights ? _weights[i - 1] : 1.0;
		RL_CALL(rl_zset_get_objects, RL_OK, db, keys[i], keys_len[i], &btree_tmp, NULL, &skiplist_tmp, NULL, 0);
		if (i == 1) {
			btree = btree_tmp;
			skiplist = skiplist_tmp;
			weight = weight_tmp;
		}
		else if (btree_tmp->number_of_elements < btree->number_of_elements) {
			weights[i - 2] = weight;
			weight = weight_tmp;
			btrees[i - 2] = btree;
			btree = btree_tmp;
			skiplists[i - 2] = skiplist;
			skiplist = skiplist_tmp;
		}
		else {
			weights[i - 2] = weight_tmp;
			btrees[i - 2] = btree_tmp;
			skiplists[i - 2] = skiplist_tmp;
		}
	}
	RL_CALL(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, 0, 0, 0);

	long target_btree_page, target_skiplist_page;
	rl_btree *target_btree;
	rl_skiplist *target_skiplist;
	RL_CALL(rl_zset_get_objects, RL_OK, db, keys[0], keys_len[0], &target_btree, &target_btree_page, &target_skiplist, &target_skiplist_page, 1);

	int found;
	void *tmp;
	double skiplist_score, tmp_score;
	long memberlen;
	unsigned char digest[20];
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		found = 1;
		skiplist_score = node->score * weight;
		for (i = 1; i < keys_size - 1; i++) {
			RL_CALL(rl_multi_string_sha1, RL_OK, db, digest, node->value);

			retval = rl_btree_find_score(db, btrees[i - 1], digest, &tmp, NULL, NULL);
			if (retval == RL_NOT_FOUND) {
				found = 0;
				break;
			}
			else if (retval == RL_FOUND) {
				tmp_score = *(double *)tmp;
				if (aggregate == RL_ZSET_AGGREGATE_SUM) {
					skiplist_score += tmp_score * weights[i - 1];
				}
				else if (
				    (aggregate == RL_ZSET_AGGREGATE_MIN && tmp_score * weights[i - 1] < skiplist_score) ||
				    (aggregate == RL_ZSET_AGGREGATE_MAX && tmp_score * weights[i - 1] > skiplist_score)
				) {
					skiplist_score = tmp_score * weights[i - 1];
				}
			}
			else {
				goto cleanup;
			}
		}
		if (found) {
			RL_CALL(rl_multi_string_get, RL_OK, db, node->value, &member, &memberlen);
			RL_CALL(add_member, RL_OK, db, target_btree, target_skiplist, skiplist_score, member, memberlen);
			rl_free(member);
			member = NULL;
		}
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(update_zset, RL_OK, db, target_btree, target_btree_page, target_skiplist, target_skiplist_page);
cleanup:
	rl_free(member);
	if (iterator) {
		rl_zset_iterator_destroy(iterator);
	}
	rl_free(weights);
	rl_free(btrees);
	rl_free(skiplists);
	return retval;
}
static int zunionstore_minmax(rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *weights, rl_btree *target_scores, rl_skiplist *target_skiplist, int aggregate)
{
	int retval;
	rl_skiplist_iterator **iterators = NULL;;
	rl_skiplist *skiplist;
	double *scores = NULL;
	unsigned char **members = NULL;
	long *memberslen = NULL, i;
	long position;
	RL_MALLOC(scores, sizeof(double) * keys_size);
	RL_MALLOC(members, sizeof(unsigned char *) * keys_size);
	for (i = 0; i < keys_size; i++) {
		members[i] = NULL;
	}
	RL_MALLOC(memberslen, sizeof(long) * keys_size);
	RL_MALLOC(iterators, sizeof(rl_skiplist_iterator *) * keys_size);
	for (i = 0; i < keys_size; i++) {
		iterators[i] = NULL;
	}

	for (i = 0; i < keys_size; i++) {
		retval = rl_zset_get_objects(db, keys[i], keys_len[i], NULL, NULL, &skiplist, NULL, 0);
		if (retval == RL_NOT_FOUND) {
			iterators[i] = NULL;
			continue;
		}
		if (retval != RL_OK) {
			goto cleanup;
		}
		RL_CALL(rl_skiplist_iterator_create, RL_OK, db, &iterators[i], skiplist, 0, aggregate == RL_ZSET_AGGREGATE_MAX ? -1 : 1, 0);
		retval = rl_zset_iterator_next(iterators[i], &scores[i], &members[i], &memberslen[i]);
		if (retval != RL_OK) {
			iterators[i] = NULL;
			if (retval != RL_END) {
				goto cleanup;
			}
		}
	}

	while (1) {
		position = -1;
		for (i = 0; i < keys_size; i++) {
			if (!iterators[i]) {
				continue;
			}
			if ((position == -1) ||
			        (aggregate == RL_ZSET_AGGREGATE_MAX && scores[i] > (weights ? weights[position] * scores[position] : scores[position])) ||
			        (aggregate == RL_ZSET_AGGREGATE_MIN && scores[i] < (weights ? weights[position] * scores[position] : scores[position]))) {
				position = i;
			}
		}
		if (position == -1) {
			break;
		}

		retval = add_member(db, target_scores, target_skiplist, (weights ? weights[position] * scores[position] : scores[position]), members[position], memberslen[position]);
		if (retval != RL_FOUND && retval != RL_OK) {
			goto cleanup;
		}
		rl_free(members[position]);
		members[position] = NULL;

		retval = rl_zset_iterator_next(iterators[position], &scores[position], &members[position], &memberslen[position]);
		if (retval != RL_OK) {
			iterators[position] = NULL;
			if (retval != RL_END) {
				goto cleanup;
			}
		}
	}

	retval = RL_OK;
cleanup:
	if (iterators) {
		for (i = 0; i < keys_size; i++) {
			if (iterators[i]) {
				rl_zset_iterator_destroy(iterators[i]);
			}
			rl_free(members[i]);
		}
	}
	rl_free(iterators);
	rl_free(memberslen);
	rl_free(members);
	rl_free(scores);
	return retval;
}

static int zunionstore_sum(rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *weights, rl_btree *target_scores, rl_skiplist *target_skiplist)
{
	rl_skiplist_iterator *iterator = NULL;
	rl_skiplist *skiplist;
	double score;
	unsigned char *member;
	long memberlen;
	int retval;
	long i;
	for (i = 0; i < keys_size; i++) {
		retval = rl_zset_get_objects(db, keys[i], keys_len[i], NULL, NULL, &skiplist, NULL, 0);
		if (retval == RL_NOT_FOUND) {
			continue;
		}
		if (retval != RL_OK) {
			goto cleanup;
		}
		RL_CALL(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, 0, 1, 0);
		while ((retval = rl_zset_iterator_next(iterator, &score, &member, &memberlen)) == RL_OK) {
			retval = incrby(db, target_scores, target_skiplist, member, memberlen, weights ? score * weights[i] : score, NULL);
			rl_free(member);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
		iterator = NULL;

		if (retval != RL_END) {
			goto cleanup;
		}
	}
	retval = RL_OK;
cleanup:
	if (iterator) {
		rl_zset_iterator_destroy(iterator);
	}
	return retval;
}

int rl_zunionstore(rlite *db, long keys_size, unsigned char **keys, long *keys_len, double *weights, int aggregate)
{
	rl_btree *scores;
	rl_skiplist *skiplist;
	long scores_page, skiplist_page;
	int retval;
	RL_CALL(rl_zset_get_objects, RL_OK, db, keys[0], keys_len[0], &scores, &scores_page, &skiplist, &skiplist_page, 1);
	if (aggregate == RL_ZSET_AGGREGATE_SUM) {
		RL_CALL(zunionstore_sum, RL_OK, db, keys_size - 1, &keys[1], &keys_len[1], weights, scores, skiplist);
	}
	else {
		RL_CALL(zunionstore_minmax, RL_OK, db, keys_size - 1, &keys[1], &keys_len[1], weights, scores, skiplist, aggregate);
	}

	RL_CALL(update_zset, RL_OK, db, scores, scores_page, skiplist, skiplist_page);
cleanup:
	return retval;
}
