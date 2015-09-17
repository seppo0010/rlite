#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "greatest.h"
#include "util.h"
#include "../src/rlite.h"
#include "../src/rlite/page_btree.h"
#include "../src/rlite/status.h"

#define INIT()\
	rl_btree *btree = NULL;\
	int retval;\
	rlite *db = NULL;\
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);\
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);

#ifdef RL_DEBUG
#define CHECK_RETVAL(r)\
	retval = r;\
	if (retval == RL_OUT_OF_MEMORY) { break; }\
	EXPECT_INT(retval, RL_OK);

TEST btree_insert_oom()
{
	long btree_node_size = 2;
	rl_btree *btree = NULL;
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	long *key;
	long *val;

	long j, i;
	int finished = 0;
	for (j = 1; !finished; j++) {
		for (i = 0; i < 7; i++) {
			test_mode = 0;
			RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);
			long btree_page = db->next_empty_page;
			RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);
			test_mode = 1;
			test_mode_caller = "rl_btree_add_element";
			test_mode_counter = j;
			key = malloc(sizeof(long));
			val = malloc(sizeof(long));
			*key = i + 1;
			*val = i * 10;
			CHECK_RETVAL(rl_btree_add_element(db, btree, btree_page, key, val));
			if (i == 6 && retval == RL_OK) {
				if (j == 1) {
					fprintf(stderr, "No OOM triggered\n");
					test_mode = 0;
					FAIL();
				}
				finished = 1;
				break;
			}
		}
		test_mode = 0;
		rl_discard(db);
	}
	test_mode = 0;
	rl_close(db);
	PASS();
}
#endif

TEST basic_insert_hash_test()
{
	long btree_node_size = 2;
	INIT();
	long **keys = malloc(sizeof(long *) * 7);
	long **vals = malloc(sizeof(long *) * 7);

	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);
	long i;
	for (i = 0; i < 7; i++) {
		keys[i] = malloc(sizeof(long));
		vals[i] = malloc(sizeof(long));
		*keys[i] = i + 1;
		*vals[i] = i * 10;
		RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, keys[i], vals[i]);
		RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
	}

	void *val;
	for (i = 0; i < 7; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_FOUND, db, btree, keys[i], &val, NULL, NULL);
		EXPECT_PTR(val, vals[i]);
		EXPECT_LONG(*(long *)val, i * 10);
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_NOT_FOUND, db, btree, &nonexistent_vals[i], NULL, NULL, NULL);
	}
	retval = 0;
cleanup:
	free(vals);
	free(keys);
	rl_close(db);
	if (retval == 0) { PASS(); } else { FAIL(); }
}

TEST random_hash_test(long size, long btree_node_size)
{
	INIT();
	long *key, *val;
	int *results = malloc(sizeof(int) * size);
	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);
	long i;
	for (i = 0; i < size; i++) {
		results[i] = 0;
		key = malloc(sizeof(long));
		val = malloc(sizeof(long));
		*key = i;
		*val = i * 10;
		RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, key, val);
		RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
	}

	for (i = 0; i < size * 20; i++) {
		RL_CALL_VERBOSE(rl_btree_random_element, RL_OK, db, btree, (void **)&key, (void **)&val);
		EXPECT_LONG(10 * (*key), *val);
		results[*key]++;
	}

	for (i = 0; i < size; i++) {
		if (results[i] == 0) {
			fprintf(stderr, "key %ld was not randomly returned\n", i);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	}

	retval = 0;
cleanup:
	free(results);
	rl_close(db);
	if (retval == 0) { PASS(); } else { FAIL(); }
}

static int contains_element(long element, long *elements, long size)
{
	long i;
	for (i = 0; i < size; i++) {
		if (elements[i] == element) {
			return 1;
		}
	}
	return 0;
}

TEST fuzzy_hash_test(long size, long btree_node_size, int _commit)
{
	INIT();

	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	long *values = malloc(sizeof(long) * size);
	void **flatten_scores = malloc(sizeof(void *) * size);

	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long i, element, value, *element_copy, *value_copy;

	long j, flatten_size;

	void *val, *tmp;

	for (i = 0; i < size; i++) {
		element = rand();
		value = rand();
		if (contains_element(element, elements, i)) {
			i--;
			continue;
		}
		else {
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_long_long, btree_page, &rl_btree_type_hash_long_long, &tmp, 1);
			elements[i] = element;
			element_copy = malloc(sizeof(long));
			*element_copy = element;
			values[i] = value;
			value_copy = malloc(sizeof(long));
			*value_copy = value;
			RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, element_copy, value_copy);
			RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
		}
		flatten_size = 0;
		RL_CALL_VERBOSE(rl_flatten_btree, RL_OK, db, btree, &flatten_scores, &flatten_size);
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		}
		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_long_long, btree_page, &rl_btree_type_hash_long_long, &tmp, 1);
			btree = tmp;
		}
	}

	for (i = 0; i < size; i++) {
		element = rand();
		if (contains_element(element, elements, size) || contains_element(element, nonelements, i)) {
			i--;
		}
		else {
			nonelements[i] = element;
		}
	}

	for (i = 0; i < size; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_FOUND, db, btree, &elements[i], &val, NULL, NULL);
		EXPECT_LONG(*(long *)val, values[i]);
		RL_CALL_VERBOSE(rl_btree_find_score, RL_NOT_FOUND, db, btree, &nonelements[i], NULL, NULL, NULL);
	}

	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	rl_close(db);
	if (retval == 0) { PASS(); } else { FAIL(); }
}

TEST fuzzy_hash_test_iterator(long size, long btree_node_size, int _commit)
{
	INIT();

	rl_btree_iterator *iterator = NULL;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	long *values = malloc(sizeof(long) * size);

	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long i, element, value, *element_copy, *value_copy;

	long j;

	void *tmp;
	long prev_score = -1.0, score;

	for (i = 0; i < size; i++) {
		element = rand();
		value = rand();
		if (contains_element(element, elements, i)) {
			i--;
			continue;
		}
		else {
			elements[i] = element;
			element_copy = malloc(sizeof(long));
			*element_copy = element;
			values[i] = value;
			value_copy = malloc(sizeof(long));
			*value_copy = value;
			RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, element_copy, value_copy);
			RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
		}

		RL_CALL_VERBOSE(rl_btree_iterator_create, RL_OK, db, btree, &iterator);
		EXPECT_LONG(iterator->size, i + 1);

		j = 0;
		while (RL_OK == (retval = rl_btree_iterator_next(iterator, &tmp, NULL))) {
			score = *(long *)tmp;
			rl_free(tmp);
			if (j++ > 0) {
				if (prev_score >= score) {
					fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
					retval = RL_UNEXPECTED;
					goto cleanup;
				}
			}
			prev_score = score;
		}

		if (retval != RL_END) {
			rl_free(tmp);
			goto cleanup;
		}

		EXPECT_LONG(j, i + 1);
		iterator = NULL;

		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_hash_long_long, btree_page, &rl_btree_type_hash_long_long, &tmp, 1);
			btree = tmp;
		}
	}


	rl_btree_iterator_destroy(iterator);
	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	rl_close(db);
	if (retval == 0) { PASS(); } else { FAIL(); }
}


#define DELETE_TESTS_COUNT 7

SUITE(btree_test)
{
	int i, j, k;
	long size, btree_node_size;
	int commit;
	RUN_TEST(basic_insert_hash_test);
	RUN_TESTp(random_hash_test, 10, 2);
	RUN_TESTp(random_hash_test, 100, 10);
#ifdef RL_DEBUG
	RUN_TEST(btree_insert_oom);
#endif

	long delete_tests[DELETE_TESTS_COUNT][2] = {
		{8, 8},
		{ -8, -5},
		{8, 5},
		{8, 3},
		{ -8, -6},
		{8, 6},
		{15, 8},
	};
	char *delete_tests_name[DELETE_TESTS_COUNT] = {
		"delete leaf node, no rebalance",
		"delete leaf node, rebalance from sibling in the right",
		"delete leaf node, rebalance from sibling in the left",
		"delete leaf node, rebalance with merge, change root",
		"delete internal node, no rebalance",
		"delete internal node, rebalance leaf",
		"delete internal node, rebalance two levels",
	};
	for (i = 0; i < DELETE_TESTS_COUNT; i++) {
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			btree_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 3; k++) {
				commit = k;
				srand(1);
				RUN_TESTp(fuzzy_hash_test, size, btree_node_size, commit);
				RUN_TESTp(fuzzy_hash_test_iterator, size, btree_node_size, commit);
			}
		}
	}
}
