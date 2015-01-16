#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../page_btree.h"
#include "../status.h"

int basic_insert_set_test()
{
	fprintf(stderr, "Start basic_insert_set_test\n");
	rl_btree *btree = NULL;
	long **vals = malloc(sizeof(long *) * 7);

	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_set_long, 2);
	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;

		RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, vals[i], NULL);
		RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
	}

	for (i = 0; i < 7; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_FOUND, db, btree, vals[i], NULL, NULL, NULL);
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_NOT_FOUND, db, btree, &nonexistent_vals[i], NULL, NULL, NULL);
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	retval = 0;
cleanup:
	free(vals);
	rl_close(db);
	return retval;
}

int basic_insert_hash_test()
{
	fprintf(stderr, "Start basic_insert_hash_test\n");
	long **keys = malloc(sizeof(long *) * 7);
	long **vals = malloc(sizeof(long *) * 7);

	rl_btree *btree = NULL;
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, 2);
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
	fprintf(stderr, "End basic_insert_set_test\n");
	retval = 0;
cleanup:
	free(vals);
	free(keys);
	rl_close(db);
	return retval;
}

int basic_delete_set_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_set_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);

	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	long **vals = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_set_long, 2);
	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long abs_elements = labs(elements);
	long pos_element_to_remove = abs_elements == elements ? element_to_remove : (-element_to_remove);
	vals = malloc(sizeof(long *) * abs_elements);
	long i, j;
	for (i = 0; i < abs_elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = elements == abs_elements ? i + 1 : (-i - 1);
		RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, vals[i], NULL);
		RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
	}

	RL_CALL_VERBOSE(rl_btree_remove_element, RL_OK, db, btree, btree_page, vals[pos_element_to_remove - 1]);
	RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);

	int expected;
	long score[1];
	for (j = 0; j < abs_elements; j++) {
		if (j == pos_element_to_remove - 1) {
			expected = RL_NOT_FOUND;
			*score = element_to_remove;
		}
		else {
			expected = RL_FOUND;
			*score = *vals[j];
		}
		RL_CALL_VERBOSE(rl_btree_find_score, expected, db, btree, score, NULL, NULL, NULL);
	}

	fprintf(stderr, "End basic_delete_set_test (%ld, %ld)\n", elements, element_to_remove);
	retval = 0;
cleanup:
	free(vals);
	rl_close(db);
	return retval;
}

int random_hash_test(long size, long btree_node_size)
{
	fprintf(stderr, "Start random_hash_test %ld %ld\n", size, btree_node_size);
	long *key, *val;
	int *results = malloc(sizeof(int) * size);

	rl_btree *btree = NULL;
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);
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

	fprintf(stderr, "End random_hash_test\n");
	retval = 0;
cleanup:
	free(results);
	rl_close(db);
	return retval;
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

int fuzzy_set_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_set_test %ld %ld %d\n", size, btree_node_size, _commit);
	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);

	void **flatten_scores = malloc(sizeof(void *) * size);
	void *tmp;
	long j, flatten_size;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_set_long, btree_node_size);
	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long i, element, *element_copy;

	for (i = 0; i < size; i++) {
		element = rand();
		if (contains_element(element, elements, i)) {
			i--;
			continue;
		}
		else {
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_set_long, btree_page, &rl_btree_type_set_long, &tmp, 1);
			btree = tmp;
			elements[i] = element;
			element_copy = malloc(sizeof(long));
			*element_copy = element;
			RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, element_copy, NULL);
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
			rl_close(db);
			db = NULL;
			RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 0);
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

	RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_set_long, btree_page, &rl_btree_type_set_long, &tmp, 1);
	btree = tmp;

	for (i = 0; i < size; i++) {
		RL_CALL_VERBOSE(rl_btree_find_score, RL_FOUND, db, btree, &elements[i], NULL, NULL, NULL);
		RL_CALL_VERBOSE(rl_btree_find_score, RL_NOT_FOUND, db, btree, &nonelements[i], NULL, NULL, NULL);
	}
	fprintf(stderr, "End fuzzy_set_test\n");

	retval = 0;
cleanup:
	rl_free(elements);
	rl_free(nonelements);
	rl_free(flatten_scores);
	rl_close(db);
	return retval;
}

int fuzzy_hash_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_hash_test %ld %ld %d\n", size, btree_node_size, _commit);
	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	long *values = malloc(sizeof(long) * size);
	void **flatten_scores = malloc(sizeof(void *) * size);

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);
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
	fprintf(stderr, "End fuzzy_hash_test\n");

	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	rl_close(db);
	return retval;
}

int fuzzy_hash_test_iterator(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_hash_test_iterator %ld %ld %d\n", size, btree_node_size, _commit);
	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	rl_btree_iterator *iterator = NULL;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	long *values = malloc(sizeof(long) * size);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);
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

	fprintf(stderr, "End fuzzy_hash_test_iterator\n");

	rl_btree_iterator_destroy(iterator);
	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	rl_close(db);
	return retval;
}


int fuzzy_set_delete_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_set_delete_test %ld %ld %d\n", size, btree_node_size, _commit);
	int retval;
	void *tmp;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	long *elements = malloc(sizeof(long) * size);

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create_size, RL_OK, db, &btree, &rl_btree_type_set_long, btree_node_size);
	long btree_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, btree->type->btree_type, btree_page, btree);

	long i, element, *element_copy;

	for (i = 0; i < size; i++) {
		element = rand();
		if (contains_element(element, elements, i)) {
			i--;
			continue;
		}
		else {
			elements[i] = element;
			element_copy = malloc(sizeof(long));
			*element_copy = element;
			RL_CALL_VERBOSE(rl_btree_add_element, RL_OK, db, btree, btree_page, element_copy, NULL);
			RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
		}
		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_btree_set_long, btree_page, &rl_btree_type_set_long, &tmp, 1);
			btree = tmp;
		}
	}

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		RL_CALL2_VERBOSE(rl_btree_remove_element, RL_OK, RL_DELETED, db, btree, btree_page, &elements[i]);

		elements[i] = elements[size - 1];
		if (size-- > 1) {
			RL_CALL_VERBOSE(rl_btree_is_balanced, RL_OK, db, btree);
		}
	}
	fprintf(stderr, "End fuzzy_set_delete_test\n");

	retval = 0;
cleanup:
	free(elements);
	rl_close(db);
	return retval;
}

#define DELETE_TESTS_COUNT 7

RL_TEST_MAIN_START(btree_test)
{
	int i, j, k;
	long size, btree_node_size;
	int commit;
	RL_TEST(basic_insert_set_test, 0);
	RL_TEST(basic_insert_hash_test, 0);
	RL_TEST(random_hash_test, 10, 2);
	RL_TEST(random_hash_test, 100, 10);

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
		RL_TEST(basic_delete_set_test, delete_tests[i][0], delete_tests[i][1], delete_tests_name[i]);
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			btree_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_set_test, size, btree_node_size, commit);
			}
		}
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			btree_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_set_delete_test, size, btree_node_size, commit);
			}
		}
	}
	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			btree_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_hash_test, size, btree_node_size, commit);
				RL_TEST(fuzzy_hash_test_iterator, size, btree_node_size, commit);
			}
		}
	}
}
RL_TEST_MAIN_END
