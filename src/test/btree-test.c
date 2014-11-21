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
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_set_long, 2);
	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;

		retval = rl_btree_add_element(db, btree, vals[i], NULL);
		if (retval != RL_OK) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			goto cleanup;
		}
		retval = rl_btree_is_balanced(db, btree);
		if (retval != RL_OK) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			goto cleanup;
		}
	}
	// rl_print_btree(btree);
	for (i = 0; i < 7; i++) {
		retval = rl_btree_find_score(db, btree, vals[i], NULL, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			goto cleanup;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		retval = rl_btree_find_score(db, btree, &nonexistent_vals[i], NULL, NULL, NULL);
		if (RL_NOT_FOUND != retval) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	retval = 0;
cleanup:
	rl_free(btree);
	free(vals);
	if (db) {
		rl_close(db);
	}
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
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_hash_long_long, 2);
	long i;
	for (i = 0; i < 7; i++) {
		keys[i] = malloc(sizeof(long));
		vals[i] = malloc(sizeof(long));
		*keys[i] = i + 1;
		*vals[i] = i * 10;
		retval = rl_btree_add_element(db, btree, keys[i], vals[i]);
		if (retval != RL_OK) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			goto cleanup;
		}
		retval = rl_btree_is_balanced(db, btree);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			goto cleanup;
		}
	}

	// rl_print_btree(btree);

	void *val;
	for (i = 0; i < 7; i++) {
		retval = rl_btree_find_score(db, btree, keys[i], &val, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			goto cleanup;
		}
		if (val != vals[i] || *(long *)val != i * 10) {
			fprintf(stderr, "Wrong value in position %ld (%ld)\n", i, *(long *)val);
			goto cleanup;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		retval = rl_btree_find_score(db, btree, &nonexistent_vals[i], NULL, NULL, NULL);
		if (RL_NOT_FOUND != retval) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	retval = 0;
cleanup:
	free(vals);
	free(keys);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
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
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_set_long, 2);

	long abs_elements = labs(elements);
	long pos_element_to_remove = abs_elements == elements ? element_to_remove : (-element_to_remove);
	vals = malloc(sizeof(long *) * abs_elements);
	long i, j;
	for (i = 0; i < abs_elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = elements == abs_elements ? i + 1 : (-i - 1);
		retval = rl_btree_add_element(db, btree, vals[i], NULL);
		if (RL_OK != retval) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			goto cleanup;
		}
		retval = rl_btree_is_balanced(db, btree);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			goto cleanup;
		}
	}

	// rl_print_btree(btree);

	retval = rl_btree_remove_element(db, btree, vals[pos_element_to_remove - 1]);
	if (RL_OK != retval) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		goto cleanup;
	}

	// rl_print_btree(btree);

	retval = rl_btree_is_balanced(db, btree);
	if (RL_OK != retval) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		goto cleanup;
	}

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
		retval = rl_btree_find_score(db, btree, score, NULL, NULL, NULL);
		if (expected != retval) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == RL_FOUND ? "" : "not ", j, *vals[j], element_to_remove);
			goto cleanup;
		}
	}

	fprintf(stderr, "End basic_delete_set_test (%ld, %ld)\n", elements, element_to_remove);
	retval = 0;
cleanup:
	free(vals);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
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
	long j, flatten_size;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_set_long, btree_node_size);

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
			retval = rl_btree_add_element(db, btree, element_copy, NULL);
			if (RL_OK != retval) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				goto cleanup;
			}
			retval = rl_btree_is_balanced(db, btree);
			if (RL_OK != retval) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
				goto cleanup;
			}
		}
		flatten_size = 0;
		retval = rl_flatten_btree(db, btree, &flatten_scores, &flatten_size);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to flatten btree\n");
			goto cleanup;
		}
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				goto cleanup;
			}
		}
		if (_commit) {
			retval = rl_commit(db);
			if (RL_OK != retval) {
				goto cleanup;
			}
			rl_close(db);
			db = NULL;
			RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 0);
		}
	}

	// rl_print_btree(btree);

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
		retval = rl_btree_find_score(db, btree, &elements[i], NULL, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			goto cleanup;
		}
		retval = rl_btree_find_score(db, btree, &nonelements[i], NULL, NULL, NULL);
		if (RL_NOT_FOUND != retval) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End fuzzy_set_test\n");

	retval = 0;
cleanup:
	rl_free(elements);
	rl_free(nonelements);
	rl_free(flatten_scores);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
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
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);

	long i, element, value, *element_copy, *value_copy;

	long j, flatten_size;

	void *val;

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
			retval = rl_btree_add_element(db, btree, element_copy, value_copy);
			if (RL_OK != retval) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				goto cleanup;
			}
			retval = rl_btree_is_balanced(db, btree);
			if (RL_OK != retval) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%ld)\n", i, value);
				goto cleanup;
			}
		}
		flatten_size = 0;
		retval = rl_flatten_btree(db, btree, &flatten_scores, &flatten_size);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to flatten btree\n");
			goto cleanup;
		}
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				goto cleanup;
			}
		}
		if (_commit) {
			retval = rl_commit(db);
			if (RL_OK != retval) {
				goto cleanup;
			}
		}
	}

	// rl_print_btree(btree);

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
		retval = rl_btree_find_score(db, btree, &elements[i], &val, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			goto cleanup;
		}
		if (*(long *)val != values[i]) {
			fprintf(stderr, "Value doesn't match expected value at position %ld (%ld) (%ld != %ld)\n", i, elements[i], *(long *)val, values[i]);
			goto cleanup;
		}
		retval = rl_btree_find_score(db, btree, &nonelements[i], NULL, NULL, NULL);
		if (RL_NOT_FOUND != retval) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End fuzzy_hash_test\n");

	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
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

	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_hash_long_long, btree_node_size);

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
			retval = rl_btree_add_element(db, btree, element_copy, value_copy);
			if (RL_OK != retval) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				goto cleanup;
			}
			retval = rl_btree_is_balanced(db, btree);
			if (RL_OK != retval) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%ld)\n", i, value);
				goto cleanup;
			}
		}

		retval = rl_btree_iterator_create(db, btree, &iterator);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to create btree iterator\n");
			goto cleanup;
		}

		j = 0;
		while (RL_OK == (retval = rl_btree_iterator_next(iterator, &tmp, NULL))) {
			score = *(long *)tmp;
			rl_free(tmp);
			if (j++ > 0) {
				if (prev_score >= score) {
					fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
					goto cleanup;
				}
			}
			prev_score = score;
		}

		if (retval != RL_END) {
			rl_free(tmp);
			goto cleanup;
		}

		if (j != i + 1) {
			fprintf(stderr, "Expected to iterate %ld elements, only did %ld\n", i + 1, j);
			goto cleanup;
		}

		iterator = NULL;

		if (_commit) {
			retval = rl_commit(db);
			if (RL_OK != retval) {
				goto cleanup;
			}
		}
	}

	fprintf(stderr, "End fuzzy_hash_test_iterator\n");

	rl_btree_iterator_destroy(iterator);
	retval = 0;
cleanup:
	free(values);
	free(elements);
	free(nonelements);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
	return retval;
}


int fuzzy_set_delete_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_set_delete_test %ld %ld %d\n", size, btree_node_size, _commit);
	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	long *elements = malloc(sizeof(long) * size);

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_set_long, btree_node_size);

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
			retval = rl_btree_add_element(db, btree, element_copy, NULL);
			if (RL_OK != retval) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				goto cleanup;
			}
			retval = rl_btree_is_balanced(db, btree);
			if (RL_OK != retval) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
				goto cleanup;
			}
		}
		if (_commit) {
			retval = rl_commit(db);
			if (RL_OK != retval) {
				goto cleanup;
			}
		}
	}

	// rl_print_btree(btree);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		retval = rl_btree_remove_element(db, btree, &elements[i]);
		if (RL_OK != retval) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			goto cleanup;
		}

		// rl_print_btree(btree);

		retval = rl_btree_is_balanced(db, btree);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			goto cleanup;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_set_delete_test\n");

	retval = 0;
cleanup:
	free(elements);
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
	return retval;
}

int basic_test_insert_delete_insert(int _commit)
{
	fprintf(stderr, "Start basic_test_insert_delete_insert %d\n", _commit);
	int retval;
	rlite *db = NULL;
	rl_btree *btree = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	RL_CALL_VERBOSE(rl_btree_create, RL_OK, db, &btree, &rl_btree_type_set_long, 2);

	long *element = malloc(sizeof(long));
	*element = 1;
	retval = rl_btree_add_element(db, btree, element, NULL);
	if (RL_OK != retval) {
		fprintf(stderr, "Failed to add child, got %d\n", retval);
		goto cleanup;
	}

	retval = rl_btree_remove_element(db, btree, element);
	if (RL_OK != retval) {
		fprintf(stderr, "Failed to remove child, got %d\n", retval);
		goto cleanup;
	}

	element = malloc(sizeof(long));
	*element = 1;
	retval = rl_btree_add_element(db, btree, element, NULL);
	if (RL_OK != retval) {
		fprintf(stderr, "Failed to add child, got %d\n", retval);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_insert_delete_insert\n");

	retval = 0;
cleanup:
	rl_free(btree);
	if (db) {
		rl_close(db);
	}
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

	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_insert_delete_insert, i);
	}

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
