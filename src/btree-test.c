#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rlite.h"
#include "page_btree.h"
#include "status.h"

static rlite *setup_db(int file)
{
	const char *filepath = "rlite-test.rld";
	if (access(filepath, F_OK) == 0) {
		unlink(filepath);
	}
	rlite *db;
	if (rl_open(file ? filepath : ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return NULL;
	}
	return db;
}

int basic_insert_set_test()
{
	fprintf(stderr, "Start basic_insert_set_test\n");

	rlite *db = setup_db(0);
	rl_btree *btree;
	if (rl_btree_create(db, &btree, &long_set, 2) != RL_OK) {
		return 1;
	}
	long **vals = malloc(sizeof(long *) * 7);
	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;
	}
	for (i = 0; i < 7; i++) {
		if (RL_OK != rl_btree_add_element(db, btree, vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (RL_OK != rl_btree_is_balanced(db, btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			return 1;
		}
	}
	// rl_print_btree(btree);
	for (i = 0; i < 7; i++) {
		if (RL_FOUND != rl_btree_find_score(db, btree, vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (RL_NOT_FOUND != rl_btree_find_score(db, btree, &nonexistent_vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	free(vals);
	free(btree);
	rl_close(db);
	return 0;
}

int basic_insert_hash_test()
{
	fprintf(stderr, "Start basic_insert_hash_test\n");

	rl_btree *btree;
	rlite *db = setup_db(0);
	if (rl_btree_create(db, &btree, &long_hash, 2) != RL_OK) {
		return 1;
	}
	long **keys = malloc(sizeof(long *) * 7);
	long **vals = malloc(sizeof(long *) * 7);
	long i;
	for (i = 0; i < 7; i++) {
		keys[i] = malloc(sizeof(long));
		vals[i] = malloc(sizeof(long));
		*keys[i] = i + 1;
		*vals[i] = i * 10;
	}
	for (i = 0; i < 7; i++) {
		if (RL_OK != rl_btree_add_element(db, btree, keys[i], vals[i])) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (RL_OK != rl_btree_is_balanced(db, btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			return 1;
		}
	}

	// rl_print_btree(btree);

	void *val;
	for (i = 0; i < 7; i++) {
		if (RL_FOUND != rl_btree_find_score(db, btree, keys[i], &val, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			return 1;
		}
		if (val != vals[i] || *(long *)val != i * 10) {
			fprintf(stderr, "Wrong value in position %ld (%ld)\n", i, *(long *)val);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (RL_NOT_FOUND != rl_btree_find_score(db, btree, &nonexistent_vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	free(vals);
	free(keys);
	free(btree);
	rl_close(db);
	return 0;
}

int basic_delete_set_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_set_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);

	rlite *db = setup_db(0);
	rl_btree *btree;
	if (rl_btree_create(db, &btree, &long_set, 2) != RL_OK) {
		return 1;
	}
	long abs_elements = labs(elements);
	long pos_element_to_remove = abs_elements == elements ? element_to_remove : (-element_to_remove);
	long **vals = malloc(sizeof(long *) * abs_elements);
	long i, j;
	for (i = 0; i < abs_elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = elements == abs_elements ? i + 1 : (-i - 1);
	}
	for (i = 0; i < abs_elements; i++) {
		if (RL_OK != rl_btree_add_element(db, btree, vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (RL_OK != rl_btree_is_balanced(db, btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
			return 1;
		}
	}

	// rl_print_btree(btree);

	if (RL_OK != rl_btree_remove_element(db, btree, vals[pos_element_to_remove - 1])) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		return 1;
	}

	// rl_print_btree(btree);

	if (RL_OK != rl_btree_is_balanced(db, btree)) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		return 1;
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
		if (expected != rl_btree_find_score(db, btree, score, NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == RL_FOUND ? "" : "not ", j, *vals[j], element_to_remove);
			return 1;
		}
	}

	fprintf(stderr, "End basic_delete_set_test (%ld, %ld)\n", elements, element_to_remove);
	free(vals);
	free(btree);
	rl_close(db);
	return 0;
}

int contains_element(long element, long *elements, long size)
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
	rlite *db = setup_db(_commit);
	rl_btree *btree;
	if (rl_btree_create(db, &btree, &long_set, btree_node_size) != RL_OK) {
		return 1;
	}

	long i, element, *element_copy;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);

	void **flatten_scores = malloc(sizeof(void *) * size);
	long j, flatten_size;

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
			if (RL_OK != rl_btree_add_element(db, btree, element_copy, NULL)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (RL_OK != rl_btree_is_balanced(db, btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
				return 1;
			}
		}
		flatten_size = 0;
		if (RL_OK != rl_flatten_btree(db, btree, &flatten_scores, &flatten_size)) {
			fprintf(stderr, "Unable to flatten btree\n");
			return 1;
		}
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				return 1;
			}
		}
		if (_commit) {
			if (RL_OK != rl_commit(db)) {
				return 1;
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
		if (RL_FOUND != rl_btree_find_score(db, btree, &elements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (RL_NOT_FOUND != rl_btree_find_score(db, btree, &nonelements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_set_test\n");

	free(elements);
	free(nonelements);
	free(flatten_scores);
	free(btree);
	rl_close(db);
	return 0;
}

int fuzzy_hash_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_hash_test %ld %ld %d\n", size, btree_node_size, _commit);
	rlite *db = setup_db(_commit);
	rl_btree *btree;
	if (rl_btree_create(db, &btree, &long_hash, btree_node_size) != RL_OK) {
		return 1;
	}

	long i, element, value, *element_copy, *value_copy;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	long *values = malloc(sizeof(long) * size);

	void **flatten_scores = malloc(sizeof(void *) * size);
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
			if (RL_OK != rl_btree_add_element(db, btree, element_copy, value_copy)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (RL_OK != rl_btree_is_balanced(db, btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%ld)\n", i, value);
				return 1;
			}
		}
		flatten_size = 0;
		if (RL_OK != rl_flatten_btree(db, btree, &flatten_scores, &flatten_size)) {
			fprintf(stderr, "Unable to flatten btree\n");
			return 1;
		}
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				return 1;
			}
		}
		if (_commit) {
			if (RL_OK != rl_commit(db)) {
				return 1;
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
		if (RL_FOUND != rl_btree_find_score(db, btree, &elements[i], &val, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (*(long *)val != values[i]) {
			fprintf(stderr, "Value doesn't match expected value at position %ld (%ld) (%ld != %ld)\n", i, elements[i], *(long *)val, values[i]);
			return 1;
		}
		if (RL_NOT_FOUND != rl_btree_find_score(db, btree, &nonelements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_hash_test\n");

	free(values);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	free(btree);
	rl_close(db);
	return 0;
}

int fuzzy_set_delete_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_set_delete_test %ld %ld %d\n", size, btree_node_size, _commit);
	rlite *db = setup_db(_commit);
	rl_btree *btree;
	if (rl_btree_create(db, &btree, &long_set, btree_node_size) != RL_OK) {
		return 1;
	}

	long i, element, *element_copy;
	long *elements = malloc(sizeof(long) * size);

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
			if (RL_OK != rl_btree_add_element(db, btree, element_copy, NULL)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (RL_OK != rl_btree_is_balanced(db, btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%d)\n", i, __LINE__);
				return 1;
			}
		}
		if (_commit) {
			if (RL_OK != rl_commit(db)) {
				return 1;
			}
		}
	}

	// rl_print_btree(btree);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		if (RL_OK != rl_btree_remove_element(db, btree, &elements[i])) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			return 1;
		}

		// rl_print_btree(btree);

		if (RL_OK != rl_btree_is_balanced(db, btree)) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			return 1;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_set_delete_test\n");

	free(elements);
	free(btree);
	rl_close(db);
	return 0;
}

#define DELETE_TESTS_COUNT 7
int main()
{
	int i, j, k;
	long size, btree_node_size;
	int commit;
	int retval = 0;
	retval = basic_insert_set_test();
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_insert_hash_test();
	if (retval != 0) {
		goto cleanup;
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
		retval = basic_delete_set_test(delete_tests[i][0], delete_tests[i][1], delete_tests_name[i]);
		if (retval != 0) {
			goto cleanup;
		}
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			btree_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				retval = fuzzy_set_test(size, btree_node_size, commit);
				if (retval != 0) {
					goto cleanup;
				}
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
				retval = fuzzy_set_delete_test(size, btree_node_size, commit);
				if (retval != 0) {
					goto cleanup;
				}
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
				retval = fuzzy_hash_test(size, btree_node_size, commit);
				if (retval != 0) {
					goto cleanup;
				}
			}
		}
	}

cleanup:
	return retval;
}
