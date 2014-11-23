#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../status.h"
#include "../page_list.h"

int basic_insert_list_test(int options)
{
	fprintf(stderr, "Start basic_insert_list_test %d\n", options);

	int retval;
	rlite *db = NULL;
	rl_list *list = NULL;
	long **vals = malloc(sizeof(long *) * 7);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	db->number_of_databases = 1;
	db->page_size = sizeof(long) * 2 + 12;
	retval = rl_list_create(db, &list, &list_long);
	if (RL_OK != retval) {
		goto cleanup;
	}

	long i, position;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;
		switch(options) {
			case 0:
				position = i;
				break;
			case 1:
				position = 0;
				break;
			case 2:
				position = -1;
				break;
			case 3:
				position = - 1 - i;
				break;
		}
		retval = rl_list_add_element(db, list, vals[i], position);
		if (0 != retval) {
			fprintf(stderr, "Failed to add child %ld (%d)\n", i, retval);
			goto cleanup;
		}
		retval = rl_list_is_balanced(db, list);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			goto cleanup;
		}
	}

	// rl_print_list(list);

	for (i = 0; i < 7; i++) {
		retval = rl_list_find_element(db, list, vals[i], NULL, &position, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld (%d)\n", i, retval);
			goto cleanup;
		}
		if (position != (options % 2 == 0 ? i : (6 - i))) {
			fprintf(stderr, "Unexpected position of item %ld, %ld\n", (options % 2 == 0 ? i : (6 - i)), position);
			goto cleanup;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (RL_NOT_FOUND != rl_list_find_element(db, list, &nonexistent_vals[i], NULL, NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End basic_insert_list_test\n");
	retval = 0;
cleanup:
	free(vals);
	if (list) {
		rl_list_destroy(db, list);
	}
	rl_close(db);
	return retval;
}

#define ITERATOR_SIZE 500
int basic_iterator_list_test(int _commit)
{
	fprintf(stderr, "Start basic_iterator_list_test %d\n", _commit);

	rlite *db = NULL;
	rl_list *list = NULL;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	retval = rl_list_create(db, &list, &list_long);
	if (RL_OK != retval) {
		goto cleanup;
	}

	long i, *element;
	for (i = 0; i < ITERATOR_SIZE; i++) {
		element = malloc(sizeof(long));
		*element = i;
		retval = rl_list_add_element(db, list, element, -1);
		if (0 != retval) {
			fprintf(stderr, "Failed to add element %ld\n", i);
			goto cleanup;
		}
	}

	if (_commit) {
		retval = rl_commit(db);
		if (RL_OK != retval) {
			fprintf(stderr, "Failed to commit\n");
			goto cleanup;
		}
	}

	rl_list_iterator *iterator;
	retval = rl_list_iterator_create(db, &iterator, list, 1);
	if (RL_OK != retval) {
		fprintf(stderr, "Failed to create iterator, got %d\n", retval);
		goto cleanup;
	}

	i = 0;
	void *tmp;
	long val;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		val = *(long *)tmp;
		if (val != i) {
			fprintf(stderr, "Expected %ld element to be %ld, got %ld instead\n", i, i, val);
			goto cleanup;
		}
		rl_free(tmp);
		i++;
	}

	if (retval != RL_END) {
		rl_free(tmp);
		goto cleanup;
	}

	if (i != ITERATOR_SIZE) {
		fprintf(stderr, "Expected %d iterations, got %ld\n", ITERATOR_SIZE, i);
		goto cleanup;
	}

	fprintf(stderr, "End basic_iterator_list_test %d\n", _commit);
	retval = 0;
cleanup:
	if (list) {
		rl_list_destroy(db, list);
	}
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
int fuzzy_list_test(long size, long list_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_list_test %ld %ld %d\n", size, list_node_size, _commit);

	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);
	void **flatten_elements = malloc(sizeof(void *) * size);
	rlite *db = NULL;
	rl_list *list = NULL;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	db->number_of_databases = 1;
	db->page_size = sizeof(long) * list_node_size + 12;
	retval = rl_list_create(db, &list, &list_long);
	if (RL_OK != retval) {
		goto cleanup;
	}

	long i, element, *element_copy;

	long j, position;
	int positive;

	for (i = 0; i < size; i++) {
		element = rand();
		if (contains_element(element, elements, i)) {
			i--;
			continue;
		}
		if (i == 0) {
			position = 0;
		}
		else {
			position = rand() % (i + 1);
			if (position != i) {
				memmove(&elements[position + 1], &elements[position], sizeof(long) * (i - position));
			}
		}
		elements[position] = element;
		element_copy = malloc(sizeof(long));
		*element_copy = element;
		positive = rand() % 2;
		retval = rl_list_add_element(db, list, element_copy, positive ? position : (- i + position - 1));
		if (0 != retval) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			goto cleanup;
		}
		retval = rl_list_is_balanced(db, list);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			goto cleanup;
		}
		rl_flatten_list(db, list, flatten_elements);
		for (j = 0; j < list->size; j++) {
			if (*(long *)flatten_elements[j] != elements[j]) {
				fprintf(stderr, "Unexpected value in position %ld after adding %ld (expected %ld, got %ld)\n", j, i, elements[j], *(long *)flatten_elements[j]);
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

	// rl_print_list(list);

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
		retval = rl_list_find_element(db, list, &elements[i], NULL, NULL, NULL, NULL);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			goto cleanup;
		}
		retval = rl_list_find_element(db, list, &nonelements[i], NULL, NULL, NULL, NULL);
		if (RL_NOT_FOUND != retval) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			goto cleanup;
		}
	}
	fprintf(stderr, "End fuzzy_list_test\n");

	retval = 0;
cleanup:
	free(elements);
	free(nonelements);
	free(flatten_elements);
	if (list) {
		rl_list_destroy(db, list);
	}
	rl_close(db);
	return retval;
}

int basic_delete_list_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_list_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);

	long **vals = malloc(sizeof(long *) * elements);
	rlite *db = NULL;
	rl_list *list = NULL;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	db->number_of_databases = 1;
	db->page_size = sizeof(long) * 2 + 12;
	retval = rl_list_create(db, &list, &list_long);
	if (RL_OK != retval) {
		goto cleanup;
	}
	long pos_element_to_remove = element_to_remove >= 0 ? (element_to_remove) : (elements + element_to_remove);
	long i, j;
	for (i = 0; i < elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
		retval = rl_list_add_element(db, list, vals[i], i);
		if (0 != retval) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			goto cleanup;
		}
		retval = rl_list_is_balanced(db, list);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			goto cleanup;
		}
	}

	// rl_print_list(list);

	retval = rl_list_remove_element(db, list, element_to_remove);
	if (0 != retval) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		goto cleanup;
	}

	// rl_print_list(list);

	retval = rl_list_is_balanced(db, list);
	if (RL_OK != retval) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		goto cleanup;
	}

	int expected;
	long element[1];
	for (j = 0; j < elements; j++) {
		if (j == pos_element_to_remove) {
			expected = RL_NOT_FOUND;
			*element = element_to_remove;
		}
		else {
			expected = RL_FOUND;
			*element = *vals[j];
		}
		retval = rl_list_find_element(db, list, element, NULL, NULL, NULL, NULL);
		if (expected != retval) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == 0 ? "" : "not ", j, *vals[j], element_to_remove);
			goto cleanup;
		}
	}

	fprintf(stderr, "End basic_delete_list_test (%ld, %ld)\n", elements, element_to_remove);
	retval = 0;
cleanup:
	free(vals);
	if (list) {
		rl_list_destroy(db, list);
	}
	rl_close(db);
	return retval;
}

int fuzzy_list_delete_test(long size, long list_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_list_delete_test %ld %ld %d\n", size, list_node_size, _commit);

	rlite *db = NULL;
	rl_list *list = NULL;
	long *elements = malloc(sizeof(long) * size);
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	db->number_of_databases = 1;
	db->page_size = sizeof(long) * list_node_size + 12;
	retval = rl_list_create(db, &list, &list_long);
	if (RL_OK != retval) {
		goto cleanup;
	}

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
			retval = rl_list_add_element(db, list, element_copy, -1);
			if (0 != retval) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				goto cleanup;
			}
			retval = rl_list_is_balanced(db, list);
			if (RL_OK != retval) {
				fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
				goto cleanup;
			}
		}
		retval = rl_commit(db);
		if (RL_OK != retval) {
			goto cleanup;
		}
	}

	// rl_print_list(list);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		retval = rl_list_remove_element(db, list, i);
		if (0 != retval) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			goto cleanup;
		}

		// rl_print_list(list);

		retval = rl_list_is_balanced(db, list);
		if (RL_OK != retval) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			goto cleanup;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_list_delete_test\n");

	retval = 0;
cleanup:
	rl_free(elements);
	if (list) {
		rl_list_destroy(db, list);
	}
	rl_close(db);
	return retval;
}

#define DELETE_TESTS_COUNT 5
RL_TEST_MAIN_START(list_test)
{
	int i, j, k;
	long size, list_node_size;
	int commit;

	for (i = 0; i < 4; i++) {
		RL_TEST(basic_insert_list_test, i);
	}

	for (i = 0; i < 2; i++) {
		RL_TEST(basic_iterator_list_test, i);
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			list_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_list_test, size, list_node_size, commit);
			}
		}
	}

	long delete_tests[DELETE_TESTS_COUNT][2] = {
		{8, 7},
		{8, -1},
		{7, -1},
		{1, 0},
		{3, 0},
	};
	char *delete_tests_name[DELETE_TESTS_COUNT] = {
		"delete last element, positive index",
		"delete last element, negative index",
		"delete last element, deletes node, negative index",
		"delete only element",
		"delete element on page, merge and delete",
	};
	for (i = 0; i < DELETE_TESTS_COUNT; i++) {
		RL_TEST(basic_delete_list_test, delete_tests[i][0], delete_tests[i][1], delete_tests_name[i]);
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			list_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_list_delete_test, size, list_node_size, commit);
			}
		}
	}
}
RL_TEST_MAIN_END
