#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rlite.h"
#include "status.h"
#include "page_list.h"

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

int basic_insert_list_test(int options)
{
	fprintf(stderr, "Start basic_insert_list_test %d\n", options);

	rlite *db = setup_db(0);
	rl_list *list;
	db->page_size = sizeof(long) * 2 + 12;
	if (RL_OK != rl_list_create(db, &list, &list_long)) {
		return 1;
	}

	int retval;
	long **vals = malloc(sizeof(long *) * 7);
	long i, position;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;
	}
	for (i = 0; i < 7; i++) {
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
			return 1;
		}
		if (RL_OK != rl_list_is_balanced(db, list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_list(list);

	for (i = 0; i < 7; i++) {
		retval = rl_list_find_element(db, list, vals[i], NULL, &position);
		if (RL_FOUND != retval) {
			fprintf(stderr, "Failed to find child %ld (%d)\n", i, retval);
			return 1;
		}
		if (position != (options % 2 == 0 ? i : (6 - i))) {
			fprintf(stderr, "Unexpected position of item %ld, %ld\n", (options % 2 == 0 ? i : (6 - i)), position);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (RL_NOT_FOUND != rl_list_find_element(db, list, &nonexistent_vals[i], NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_list_test\n");
	free(vals);
	free(list);
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
int fuzzy_list_test(long size, long list_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_list_test %ld %ld %d\n", size, list_node_size, _commit);

	rlite *db = setup_db(_commit);
	rl_list *list;
	db->page_size = sizeof(long) * list_node_size + 12;
	if (RL_OK != rl_list_create(db, &list, &list_long)) {
		return 1;
	}

	long i, element, *element_copy;
	long *elements = malloc(sizeof(long) * size);
	long *nonelements = malloc(sizeof(long) * size);

	void **flatten_elements = malloc(sizeof(void *) * size);
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
		if (0 != rl_list_add_element(db, list, element_copy, positive ? position : (- i + position - 1))) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (RL_OK != rl_list_is_balanced(db, list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
		rl_flatten_list(db, list, &flatten_elements);
		for (j = 0; j < list->size; j++) {
			if (*(long *)flatten_elements[j] != elements[j]) {
				fprintf(stderr, "Unexpected value in position %ld after adding %ld (expected %ld, got %ld)\n", j, i, elements[j], *(long *)flatten_elements[j]);
				return 1;
			}
		}
		if (_commit) {
			if (RL_OK != rl_commit(db)) {
				return 1;
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
		if (RL_FOUND != rl_list_find_element(db, list, &elements[i], NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (RL_NOT_FOUND != rl_list_find_element(db, list, &nonelements[i], NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_list_test\n");

	free(elements);
	free(nonelements);
	free(flatten_elements);
	free(list);
	rl_close(db);
	return 0;
}

int basic_delete_list_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_list_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);

	rlite *db = setup_db(0);
	rl_list *list;
	db->page_size = sizeof(long) * 2 + 12;
	if (RL_OK != rl_list_create(db, &list, &list_long)) {
		return 1;
	}
	long pos_element_to_remove = element_to_remove >= 0 ? (element_to_remove) : (elements + element_to_remove);
	long **vals = malloc(sizeof(long *) * elements);
	long i, j;
	for (i = 0; i < elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
	}
	for (i = 0; i < elements; i++) {
		if (0 != rl_list_add_element(db, list, vals[i], i)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (RL_OK != rl_list_is_balanced(db, list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_list(list);

	if (0 != rl_list_remove_element(db, list, element_to_remove)) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		return 1;
	}

	// rl_print_list(list);

	if (RL_OK != rl_list_is_balanced(db, list)) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		return 1;
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
		if (expected != rl_list_find_element(db, list, element, NULL, NULL)) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == 0 ? "" : "not ", j, *vals[j], element_to_remove);
			return 1;
		}
	}

	fprintf(stderr, "End basic_delete_list_test (%ld, %ld)\n", elements, element_to_remove);
	free(vals);
	free(list);
	rl_close(db);
	return 0;
}

int fuzzy_list_delete_test(long size, long list_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_list_delete_test %ld %ld %d\n", size, list_node_size, _commit);

	rlite *db = setup_db(_commit);
	rl_list *list;
	db->page_size = sizeof(long) * list_node_size + 12;
	if (RL_OK != rl_list_create(db, &list, &list_long)) {
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
			if (0 != rl_list_add_element(db, list, element_copy, -1)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (RL_OK != rl_list_is_balanced(db, list)) {
				fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
				return 1;
			}
		}
		if (RL_OK != rl_commit(db)) {
			return 1;
		}
	}

	// rl_print_list(list);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		if (0 != rl_list_remove_element(db, list, i)) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			return 1;
		}

		// rl_print_list(list);

		if (RL_OK != rl_list_is_balanced(db, list)) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			return 1;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_list_delete_test\n");

	free(elements);
	free(list);
	rl_close(db);
	return 0;
}

#define DELETE_TESTS_COUNT 5
int main()
{
	int i, j, k;
	long size, list_node_size;
	int commit;
	int retval = 0;

	for (i = 0; i < 4; i++) {
		retval = basic_insert_list_test(i);
		if (retval != 0) {
			goto cleanup;
		}
	}
	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			list_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				retval = fuzzy_list_test(size, list_node_size, commit);
				if (retval != 0) {
					goto cleanup;
				}
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
		retval = basic_delete_list_test(delete_tests[i][0], delete_tests[i][1], delete_tests_name[i]);
		if (retval != 0) {
			goto cleanup;
		}
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			list_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 2; k++) {
				commit = k;
				srand(1);
				retval = fuzzy_list_delete_test(size, list_node_size, commit);
				if (retval != 0) {
					goto cleanup;
				}
			}
		}
	}

cleanup:
	return retval;
}
