#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../src/rlite.h"
#include "../src/status.h"
#include "../src/page_list.h"

int basic_insert_list_test(int options)
{
	fprintf(stderr, "Start basic_insert_list_test %d\n", options);

	int retval;
	rlite *db = NULL;
	rl_list *list = NULL;
	long **vals = malloc(sizeof(long *) * 7);
	long *element;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	db->number_of_databases = 1;
	db->page_size = sizeof(long) * 2 + 12;
	RL_CALL_VERBOSE(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	long list_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, list->type->list_type, list_page, list);

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
			default:
				retval = RL_UNEXPECTED;
				goto cleanup;
		}
		RL_CALL_VERBOSE(rl_list_add_element, RL_OK, db, list, list_page, vals[i], position);
		RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);
	}

	for (i = 0; i < 7; i++) {
		RL_CALL_VERBOSE(rl_list_find_element, RL_FOUND, db, list, vals[i], NULL, &position, NULL, NULL);
		EXPECT_LONG(position, (options % 2 == 0 ? i : (6 - i)));
	}
	for (i = 0; i < 7; i++) {
		RL_CALL_VERBOSE(rl_list_get_element, RL_FOUND, db, list, (void **)&element, i);
		EXPECT_LONG(*element, *vals[(options % 2 == 0 ? i : (6 - i))]);
		RL_CALL_VERBOSE(rl_list_get_element, RL_FOUND, db, list, (void **)&element, - 7 + i);
		EXPECT_LONG(*element, *vals[(options % 2 == 0 ? i : (6 - i))]);
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		RL_CALL_VERBOSE(rl_list_find_element, RL_NOT_FOUND, db, list, &nonexistent_vals[i], NULL, NULL, NULL, NULL);
	}
	fprintf(stderr, "End basic_insert_list_test\n");
	retval = 0;
cleanup:
	free(vals);
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
	RL_CALL_VERBOSE(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	long list_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, list->type->list_type, list_page, list);

	long i, *element;
	for (i = 0; i < ITERATOR_SIZE; i++) {
		element = malloc(sizeof(long));
		*element = i;
		RL_CALL_VERBOSE(rl_list_add_element, RL_OK, db, list, list_page, element, -1);
	}

	RL_COMMIT();
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_list_long, list_page, &rl_list_type_long, (void **)&list, 1);
	}

	rl_list_iterator *iterator;
	RL_CALL_VERBOSE(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);

	i = 0;
	void *tmp;
	long val;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		val = *(long *)tmp;
		EXPECT_LONG(val, i);
		rl_free(tmp);
		i++;
	}

	if (retval != RL_END) {
		rl_free(tmp);
		goto cleanup;
	}

	EXPECT_LONG(i, ITERATOR_SIZE);
	fprintf(stderr, "End basic_iterator_list_test %d\n", _commit);
	retval = 0;
cleanup:
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
	RL_CALL_VERBOSE(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	long list_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, list->type->list_type, list_page, list);

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
		RL_CALL_VERBOSE(rl_list_add_element, RL_OK, db, list, list_page, element_copy, positive ? position : (- i + position - 1));
		RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);
		rl_flatten_list(db, list, flatten_elements);
		for (j = 0; j < list->size; j++) {
			EXPECT_LONG(*(long *)flatten_elements[j], elements[j]);
		}
		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_list_long, list_page, &rl_list_type_long, (void **)&list, 1);
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
		RL_CALL_VERBOSE(rl_list_find_element, RL_FOUND, db, list, &elements[i], NULL, NULL, NULL, NULL);
		RL_CALL_VERBOSE(rl_list_find_element, RL_NOT_FOUND, db, list, &nonelements[i], NULL, NULL, NULL, NULL);
	}
	fprintf(stderr, "End fuzzy_list_test\n");

	retval = RL_OK;
cleanup:
	free(elements);
	free(nonelements);
	free(flatten_elements);
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
	RL_CALL_VERBOSE(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	long list_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, list->type->list_type, list_page, list);
	long pos_element_to_remove = element_to_remove >= 0 ? (element_to_remove) : (elements + element_to_remove);
	long i, j;
	for (i = 0; i < elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
		RL_CALL_VERBOSE(rl_list_add_element, RL_OK, db, list, list_page, vals[i], i);
		RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);
	}

	RL_CALL2_VERBOSE(rl_list_remove_element, RL_OK, RL_DELETED, db, list, list_page, element_to_remove);

	if (retval == RL_DELETED) {
		if (elements == 1) {
			fprintf(stderr, "End basic_delete_list_test (%ld, %ld)\n", elements, element_to_remove);
			retval = RL_OK;
			goto cleanup;
		} else {
			fprintf(stderr, "Deleted list that was not supposed to be deleted on line %d\n", __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	}

	RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);

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
		RL_CALL_VERBOSE(rl_list_find_element, expected, db, list, element, NULL, NULL, NULL, NULL);
	}

	fprintf(stderr, "End basic_delete_list_test (%ld, %ld)\n", elements, element_to_remove);
	retval = RL_OK;
cleanup:
	free(vals);
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
	RL_CALL_VERBOSE(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	long list_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, list->type->list_type, list_page, list);

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
			RL_CALL_VERBOSE(rl_list_add_element, RL_OK, db, list, list_page, element_copy, -1);
			RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);
		}
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_list_long, list_page, &rl_list_type_long, (void **)&list, 1);
	}

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		retval = rl_list_remove_element(db, list, list_page, i);
		if (retval == RL_DELETED && size == 1) {
			break;
		}
		if (RL_OK != retval) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			goto cleanup;
		}

		RL_CALL_VERBOSE(rl_list_is_balanced, RL_OK, db, list);
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_list_delete_test\n");

	retval = RL_OK;
cleanup:
	rl_free(elements);
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

	for (i = 0; i < 3; i++) {
		RL_TEST(basic_iterator_list_test, i);
	}

	for (i = 0; i < 2; i++) {
		size = i == 0 ? 100 : 200;
		for (j = 0; j < 2; j++) {
			list_node_size = j == 0 ? 2 : 10;
			for (k = 0; k < 3; k++) {
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
			for (k = 0; k < 3; k++) {
				commit = k;
				srand(1);
				RL_TEST(fuzzy_list_delete_test, size, list_node_size, commit);
			}
		}
	}
}
RL_TEST_MAIN_END
