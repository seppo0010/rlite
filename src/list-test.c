#include "list.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

typedef struct rl_test_context {
	long size;
	unsigned char **serialized_nodes;
	rl_list_node **active_nodes;
} rl_test_context;

static void *_select(void *_list, long _number)
{
	rl_list *list = (rl_list *)_list;
	rl_test_context *context = (rl_test_context *)list->accessor->context;
	long number = _number - 1;
	if (!context->active_nodes[number]) {
		void *node;
		if (0 != list->type->deserialize(list, context->serialized_nodes[number], &node)) {
			fprintf(stderr, "Failed to deserialize node %ld\n", number);
			return NULL;
		}
		context->active_nodes[number] = (rl_list_node *)node;
	}
	return context->active_nodes[number];
}

static long insert(void *list, long *number, void *node)
{
	rl_test_context *context = (rl_test_context *)((rl_list *)list)->accessor->context;
	context->active_nodes[context->size] = node;
	*number = ++context->size;
	return 0;
}

static long update(void *list, long *number, void *node)
{
	rl_test_context *context = (rl_test_context *)((rl_list *)list)->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i] == node) {
			if (number) {
				*number = i + 1;
			}
			return 0;
		}
	}
	return -1;
}

static long _remove(void *list, void *node)
{
	rl_test_context *context = (rl_test_context *)((rl_list *)list)->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i] == node) {
			context->active_nodes[i] = NULL;
			free(context->serialized_nodes[i]);
			context->serialized_nodes[i] = NULL;
			return 0;
		}
	}
	return -1;
}

static long list(void *list, rl_list_node *** nodes, long *size)
{
	rl_test_context *context = (rl_test_context *)((rl_list *)list)->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		nodes[i] = _select(list, i);
	}
	*size = context->size;
	return 0;
}

static long discard(void *list)
{
	rl_test_context *context = (rl_test_context *)((rl_list *)list)->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i]) {
			rl_list_node_destroy(list, context->active_nodes[i]);
			context->active_nodes[i] = NULL;
		}
	}
	return 0;
}

static long commit(void *_list)
{
	rl_list *list = (rl_list *)_list;
	rl_test_context *context = (rl_test_context *)list->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i]) {
			if (context->serialized_nodes[i]) {
				free(context->serialized_nodes[i]);
			}
			list->type->serialize(list, context->active_nodes[i], &context->serialized_nodes[i], NULL);
			rl_list_node_destroy(list, context->active_nodes[i]);
			context->active_nodes[i] = NULL;
		}
	}
	return 0;
}

rl_test_context *context_create(long size)
{
	rl_test_context *context = malloc(sizeof(rl_test_context));
	context->size = 0;
	context->serialized_nodes = calloc(size, sizeof(unsigned char **));
	context->active_nodes = calloc(size, sizeof(rl_list_node *));
	return context;
}

void context_destroy(rl_list *list, rl_test_context *context)
{
	discard(list);
	long i;
	for (i = 0; i < context->size; i++) {
		free(context->serialized_nodes[i]);
	}
	free(context->serialized_nodes);
	free(context->active_nodes);
	free(context);
}

rl_accessor *accessor_create(void *context)
{
	rl_accessor *accessor = malloc(sizeof(rl_accessor));
	if (accessor == NULL) {
		fprintf(stderr, "Failed to create accessor\n");
		return NULL;
	}
	accessor->select = _select;
	accessor->insert = insert;
	accessor->update = update;
	accessor->remove = _remove;
	accessor->list = list;
	accessor->commit = commit;
	accessor->discard = discard;
	accessor->context = context;
	return accessor;
}

int basic_insert_list_test(int options)
{
	fprintf(stderr, "Start basic_insert_list_test %d\n", options);
	rl_test_context *context = context_create(100);

	init_long_list();
	rl_accessor *accessor = accessor_create(context);
	rl_list *list = rl_list_create(&long_list, 2, accessor);
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
		if (0 != rl_list_add_element(list, vals[i], position)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_list_is_balanced(list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_list(list);

	for (i = 0; i < 7; i++) {
		if (1 != rl_list_find_element(list, vals[i], &position)) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			return 1;
		}
		if (position != (options % 2 == 0 ? i : (6 - i))) {
			fprintf(stderr, "Unexpected position of item %ld, %ld\n", (options % 2 == 0 ? i : (6 - i)), position);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (0 != rl_list_find_element(list, &nonexistent_vals[i], NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_list_test\n");
	context_destroy(list, context);
	free(accessor);
	free(vals);
	free(list);
	return 0;
}

int basic_list_serde_test()
{
	fprintf(stderr, "Start basic_list_serde_test\n");

	init_long_list();
	rl_test_context *context = context_create(100);
	rl_accessor *accessor = accessor_create(context);
	rl_list *list = rl_list_create(&long_list, 10, accessor);

	long **vals = malloc(sizeof(long *) * 7);
	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
	}
	for (i = 0; i < 7; i++) {
		if (0 != rl_list_add_element(list, vals[i], i)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_list_is_balanced(list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	long data_size;
	unsigned char *data;
	list->type->serialize(list, _select(list, list->left), &data, &data_size);
	unsigned char expected[128];
	memset(expected, 0, 128);
	expected[3] = 7; // length
	for (i = 1; i < 8; i++) {
		expected[4 * i + 15] = i;
	}
	for (i = 0; i < data_size; i++) {
		if (data[i] != expected[i]) {
			fprintf(stderr, "Unexpected value in position %ld (got %d, expected %d)\n", i, data[i], expected[i]);
			return 1;
		}
	}
	free(data);

	fprintf(stderr, "End basic_list_serde_test\n");

	context_destroy(list, context);
	free(accessor);
	free(vals);
	free(list);
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
	rl_test_context *context = context_create(size);
	init_long_list();
	rl_accessor *accessor = accessor_create(context);
	rl_list *list = rl_list_create(&long_list, list_node_size, accessor);

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
		if (0 != rl_list_add_element(list, element_copy, positive ? position : (- i + position - 1))) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_list_is_balanced(list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
		rl_flatten_list(list, &flatten_elements);
		for (j = 0; j < list->size; j++) {
			if (*(long *)flatten_elements[j] != elements[j]) {
				fprintf(stderr, "Unexpected value in position %ld after adding %ld (expected %ld, got %ld)\n", j, i, elements[j], *(long *)flatten_elements[j]);
				return 1;
			}
		}
		if (_commit) {
			commit(list);
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
		if (1 != rl_list_find_element(list, &elements[i], NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (0 != rl_list_find_element(list, &nonelements[i], NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_list_test\n");

	context_destroy(list, context);
	free(elements);
	free(nonelements);
	free(flatten_elements);
	free(accessor);
	free(list);
	return 0;
}

int basic_delete_list_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_list_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);
	rl_test_context *context = context_create(100);

	init_long_list();
	rl_accessor *accessor = accessor_create(context);
	rl_list *list = rl_list_create(&long_list, 2, accessor);
	long pos_element_to_remove = element_to_remove >= 0 ? (element_to_remove) : (elements + element_to_remove);
	long **vals = malloc(sizeof(long *) * elements);
	long i, j;
	for (i = 0; i < elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
	}
	for (i = 0; i < elements; i++) {
		if (0 != rl_list_add_element(list, vals[i], i)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_list_is_balanced(list)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_list(list);

	if (0 != rl_list_remove_element(list, element_to_remove)) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		return 1;
	}

	// rl_print_list(list);

	if (0 == rl_list_is_balanced(list)) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		return 1;
	}

	int expected;
	long element[1];
	for (j = 0; j < elements; j++) {
		if (j == pos_element_to_remove) {
			expected = 0;
			*element = element_to_remove;
		}
		else {
			expected = 1;
			*element = *vals[j];
		}
		if (expected != rl_list_find_element(list, element, NULL)) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == 0 ? "" : "not ", j, *vals[j], element_to_remove);
			return 1;
		}
	}

	fprintf(stderr, "End basic_delete_list_test (%ld, %ld)\n", elements, element_to_remove);
	context_destroy(list, context);
	free(accessor);
	free(vals);
	free(list);
	return 0;
}

int fuzzy_list_delete_test(long size, long list_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_list_delete_test %ld %ld %d\n", size, list_node_size, _commit);
	rl_test_context *context = context_create(size);
	init_long_list();
	rl_accessor *accessor = accessor_create(context);
	rl_list *list = rl_list_create(&long_list, list_node_size, accessor);

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
			if (0 != rl_list_add_element(list, element_copy, -1)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (0 == rl_list_is_balanced(list)) {
				fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
				return 1;
			}
		}
		if (_commit) {
			commit(list);
		}
	}

	// rl_print_list(list);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		if (0 != rl_list_remove_element(list, i)) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			return 1;
		}

		// rl_print_list(list);

		if (0 == rl_list_is_balanced(list)) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			return 1;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_list_delete_test\n");

	context_destroy(list, context);
	free(elements);
	free(accessor);
	free(list);
	return 0;
}

#define DELETE_TESTS_COUNT 5
int main()
{
	int i, j, k;
	long size, list_node_size;
	int commit;
	int retval = 0;
	/*
	for (i = 0; i < 4; i++) {
		retval = basic_insert_list_test(i);
		if (retval != 0) {
			goto cleanup;
		}
	}
	retval = basic_list_serde_test();
	if (retval != 0) {
		goto cleanup;
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
	*/
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
