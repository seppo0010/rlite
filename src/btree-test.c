#include "btree.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

typedef struct rl_test_context {
	long size;
	unsigned char **serialized_nodes;
	rl_btree_node **active_nodes;
} rl_test_context;

static rl_btree_node *_select(rl_btree *btree, long _number)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
	long number = _number - 1;
	if (!context->active_nodes[number]) {
		rl_btree_node *node;
		if (0 != btree->type->deserialize(btree, context->serialized_nodes[number], &node)) {
			fprintf(stderr, "Failed to deserialize node %ld\n", number);
			return NULL;
		}
		context->active_nodes[number] = (rl_btree_node *)node;
	}
	return context->active_nodes[number];
}

static long insert(rl_btree *btree, long *number, rl_btree_node *node)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
	context->active_nodes[context->size] = node;
	*number = ++context->size;
	return 0;
}

static long update(rl_btree *btree, long *number, rl_btree_node *node)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
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

static long _remove(rl_btree *btree, rl_btree_node *node)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
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

static long list(rl_btree *btree, rl_btree_node *** nodes, long *size)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		*nodes[i] = _select(btree, i);
	}
	*size = context->size;
	return 0;
}

static long discard(rl_btree *btree)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i]) {
			rl_btree_node_destroy(btree, context->active_nodes[i]);
			context->active_nodes[i] = NULL;
		}
	}
	return 0;
}

static long commit(rl_btree *btree)
{
	rl_test_context *context = (rl_test_context *)btree->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->active_nodes[i]) {
			if (context->serialized_nodes[i]) {
				free(context->serialized_nodes[i]);
			}
			btree->type->serialize(btree, context->active_nodes[i], &context->serialized_nodes[i], NULL);
			rl_btree_node_destroy(btree, context->active_nodes[i]);
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
	context->active_nodes = calloc(size, sizeof(rl_btree_node *));
	return context;
}

void context_destroy(rl_btree *btree, rl_test_context *context)
{
	discard(btree);
	long i;
	for (i = 0; i < context->size; i++) {
		free(context->serialized_nodes[i]);
	}
	free(context->serialized_nodes);
	free(context->active_nodes);
	free(context);
}

rl_accessor *accessor_create(rl_test_context *context)
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

int basic_set_serde_test()
{
	fprintf(stderr, "Start basic_set_serde_test\n");

	init_long_set();
	rl_test_context *context = context_create(100);
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_set, 10, accessor);

	long **vals = malloc(sizeof(long *) * 7);
	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i;
	}
	for (i = 0; i < 7; i++) {
		if (0 != rl_btree_add_element(btree, vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_btree_is_balanced(btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	long data_size;
	unsigned char *data;
	btree->type->serialize(btree, _select(btree, btree->root), &data, &data_size);
	unsigned char expected[128];
	memset(expected, 0, 128);
	expected[3] = 7; // length
	for (i = 1; i < 8; i++) {
		expected[8 * i - 1] = i;
	}
	for (i = 0; i < data_size; i++) {
		if (data[i] != expected[i]) {
			fprintf(stderr, "Unexpected value in position %ld (got %d, expected %d)\n", i, data[i], expected[i]);
			return 1;
		}
	}
	free(data);

	fprintf(stderr, "End basic_set_serde_test\n");

	context_destroy(btree, context);
	free(accessor);
	free(vals);
	free(btree);
	return 0;
}

int basic_insert_set_test()
{
	fprintf(stderr, "Start basic_insert_set_test\n");
	rl_test_context *context = context_create(100);

	init_long_set();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_set, 2, accessor);
	long **vals = malloc(sizeof(long *) * 7);
	long i;
	for (i = 0; i < 7; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = i + 1;
	}
	for (i = 0; i < 7; i++) {
		if (0 != rl_btree_add_element(btree, vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_btree_is_balanced(btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}
	// rl_print_btree(btree);
	for (i = 0; i < 7; i++) {
		if (1 != rl_btree_find_score(btree, vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld\n", i);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (0 != rl_btree_find_score(btree, &nonexistent_vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	context_destroy(btree, context);
	free(accessor);
	free(vals);
	free(btree);
	return 0;
}

int basic_insert_hash_test()
{
	fprintf(stderr, "Start basic_insert_hash_test\n");
	rl_test_context *context = context_create(100);

	init_long_hash();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_hash, 2, accessor);
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
		if (0 != rl_btree_add_element(btree, keys[i], vals[i])) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_btree_is_balanced(btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_btree(btree);

	void *val;
	for (i = 0; i < 7; i++) {
		if (1 != rl_btree_find_score(btree, keys[i], &val, NULL, NULL)) {
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
		if (0 != rl_btree_find_score(btree, &nonexistent_vals[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_insert_set_test\n");
	context_destroy(btree, context);
	free(accessor);
	free(vals);
	free(keys);
	free(btree);
	return 0;
}

int basic_delete_set_test(long elements, long element_to_remove, char *name)
{
	fprintf(stderr, "Start basic_delete_set_test (%ld, %ld) (%s)\n", elements, element_to_remove, name);
	rl_test_context *context = context_create(100);

	init_long_set();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_set, 2, accessor);
	long abs_elements = labs(elements);
	long pos_element_to_remove = abs_elements == elements ? element_to_remove : (-element_to_remove);
	long **vals = malloc(sizeof(long *) * abs_elements);
	long i, j;
	for (i = 0; i < abs_elements; i++) {
		vals[i] = malloc(sizeof(long));
		*vals[i] = elements == abs_elements ? i + 1 : (-i - 1);
	}
	for (i = 0; i < abs_elements; i++) {
		if (0 != rl_btree_add_element(btree, vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %ld\n", i);
			return 1;
		}
		if (0 == rl_btree_is_balanced(btree)) {
			fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
			return 1;
		}
	}

	// rl_print_btree(btree);

	if (0 != rl_btree_remove_element(btree, vals[pos_element_to_remove - 1])) {
		fprintf(stderr, "Failed to remove child %ld\n", element_to_remove - 1);
		return 1;
	}

	// rl_print_btree(btree);

	if (0 == rl_btree_is_balanced(btree)) {
		fprintf(stderr, "Node is not balanced after removing child %ld\n", element_to_remove - 1);
		return 1;
	}

	int expected;
	long score[1];
	for (j = 0; j < abs_elements; j++) {
		expected = j == pos_element_to_remove - 1;
		if (j == pos_element_to_remove - 1) {
			expected = 0;
			*score = element_to_remove;
		}
		else {
			expected = 1;
			*score = *vals[j];
		}
		if (expected != rl_btree_find_score(btree, score, NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to %sfind child %ld (%ld) after deleting element %ld\n", expected == 0 ? "" : "not ", j, *vals[j], element_to_remove);
			return 1;
		}
	}

	fprintf(stderr, "End basic_delete_set_test (%ld, %ld)\n", elements, element_to_remove);
	context_destroy(btree, context);
	free(accessor);
	free(vals);
	free(btree);
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
	rl_test_context *context = context_create(size);
	init_long_set();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_set, btree_node_size, accessor);

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
			if (0 != rl_btree_add_element(btree, element_copy, NULL)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (0 == rl_btree_is_balanced(btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
				return 1;
			}
		}
		flatten_size = 0;
		rl_flatten_btree(btree, &flatten_scores, &flatten_size);
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				return 1;
			}
		}
		if (_commit) {
			commit(btree);
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
		if (1 != rl_btree_find_score(btree, &elements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (0 != rl_btree_find_score(btree, &nonelements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_set_test\n");

	context_destroy(btree, context);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	free(accessor);
	free(btree);
	return 0;
}

int fuzzy_hash_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_hash_test %ld %ld %d\n", size, btree_node_size, _commit);
	rl_test_context *context = context_create(size);
	init_long_hash();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_hash, btree_node_size, accessor);

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
			if (0 != rl_btree_add_element(btree, element_copy, value_copy)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (0 == rl_btree_is_balanced(btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld (%ld)\n", i, value);
				return 1;
			}
		}
		flatten_size = 0;
		rl_flatten_btree(btree, &flatten_scores, &flatten_size);
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				return 1;
			}
		}
		if (_commit) {
			commit(btree);
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
		if (1 != rl_btree_find_score(btree, &elements[i], &val, NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (*(long *)val != values[i]) {
			fprintf(stderr, "Value doesn't match expected value at position %ld (%ld) (%ld != %ld)\n", i, elements[i], *(long *)val, values[i]);
			return 1;
		}
		if (0 != rl_btree_find_score(btree, &nonelements[i], NULL, NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_hash_test\n");

	context_destroy(btree, context);
	free(values);
	free(elements);
	free(nonelements);
	free(flatten_scores);
	free(accessor);
	free(btree);
	return 0;
}

int fuzzy_set_delete_test(long size, long btree_node_size, int _commit)
{
	fprintf(stderr, "Start fuzzy_set_delete_test %ld %ld %d\n", size, btree_node_size, _commit);
	rl_test_context *context = context_create(size);
	init_long_set();
	rl_accessor *accessor = accessor_create(context);
	rl_btree *btree = rl_btree_create(&long_set, btree_node_size, accessor);

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
			if (0 != rl_btree_add_element(btree, element_copy, NULL)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
			if (0 == rl_btree_is_balanced(btree)) {
				fprintf(stderr, "Node is not balanced after adding child %ld\n", i);
				return 1;
			}
		}
		if (_commit) {
			commit(btree);
		}
	}

	// rl_print_btree(btree);

	while (size > 0) {
		i = (long)(((float)rand() / RAND_MAX) * size);
		if (0 != rl_btree_remove_element(btree, &elements[i])) {
			fprintf(stderr, "Failed to delete child %ld\n", elements[i]);
			return 1;
		}

		// rl_print_btree(btree);

		if (0 == rl_btree_is_balanced(btree)) {
			fprintf(stderr, "Node is not balanced after deleting child %ld\n", i);
			return 1;
		}
		elements[i] = elements[size - 1];
		size--;
	}
	fprintf(stderr, "End fuzzy_set_delete_test\n");

	context_destroy(btree, context);
	free(elements);
	free(accessor);
	free(btree);
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
