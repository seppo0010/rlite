#include "btree.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

typedef struct rl_test_context {
	long size;
	rl_tree_node **nodes;
} rl_test_context;

static void *_select(void *tree, long number)
{
	rl_test_context *context = (rl_test_context *)((rl_tree *)tree)->accessor->context;
	return context->nodes[number];
}

static long insert(void *tree, long *number, void *node)
{
	rl_test_context *context = (rl_test_context *)((rl_tree *)tree)->accessor->context;
	context->nodes[context->size] = node;
	*number = context->size++;
	return 0;
}

static long update(void *tree, long *number, void *node)
{
	rl_test_context *context = (rl_test_context *)((rl_tree *)tree)->accessor->context;
	long i;
	for (i = 0; i < context->size; i++) {
		if (context->nodes[i] == node) {
			// this doesn't make sense now, but it should involve some serialization
			// context->nodes[i] = node;
			if (number) {
				*number = i;
			}
			return 0;
		}
	}
	return -1;
}

static long list(void *tree, rl_tree_node *** nodes, long *size)
{
	rl_test_context *context = (rl_test_context *)((rl_tree *)tree)->accessor->context;
	*nodes = context->nodes;
	*size = context->size;
	return 0;
}

rl_accessor *accessor_long_set_create(void *context)
{
	rl_accessor *accessor = malloc(sizeof(rl_accessor));
	if (accessor == NULL) {
		fprintf(stderr, "Failed to create accessor\n");
		return NULL;
	}
	accessor->select = _select;
	accessor->insert = insert;
	accessor->update = update;
	accessor->list = list;
	accessor->context = context;
	return accessor;
}

int basic_set_serde_test()
{
	fprintf(stderr, "Start basic_set_serde_test\n");
	rl_test_context *context = malloc(sizeof(rl_test_context));
	context->size = 0;
	context->nodes = malloc(sizeof(rl_tree_node *) * 100);

	init_long_set();
	rl_accessor *accessor = accessor_long_set_create(context);
	rl_tree *tree = rl_tree_create(&long_set, 10, accessor);

	long vals[7] = {1, 2, 3, 4, 5, 6, 7};
	int i;
	for (i = 0; i < 7; i++) {
		if (0 != rl_tree_add_child(tree, &vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %d\n", i);
			return 1;
		}
	}

	long data_size;
	unsigned char *data;
	tree->type->serialize(tree, _select(tree, tree->root), &data, &data_size);
	unsigned char expected[128];
	memset(expected, 0, 128);
	expected[3] = 7; // length
	for (i = 1; i < 8; i++) {
		expected[8 * i - 1] = i;
	}
	for (i = 0; i < data_size; i++) {
		if (data[i] != expected[i]) {
			fprintf(stderr, "Unexpected value in position %d (got %d, expected %d)\n", i, data[i], expected[i]);
			return 1;
		}
	}
	free(data);

	fprintf(stderr, "End basic_set_serde_test\n");

	if (0 != rl_tree_destroy(tree)) {
		fprintf(stderr, "Failed to destroy tree\n");
		return 1;
	}
	free(context->nodes);
	free(context);
	free(accessor);
	return 0;
}

int basic_set_test()
{
	fprintf(stderr, "Start basic_set_test\n");
	rl_test_context *context = malloc(sizeof(rl_test_context));
	context->size = 0;
	context->nodes = malloc(sizeof(rl_tree_node *) * 100);

	init_long_set();
	rl_accessor *accessor = accessor_long_set_create(context);
	rl_tree *tree = rl_tree_create(&long_set, 2, accessor);
	long vals[7] = {1, 2, 3, 4, 5, 6, 7};
	int i;
	for (i = 0; i < 7; i++) {
		if (0 != rl_tree_add_child(tree, &vals[i], NULL)) {
			fprintf(stderr, "Failed to add child %d\n", i);
			return 1;
		}
	}
	// rl_print_tree(tree);
	for (i = 0; i < 7; i++) {
		if (1 != rl_tree_find_score(tree, &vals[i], NULL, NULL)) {
			fprintf(stderr, "Failed to find child %d\n", i);
			return 1;
		}
	}
	long nonexistent_vals[2] = {0, 8};
	for (i = 0; i < 2; i++) {
		if (0 != rl_tree_find_score(tree, &nonexistent_vals[i], NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %d\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End basic_set_test\n");
	if (0 != rl_tree_destroy(tree)) {
		fprintf(stderr, "Failed to destroy tree\n");
		return 1;
	}
	free(context->nodes);
	free(context);
	free(accessor);
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
int fuzzy_set_test(long size, long tree_node_size)
{
	fprintf(stderr, "Start fuzzy_set_test %ld %ld\n", size, tree_node_size);
	rl_test_context *context = malloc(sizeof(rl_test_context));
	context->size = 0;
	context->nodes = malloc(sizeof(rl_tree_node *) * size);
	init_long_set();
	rl_accessor *accessor = accessor_long_set_create(context);
	rl_tree *tree = rl_tree_create(&long_set, tree_node_size, accessor);

	long i, element;
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
			if (0 != rl_tree_add_child(tree, &elements[i], NULL)) {
				fprintf(stderr, "Failed to add child %ld\n", i);
				return 1;
			}
		}
		flatten_size = 0;
		rl_flatten_tree(tree, &flatten_scores, &flatten_size);
		for (j = 1; j < flatten_size; j++) {
			if (*(long *)flatten_scores[j - 1] >= *(long *)flatten_scores[j]) {
				fprintf(stderr, "Tree is in a bad state in element %ld after adding child %ld\n", j, i);
				return 1;
			}
		}
	}

	// rl_print_tree(tree);

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
		if (1 != rl_tree_find_score(tree, &elements[i], NULL, NULL)) {
			fprintf(stderr, "Failed to find child %ld (%ld)\n", i, elements[i]);
			return 1;
		}
		if (0 != rl_tree_find_score(tree, &nonelements[i], NULL, NULL)) {
			fprintf(stderr, "Failed to not find child %ld\n", i);
			return 1;
		}
	}
	fprintf(stderr, "End fuzzy_set_test\n");

	if (0 != rl_tree_destroy(tree)) {
		fprintf(stderr, "Failed to destroy tree\n");
		return 1;
	}

	free(elements);
	free(nonelements);
	free(flatten_scores);
	free(context->nodes);
	free(context);
	free(accessor);
	return 0;
}

int main()
{
	int retval = 0;
	retval = basic_set_test();
	if (retval != 0) {
		goto cleanup;
	}
	srand(1);
	retval = fuzzy_set_test(100, 2);
	if (retval != 0) {
		goto cleanup;
	}
	srand(1);
	retval = fuzzy_set_test(200, 2);
	if (retval != 0) {
		goto cleanup;
	}
	srand(1);
	retval = fuzzy_set_test(200, 10);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_set_serde_test();
	if (retval != 0) {
		goto cleanup;
	}

cleanup:
	return retval;
}
