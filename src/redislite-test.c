#include "btree.h"
#include <stdlib.h>
#include <stdio.h>

static void *getter(void *tree, long number)
{
	return (void *)number;
}

static long setter(void *tree, void *node)
{
	return (long)node;
}

int basic_set_test()
{
	fprintf(stderr, "Start basic_set_test\n");
	rl_accessor accessor;
	accessor.getter = getter;
	accessor.setter = setter;
	init_long_set();
	rl_tree *tree = rl_tree_create(&long_set, 2, &accessor);
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
	rl_accessor accessor;
	accessor.getter = getter;
	accessor.setter = setter;
	init_long_set();
	rl_tree *tree = rl_tree_create(&long_set, tree_node_size, &accessor);

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

cleanup:
	return retval;
}
