#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../page_skiplist.h"
#include "../page_multi_string.h"

#define TEST_SIZE 100

int basic_skiplist_test(int sign, int commit)
{
	fprintf(stderr, "Start basic_skiplist_test %d %d\n", sign, commit);

	rlite *db = setup_db(commit, 1);
	rl_skiplist *skiplist;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i * sign, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			return 1;
		}
		if (commit) {
			rl_commit(db);
		}
	}
	rl_skiplist_destroy(db, skiplist);
	free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_test\n");
	return 0;
}

int basic_skiplist_first_node_test()
{
	fprintf(stderr, "Start basic_skiplist_first_node_test\n");

	rlite *db = setup_db(0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	long rank;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i, size;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	unsigned char *data2;
	for (i = 0; i < 10; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			return 1;
		}
	}

	for (i = 0; i < 2; i++) {
		retval = rl_skiplist_first_node(db, skiplist, i == 0 ? 1.5 : 2.0, NULL, 0, &node, &rank);
		if (retval != RL_FOUND) {
			fprintf(stderr, "Expected to find a node with a score bigger than 2.0, got %d\n", retval);
			return 1;
		}

		if (node->score != 2.0) {
			fprintf(stderr, "Expected node score to be 2.0, got %lf instead\n", node->score);
			return 1;
		}
		if (rank != 2) {
			fprintf(stderr, "Expected rank to be 2, got %ld instead\n", rank);
			return 1;
		}

		retval = rl_multi_string_get(db, node->value, &data2, &size);
		if (retval != RL_OK) {
			fprintf(stderr, "Error fetching string value %d\n", retval);
			return 1;
		}

		if (size != 1) {
			fprintf(stderr, "Expected data size to be 1, got %ld instead\n", size);
			return 1;
		}

		if (data2[0] != 2) {
			fprintf(stderr, "Expected data to be 2, got %d instead\n", data2[0]);
			return 1;
		}
		free(data2);
	}

	retval = rl_skiplist_first_node(db, skiplist, 20.0, NULL, 0, NULL, NULL);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Expected to not find any node after 20, got %d\n", retval);
		return 1;
	}

	rl_skiplist_destroy(db, skiplist);
	free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_first_node_test\n");
	return 0;
}

int basic_skiplist_delete_node_test(int commit)
{
	fprintf(stderr, "Start basic_skiplist_delete_node_test %d\n", commit);

	rlite *db = setup_db(commit, 1);
	rl_skiplist *skiplist;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i;
	unsigned char *data[10];
	for (i = 0; i < 10; i++) {
		data[i] = malloc(sizeof(unsigned char) * 1);
		data[i][0] = i;
		// using i / 2 to have some score collisions because i is int
		retval = rl_skiplist_add(db, skiplist, i / 2, data[i], 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			return 1;
		}
		if (commit) {
			if (RL_OK != rl_commit(db)) {
				fprintf(stderr, "Failed to commit in line %d\n", __LINE__);
			}
		}
	}

	for (i = 0; i < 10; i++) {
		retval = rl_skiplist_delete(db, skiplist, i / 2, data[i], 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Failed to delete node at position %ld with score %lf and value %u, got %d\n", i, (double)(i / 2), *data[i], retval);
			return 1;
		}

		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after removing item %ld\n", i);
			return 1;
		}

		if (commit) {
			if (RL_OK != rl_commit(db)) {
				fprintf(stderr, "Failed to commit in line %d\n", __LINE__);
			}
		}
	}

	rl_skiplist_destroy(db, skiplist);
	for (i = 0; i < 10; i++) {
		free(data[i]);
	}
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_delete_node_test\n");
	return 0;
}

int basic_skiplist_node_by_rank()
{
	fprintf(stderr, "Start basic_skiplist_node_by_rank\n");

	rlite *db = setup_db(0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
	}

	for (i = 0; i < TEST_SIZE; i++) {
		retval = rl_skiplist_node_by_rank(db, skiplist, i, &node);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to get %ld node by rank, got %d\n", i, retval);
			return 1;
		}
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected %ld node score to be %lf, got %lf\n", i, 5.2 * i, node->score);
			return 1;
		}
	}
	rl_skiplist_destroy(db, skiplist);
	free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_node_by_rank\n");
	return 0;
}

int basic_skiplist_iterator_test(int commit)
{
	fprintf(stderr, "Start basic_skiplist_iterator_test %d\n", commit);

	rlite *db = setup_db(commit, 1);
	rl_skiplist_iterator *iterator;
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval, cmp;
	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
	}

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, 0, 0);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		return 1;
	}

	long random_node = 0;
	i = 0;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (i == TEST_SIZE / 2 - 1) {
			random_node = iterator->node_page;
		}
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			return 1;
		}
		data[0] = i;
		if (RL_OK != rl_multi_string_cmp_str(db, node->value, data, 1, &cmp)) {
			fprintf(stderr, "Unable to compare node vale\n");
			return 1;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			return 1;
		}
		i++;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		return 1;
	}
	if (i != TEST_SIZE) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE, i);
		return 1;
	}
	rl_skiplist_iterator_destroy(db, iterator);

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, -1, 0);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		return 1;
	}

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			return 1;
		}
		data[0] = i;
		if (RL_OK != rl_multi_string_cmp_str(db, node->value, data, 1, &cmp)) {
			fprintf(stderr, "Unable to compare node vale\n");
			return 1;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			return 1;
		}
		i--;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		return 1;
	}
	if (i != -1) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", -1, i);
		return 1;
	}
	rl_skiplist_iterator_destroy(db, iterator);

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, -1, 1);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		return 1;
	}

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			return 1;
		}
		data[0] = i;
		if (RL_OK != rl_multi_string_cmp_str(db, node->value, data, 1, &cmp)) {
			fprintf(stderr, "Unable to compare node vale\n");
			return 1;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			return 1;
		}
		i--;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		return 1;
	}
	if (i != TEST_SIZE - 2) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE - 2, i);
		return 1;
	}
	rl_skiplist_iterator_destroy(db, iterator);

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, random_node, 1, 5);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		return 1;
	}

	i = TEST_SIZE / 2;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			return 1;
		}
		data[0] = i;
		if (RL_OK != rl_multi_string_cmp_str(db, node->value, data, 1, &cmp)) {
			fprintf(stderr, "Unable to compare node vale\n");
			return 1;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			return 1;
		}
		i++;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		return 1;
	}
	if (i != TEST_SIZE / 2 + 5) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE / 2 + 5, i);
		return 1;
	}
	rl_skiplist_iterator_destroy(db, iterator);


	rl_skiplist_destroy(db, skiplist);
	free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_iterator_test\n");
	return 0;
}

int main()
{
	int retval = 0;
	int i, j;
	for (i = -1; i <= 1; i++) {
		for (j = 0; j < 2; j++) {
			retval = basic_skiplist_test(i, j);
			if (retval != 0) {
				goto cleanup;
			}
		}
	}
	retval = basic_skiplist_first_node_test();
	if (retval != 0) {
		goto cleanup;
	}
	for (i = 0; i < 2; i++) {
		retval = basic_skiplist_delete_node_test(i);
		if (retval != 0) {
			goto cleanup;
		}

		retval = basic_skiplist_iterator_test(i);
		if (retval != 0) {
			goto cleanup;
		}
	}

	retval = basic_skiplist_node_by_rank();
	if (retval != 0) {
		goto cleanup;
	}
cleanup:
	return retval;
}
