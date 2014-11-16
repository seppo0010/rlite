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

	rlite *db;
	int retval;
	RL_CALL(setup_db, RL_OK, &db, commit, 1);
	rl_skiplist *skiplist;
	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i * sign, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			goto cleanup;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			goto cleanup;
		}
		if (commit) {
			rl_commit(db);
		}
	}
	rl_skiplist_destroy(db, skiplist);
	rl_free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_test\n");
	retval = 0;
cleanup:
	return retval;
}

int basic_skiplist_first_node_test()
{
	fprintf(stderr, "Start basic_skiplist_first_node_test\n");

	rlite *db;
	int retval;
	RL_CALL(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	long rank;
	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long i, size;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	unsigned char *data2;
	for (i = 0; i < 10; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			goto cleanup;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			goto cleanup;
		}
	}

	for (i = 0; i < 2; i++) {
		retval = rl_skiplist_first_node(db, skiplist, i == 0 ? 1.5 : 2.0, RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, &node, &rank);
		if (retval != RL_FOUND) {
			fprintf(stderr, "Expected to find a node with a score greater than or equal to 2.0, got %d\n", retval);
			goto cleanup;
		}

		if (node->score != 2.0) {
			fprintf(stderr, "Expected node score to be 2.0, got %lf instead on line %d iteration %ld\n", node->score, __LINE__, i);
			retval = 1;
			goto cleanup;
		}
		if (rank != 2) {
			fprintf(stderr, "Expected rank to be 2, got %ld instead iteration %ld\n", rank, i);
			retval = 1;
			goto cleanup;
		}

		retval = rl_multi_string_get(db, node->value, &data2, &size);
		if (retval != RL_OK) {
			fprintf(stderr, "Error fetching string value %d\n", retval);
			goto cleanup;
		}

		if (size != 1) {
			fprintf(stderr, "Expected data size to be 1, got %ld instead\n", size);
			retval = 1;
			goto cleanup;
		}

		if (data2[0] != 2) {
			fprintf(stderr, "Expected data to be 2, got %d instead\n", data2[0]);
			retval = 1;
			goto cleanup;
		}
		rl_free(data2);
	}

	retval = rl_skiplist_first_node(db, skiplist, 2.0, RL_SKIPLIST_EXCLUDE_SCORE, NULL, 0, &node, &rank);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Expected to find a node with a score greater than 2.0, got %d\n", retval);
		goto cleanup;
	}

	if (node->score != 3.0) {
		fprintf(stderr, "Expected node score to be 3.0, got %lf instead %d\n", node->score, __LINE__);
		retval = 1;
		goto cleanup;
	}
	if (rank != 3) {
		fprintf(stderr, "Expected rank to be 3, got %ld instead iteration %ld\n", rank, i);
		retval = 1;
		goto cleanup;
	}

	retval = rl_skiplist_first_node(db, skiplist, 2.0, RL_SKIPLIST_BEFORE_SCORE, NULL, 0, &node, &rank);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Expected to find a node with a score less than 2.0, got %d\n", retval);
		goto cleanup;
	}

	if (node->score != 1.0) {
		fprintf(stderr, "Expected node score to be 1.0, got %lf instead %d\n", node->score, __LINE__);
		retval = 1;
		goto cleanup;
	}
	if (rank != 1) {
		fprintf(stderr, "Expected rank to be 1, got %ld instead iteration %ld\n", rank, i);
		retval = 1;
		goto cleanup;
	}

	retval = rl_skiplist_first_node(db, skiplist, 20.0, RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, NULL, NULL);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Expected to not find any node after 20, got %d\n", retval);
		goto cleanup;
	}

	rl_skiplist_destroy(db, skiplist);
	rl_free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_first_node_test\n");
	retval = 0;
cleanup:
	return retval;
}

int basic_skiplist_delete_node_test(int commit)
{
	fprintf(stderr, "Start basic_skiplist_delete_node_test %d\n", commit);

	rlite *db;
	int retval;
	RL_CALL(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long i;
	unsigned char *data[10];
	for (i = 0; i < 10; i++) {
		data[i] = malloc(sizeof(unsigned char) * 1);
		data[i][0] = i;
		// using i / 2 to have some score collisions because i is int
		retval = rl_skiplist_add(db, skiplist, i / 2, data[i], 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			goto cleanup;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			goto cleanup;
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
			goto cleanup;
		}

		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after removing item %ld\n", i);
			goto cleanup;
		}

		if (commit) {
			if (RL_OK != rl_commit(db)) {
				fprintf(stderr, "Failed to commit in line %d\n", __LINE__);
			}
		}
	}

	rl_skiplist_destroy(db, skiplist);
	for (i = 0; i < 10; i++) {
		rl_free(data[i]);
	}
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_delete_node_test\n");
	retval = 0;
cleanup:
	return retval;
}

int basic_skiplist_node_by_rank()
{
	fprintf(stderr, "Start basic_skiplist_node_by_rank\n");

	rlite *db;
	int retval;
	RL_CALL(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			goto cleanup;
		}
	}

	for (i = 0; i < TEST_SIZE; i++) {
		retval = rl_skiplist_node_by_rank(db, skiplist, i, &node, NULL);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to get %ld node by rank, got %d\n", i, retval);
			goto cleanup;
		}
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected %ld node score to be %lf, got %lf\n", i, 5.2 * i, node->score);
			retval = 1;
			goto cleanup;
		}
	}
	rl_skiplist_destroy(db, skiplist);
	rl_free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_node_by_rank\n");
	retval = 0;
cleanup:
	return retval;
}

int basic_skiplist_iterator_test(int commit)
{
	fprintf(stderr, "Start basic_skiplist_iterator_test %d\n", commit);

	rlite *db;
	int retval;
	RL_CALL(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist_iterator *iterator;
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	retval = rl_skiplist_create(db, &skiplist);
	if (retval != RL_OK) {
		goto cleanup;
	}

	int cmp;
	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		retval = rl_skiplist_add(db, skiplist, 5.2 * i, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			goto cleanup;
		}
	}

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, 0, 0);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		goto cleanup;
	}

	long random_node = 0;
	i = 0;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (i == TEST_SIZE / 2 - 1) {
			random_node = iterator->node_page;
		}
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			retval = 1;
			goto cleanup;
		}
		data[0] = i;
		retval = rl_multi_string_cmp_str(db, node->value, data, 1, &cmp);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to compare node vale\n");
			goto cleanup;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			retval = 1;
			goto cleanup;
		}
		i++;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		goto cleanup;
	}
	if (i != TEST_SIZE) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE, i);
		retval = 1;
		goto cleanup;
	}

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, -1, 0);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		goto cleanup;
	}

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			retval = 1;
			goto cleanup;
		}
		data[0] = i;
		retval = rl_multi_string_cmp_str(db, node->value, data, 1, &cmp);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to compare node vale\n");
			goto cleanup;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			retval = 1;
			goto cleanup;
		}
		i--;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		goto cleanup;
	}
	if (i != -1) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", -1, i);
		retval = 1;
		goto cleanup;
	}

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, 0, -1, 1);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		goto cleanup;
	}

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			retval = 1;
			goto cleanup;
		}
		data[0] = i;
		retval = rl_multi_string_cmp_str(db, node->value, data, 1, &cmp);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to compare node vale\n");
			goto cleanup;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			retval = 1;
			goto cleanup;
		}
		i--;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		goto cleanup;
	}
	if (i != TEST_SIZE - 2) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE - 2, i);
		retval = 1;
		goto cleanup;
	}

	retval = rl_skiplist_iterator_create(db, &iterator, skiplist, random_node, 1, 5);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create iterator, got %d\n", retval);
		goto cleanup;
	}

	i = TEST_SIZE / 2;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (node->score != 5.2 * i) {
			fprintf(stderr, "Expected score at position %ld to be %lf, got %lf instead\n", i, 5.2 * i, node->score);
			retval = 1;
			goto cleanup;
		}
		data[0] = i;
		retval = rl_multi_string_cmp_str(db, node->value, data, 1, &cmp);
		if (RL_OK != retval) {
			fprintf(stderr, "Unable to compare node vale\n");
			goto cleanup;
		}
		if (cmp != 0) {
			fprintf(stderr, "Expected value at position %ld to match %d, cmp got %d\n", i, data[0], cmp);
			retval = 1;
			goto cleanup;
		}
		i++;
	}
	if (retval != RL_END) {
		fprintf(stderr, "Expected skiplist last return value to be %d after %ld iterations, got %d instead\n", RL_END, i, retval);
		goto cleanup;
	}
	if (i != TEST_SIZE / 2 + 5) {
		fprintf(stderr, "Expected iterator to have %d iterations, got %ld instead\n", TEST_SIZE / 2 + 5, i);
		retval = 1;
		goto cleanup;
	}


	rl_skiplist_destroy(db, skiplist);
	rl_free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_iterator_test\n");
	retval = 0;
cleanup:
	return retval;
}

RL_TEST_MAIN_START(skiplist_test)
{
	int i, j;
	for (i = -1; i <= 1; i++) {
		for (j = 0; j < 2; j++) {
			RL_TEST(basic_skiplist_test, i, j);
		}
	}
	RL_TEST(basic_skiplist_first_node_test, 0);
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_skiplist_delete_node_test, i);
		RL_TEST(basic_skiplist_iterator_test, i);
	}
	RL_TEST(basic_skiplist_node_by_rank, 0);
}
RL_TEST_MAIN_END
