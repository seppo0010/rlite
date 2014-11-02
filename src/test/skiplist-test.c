#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../page_skiplist.h"
#include "../obj_string.h"

int basic_skiplist_test(int sign, int commit)
{
	fprintf(stderr, "Start basic_skiplist_test %d %d\n", sign, commit);

	rlite *db = setup_db(commit, 1);
	rl_skiplist *skiplist;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i, page;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < 200; i++) {
		data[0] = i;
		retval = rl_obj_string_set(db, &page, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to set %ld string, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_add(db, skiplist, 5.2 * i * sign, page);
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
	long i, page, size;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	unsigned char *data2;
	for (i = 0; i < 10; i++) {
		data[0] = i;
		retval = rl_obj_string_set(db, &page, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to set %ld string, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_add(db, skiplist, i, page);
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
		retval = rl_skiplist_first_node(db, skiplist, i == 0 ? 1.5 : 2.0, &node, &rank);
		if (retval != RL_FOUND) {
			fprintf(stderr, "Expected to find a node with a score bigger than 2.0, got %d\n", retval);
			return 1;
		}

		if (node->score != 2.0) {
			fprintf(stderr, "Expected node score to be 2.0, got %lf instead\n", node->score);
			return 1;
		}

		retval = rl_obj_string_get(db, node->value, &data2, &size);
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

	retval = rl_skiplist_first_node(db, skiplist, 20.0, NULL, NULL);
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
cleanup:
	return retval;
}
