#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../src/rlite/rlite.h"
#include "../src/rlite/page_skiplist.h"
#include "../src/rlite/page_multi_string.h"
#include "util.h"

#define TEST_SIZE 100

TEST basic_skiplist_test(int sign, int commit)
{
	rlite *db;
	void *tmp;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, commit, 1);
	rl_skiplist *skiplist;
	RL_CALL_VERBOSE(rl_skiplist_create, RL_OK, db, &skiplist);
	long skiplist_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);

	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_skiplist_add, RL_OK, db, skiplist, skiplist_page, 5.2 * i * sign, data, 1);
		RL_CALL_VERBOSE(rl_skiplist_is_balanced, RL_OK, db, skiplist);
		if (commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_skiplist, skiplist_page, NULL, &tmp, 1);
			skiplist = tmp;
		}
	}
	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_skiplist_first_node_test()
{
	rlite *db;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	long rank;
	RL_CALL_VERBOSE(rl_skiplist_create, RL_OK, db, &skiplist);
	long skiplist_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);

	long i, size;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	unsigned char *data2;
	for (i = 0; i < 10; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_skiplist_add, RL_OK, db, skiplist, skiplist_page, i, data, 1);
		RL_CALL_VERBOSE(rl_skiplist_is_balanced, RL_OK, db, skiplist);
	}

	for (i = 0; i < 2; i++) {
		RL_CALL_VERBOSE(rl_skiplist_first_node, RL_FOUND, db, skiplist, i == 0 ? 1.5 : 2.0, RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, &node, &rank);
		EXPECT_DOUBLE(node->score, 2.0);
		EXPECT_LONG(rank, 2);
		RL_CALL_VERBOSE(rl_multi_string_get, RL_OK, db, node->value, &data2, &size);
		EXPECT_LONG(size, 1);
		EXPECT_INT(data2[0], 2);
		rl_free(data2);
	}

	RL_CALL_VERBOSE(rl_skiplist_first_node, RL_FOUND, db, skiplist, 2.0, RL_SKIPLIST_EXCLUDE_SCORE, NULL, 0, &node, &rank);
	EXPECT_DOUBLE(node->score, 3.0);
	EXPECT_LONG(rank, 3);
	RL_CALL_VERBOSE(rl_skiplist_first_node, RL_FOUND, db, skiplist, 2.0, RL_SKIPLIST_BEFORE_SCORE, NULL, 0, &node, &rank);
	EXPECT_DOUBLE(node->score, 1.0);
	EXPECT_LONG(rank, 1);
	RL_CALL_VERBOSE(rl_skiplist_first_node, RL_NOT_FOUND, db, skiplist, 20.0, RL_SKIPLIST_INCLUDE_SCORE, NULL, 0, NULL, NULL);

	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_skiplist_delete_node_test(int commit)
{
	void *tmp;
	rlite *db;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	RL_CALL_VERBOSE(rl_skiplist_create, RL_OK, db, &skiplist);
	long skiplist_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);

	long i;
	unsigned char *data[10];
	for (i = 0; i < 10; i++) {
		data[i] = malloc(sizeof(unsigned char) * 1);
		data[i][0] = i;
		// using i / 2 to have some score collisions because i is int
		RL_CALL_VERBOSE(rl_skiplist_add, RL_OK, db, skiplist, skiplist_page, i / 2, data[i], 1);
		RL_CALL_VERBOSE(rl_skiplist_is_balanced, RL_OK, db, skiplist);

		if (commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_skiplist, skiplist_page, NULL, &tmp, 1);
			skiplist = tmp;
		}
	}

	for (i = 0; i < 10; i++) {
		RL_CALL2_VERBOSE(rl_skiplist_delete, RL_OK, RL_DELETED, db, skiplist, skiplist_page, i / 2, data[i], 1);

		if (retval != RL_DELETED) {
			RL_CALL_VERBOSE(rl_skiplist_is_balanced, RL_OK, db, skiplist);
		}

		if (commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_skiplist, skiplist_page, NULL, &tmp, 1);
			skiplist = tmp;
		}
	}

	for (i = 0; i < 10; i++) {
		rl_free(data[i]);
	}
	rl_close(db);
	PASS();
}

TEST basic_skiplist_node_by_rank()
{
	rlite *db;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	RL_CALL_VERBOSE(rl_skiplist_create, RL_OK, db, &skiplist);
	long skiplist_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);

	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_skiplist_add, RL_OK, db, skiplist, skiplist_page, 5.2 * i, data, 1);
	}

	for (i = 0; i < TEST_SIZE; i++) {
		RL_CALL_VERBOSE(rl_skiplist_node_by_rank, RL_OK, db, skiplist, i, &node, NULL);
		EXPECT_DOUBLE(node->score, 5.2 * i)
	}
	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_skiplist_iterator_test(int commit)
{
	rlite *db;
	int retval;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	rl_skiplist_iterator *iterator;
	rl_skiplist *skiplist;
	rl_skiplist_node *node;
	RL_CALL_VERBOSE(rl_skiplist_create, RL_OK, db, &skiplist);
	long skiplist_page = db->next_empty_page;
	RL_CALL_VERBOSE(rl_write, RL_OK, db, &rl_data_type_skiplist, skiplist_page, skiplist);

	int cmp;
	long i;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < TEST_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_skiplist_add, RL_OK, db, skiplist, skiplist_page, 5.2 * i, data, 1);
	}

	RL_CALL_VERBOSE(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, 0, 0, 0);

	long random_node = 0;
	i = 0;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		if (i == TEST_SIZE / 2 - 1) {
			random_node = iterator->node_page;
		}
		EXPECT_DOUBLE(node->score, 5.2 * i);
		data[0] = i;
		RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, node->value, data, 1, &cmp);
		EXPECT_INT(cmp, 0);
		i++;
	}
	EXPECT_INT(retval, RL_END);
	EXPECT_LONG(i, TEST_SIZE);

	RL_CALL_VERBOSE(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, 0, -1, 0);

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		EXPECT_DOUBLE(node->score, 5.2 * i);
		data[0] = i;
		RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, node->value, data, 1, &cmp);
		EXPECT_INT(cmp, 0);
		i--;
	}
	EXPECT_INT(retval, RL_END);
	EXPECT_LONG(i, -1);

	RL_CALL_VERBOSE(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, 0, -1, 1);

	i = TEST_SIZE - 1;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		EXPECT_DOUBLE(node->score, 5.2 * i);
		data[0] = i;
		RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, node->value, data, 1, &cmp);
		EXPECT_INT(cmp, 0);
		i--;
	}
	EXPECT_INT(retval, RL_END);
	EXPECT_LONG(i, TEST_SIZE - 2);

	RL_CALL_VERBOSE(rl_skiplist_iterator_create, RL_OK, db, &iterator, skiplist, random_node, 1, 5);

	i = TEST_SIZE / 2;
	while ((retval = rl_skiplist_iterator_next(iterator, &node)) == RL_OK) {
		EXPECT_DOUBLE(node->score, 5.2 * i);
		data[0] = i;
		RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, node->value, data, 1, &cmp);
		EXPECT_INT(cmp, 0);
		i++;
	}
	EXPECT_INT(retval, RL_END);
	EXPECT_LONG(i, TEST_SIZE / 2 + 5);

	rl_free(data);
	rl_close(db);
	PASS();
}

SUITE(skiplist_test)
{
	int i, j;
	for (i = -1; i <= 1; i++) {
		for (j = 0; j < 3; j++) {
			RUN_TESTp(basic_skiplist_test, i, j);
		}
	}
	RUN_TEST(basic_skiplist_first_node_test);
	for (i = 0; i < 3; i++) {
		RUN_TEST1(basic_skiplist_delete_node_test, i);
		RUN_TEST1(basic_skiplist_iterator_test, i);
	}
	RUN_TEST(basic_skiplist_node_by_rank);
}
