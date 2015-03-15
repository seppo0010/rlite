#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "util.h"
#include "../src/rlite.h"
#include "../src/status.h"
#include "../src/sort.h"

TEST sort_nosort_list_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 0, 3, (unsigned char **)values, valueslen, NULL);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 1, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "1", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "0", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_list_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 0, 3, (unsigned char **)values, valueslen, NULL);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "0", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "1", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_list_sortby_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 0, 3, (unsigned char **)values, valueslen, NULL);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_list_sortby_alpha_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 0, 3, (unsigned char **)values, valueslen, NULL);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 1, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_nosort_set_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 3, (unsigned char **)values, valueslen, NULL);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 1, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "1", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "0", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_set_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 3, (unsigned char **)values, valueslen, NULL);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "0", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "1", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_set_sortby_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 3, (unsigned char **)values, valueslen, NULL);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_set_sortby_alpha_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 3, (unsigned char **)values, valueslen, NULL);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 1, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_nosort_zset_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[0], valueslen[0]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[1], valueslen[1]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[2], valueslen[2]);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 1, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "0", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "1", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_zset_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[0], valueslen[0]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[1], valueslen[1]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[2], valueslen[2]);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "0", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "1", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_zset_sortby_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[0], valueslen[0]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[1], valueslen[1]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[2], valueslen[2]);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_zset_sortby_alpha_test(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"a", "s", "d"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[0], valueslen[0]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[1], valueslen[1]);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, (unsigned char *)values[2], valueslen[2]);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wa", 2, (unsigned char *)"1", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"ws", 2, (unsigned char *)"0", 1, 1, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, (unsigned char *)"wd", 2, (unsigned char *)"2", 1, 1, 0);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, (unsigned char *)"w*", 2, 0, 0, 1, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "s", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "a", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "d", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}

TEST sort_set_nosort_lua(int _commit) {
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = (unsigned char *)"key";
	long keylen = 3;
	char *values[] = {"1", "0", "2"};
	long valueslen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 3, (unsigned char **)values, valueslen, NULL);

	long objc;
	unsigned char **objv;
	long *objvlen;
	RL_CALL_VERBOSE(rl_sort, RL_OK, db, key, keylen, NULL, 0, 1, 1, 0, 0, 0, -1, 0, NULL, NULL, NULL, 0, &objc, &objv, &objvlen);

	EXPECT_LONG(objc, 3);
	EXPECT_BYTES(objv[0], objvlen[0], "0", 1);
	EXPECT_BYTES(objv[1], objvlen[1], "1", 1);
	EXPECT_BYTES(objv[2], objvlen[2], "2", 1);

	rl_free(objv[0]);
	rl_free(objv[1]);
	rl_free(objv[2]);
	rl_free(objv);
	rl_free(objvlen);

	rl_close(db);
	PASS();
}


SUITE(sort_test)
{
	int i;
	for (i = 0; i < 3; i++) {
		RUN_TEST1(sort_nosort_list_test, i);
		RUN_TEST1(sort_list_test, i);
		RUN_TEST1(sort_list_sortby_test, i);
		RUN_TEST1(sort_list_sortby_alpha_test, i);
		RUN_TEST1(sort_nosort_set_test, i);
		RUN_TEST1(sort_set_test, i);
		RUN_TEST1(sort_set_sortby_test, i);
		RUN_TEST1(sort_set_sortby_alpha_test, i);
		RUN_TEST1(sort_nosort_zset_test, i);
		RUN_TEST1(sort_zset_test, i);
		RUN_TEST1(sort_zset_sortby_test, i);
		RUN_TEST1(sort_zset_sortby_alpha_test, i);
		RUN_TEST1(sort_set_nosort_lua, i);
	}
}
