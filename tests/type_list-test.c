#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../src/rlite.h"
#include "../src/type_list.h"
#include "util.h"

#define UNSIGN(str) ((unsigned char *)(str))

static int create(rlite *db, unsigned char *key, long keylen, int maxsize, int _commit, int left)
{
	int i;
	int retval;
	long valuelen = 2, size;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	value[1] = 0;
	for (i = maxsize - 1; i >= 0; i--) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, left, 1, &value, &valuelen, &size);
		EXPECT_LONG(size, maxsize - i);
		RL_BALANCED();

	}
	EXPECT_LONG(size, maxsize);
	free(value);
	return retval;
}

TEST basic_test_lpush_llen(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	value[1] = 0;
	long size;
	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, maxsize);

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_lpush_lpop(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_pop, RL_OK, db, key, keylen, &testvalue, &testvaluelen, 1);
		EXPECT_BYTES(value, 2, testvalue, testvaluelen);
		rl_free(testvalue);

		RL_BALANCED();
	}

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_rpush_lpop(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 0);

	for (i = maxsize - 1; i >= 0; i--) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_pop, RL_OK, db, key, keylen, &testvalue, &testvaluelen, 1);
		EXPECT_BYTES(value, 2, testvalue, testvaluelen);
		rl_free(testvalue);

		RL_BALANCED();
	}

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_lpush_rpop(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	for (i = maxsize - 1; i >= 0; i--) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_pop, RL_OK, db, key, keylen, &testvalue, &testvaluelen, 0);
		EXPECT_BYTES(value, 2, testvalue, testvaluelen);
		rl_free(testvalue);

		RL_BALANCED();
	}

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_lpush_lindex(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, i, &testvalue, &testvaluelen);
		EXPECT_BYTES(value, 2, testvalue, testvaluelen);
		rl_free(testvalue);

		RL_BALANCED();
	}

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_lpush_linsert(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	// note that LPUSH reverses the order
	unsigned char *values[3] = {UNSIGN("CC"), UNSIGN("BB"), UNSIGN("AA")};
	long valueslen[3] = {2, 2, 2};
	long size;
	unsigned char *testvalue;
	long testvaluelen;

	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 3, values, valueslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_linsert, RL_OK, db, key, keylen, 1, UNSIGN("AA"), 2, UNSIGN("AB"), 2, &size);
	RL_BALANCED();
	EXPECT_LONG(size, 4);

	RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, 1, &testvalue, &testvaluelen);
	EXPECT_STR("AB", testvalue, testvaluelen);
	rl_free(testvalue);

	RL_CALL_VERBOSE(rl_linsert, RL_NOT_FOUND, db, key, keylen, 1, UNSIGN("PP"), 2, UNSIGN("AB"), 2, NULL);
	RL_BALANCED();

	size = 0;
	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 4);

	RL_CALL_VERBOSE(rl_linsert, RL_NOT_FOUND, db, UNSIGN("non existent key"), 10, 1, UNSIGN("PP"), 2, UNSIGN("AB"), 2, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_lpushx(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	value[1] = 0;
	long size;

	RL_CALL_VERBOSE(rl_push, RL_NOT_FOUND, db, key, keylen, 0, 1, 1, &key, &keylen, NULL);
	RL_CALL_VERBOSE(rl_llen, RL_NOT_FOUND, db, key, keylen, &size);

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, 1, _commit, 1);
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 0, 1, 1, &key, &keylen, NULL);

	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 2);

	free(value);
	rl_close(db);
	PASS();
}

TEST test_lrange(rlite *db, unsigned char *key, long keylen, long start, long stop, long cstart, long cstop)
{
	unsigned char testvalue[2];
	long testvaluelen = 2;
	testvalue[1] = 0;

	long i, size = 0, *valueslen = NULL;
	unsigned char **values = NULL;
	long pos;
	int retval;
	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, start, stop, &size, &values, &valueslen);
	EXPECT_LONG(size, cstop - cstart);

	for (i = cstart; i < cstop; i++) {
		testvalue[0] = i;
		pos = i - cstart;
		EXPECT_BYTES(values[pos], valueslen[pos], testvalue, testvaluelen);
	}

	for (i = 0; i < size; i++) {
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);
	PASS();
}

TEST basic_test_lrange(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	value[1] = 0;

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, 10, _commit, 1);

	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, 0, -1, 0, 10);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, -100, 100, 0, 10);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, -1, -1, 9, 10);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, -1, 100, 9, 10);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, -1, 0, 0, 0);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, 0, -1000, 0, 0);
	RL_CALL_VERBOSE(test_lrange, RL_OK, db, key, keylen, -5, -4, 5, 7);

	free(value);
	rl_close(db);
	PASS();
}

TEST basic_test_lrem(int _commit)
{
	int retval;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value1 = UNSIGN("AA"), *value2 = UNSIGN("BB");
	long valuelen = 2, deleted;
	long size;
	unsigned char **values;
	long *valueslen;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	long i, times = 100;

	for (i = 0; i < times; i++) {
		RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &value2, &valuelen, NULL);
		RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &value1, &valuelen, NULL);
		RL_BALANCED();
	}

	RL_CALL_VERBOSE(rl_lrem, RL_OK, db, key, keylen, 1, 2, value2, valuelen, &deleted);
	RL_BALANCED();

	EXPECT_LONG(deleted, 2);

#define TEST_VALUE(index, value)\
	{\
		unsigned char *testvalue;\
		long testvaluelen;\
		RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, index, &testvalue, &testvaluelen);\
		if (testvaluelen != (long)strlen((char *)value) || memcmp(testvalue, value, testvaluelen) != 0) {\
			FAIL();\
		}\
		rl_free(testvalue);\
	}

	TEST_VALUE(0, value1);
	TEST_VALUE(1, value1);
	TEST_VALUE(2, value1);
	TEST_VALUE(3, value2);
	TEST_VALUE(4, value1);

	RL_CALL_VERBOSE(rl_lrem, RL_OK, db, key, keylen, -1, 200, value1, valuelen, &deleted);
	RL_BALANCED();

	EXPECT_LONG(deleted, 100);

	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	for (i = 0; i < size; i++) {
		EXPECT_STR("BB", values[i], valueslen[i]);
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);

	RL_CALL_VERBOSE(rl_lrem, RL_DELETED, db, key, keylen, 1, 0, value2, valuelen, &deleted);
	EXPECT_LONG(deleted, 98);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_lrange, RL_NOT_FOUND, db, key, keylen, 0, -1, &size, &values, &valueslen);

	rl_close(db);
	PASS();
}

TEST basic_test_lset(int maxsize, int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;
	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	RL_CALL_VERBOSE(rl_lset, RL_INVALID_PARAMETERS, db, key, keylen, maxsize, value, valuelen);
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_lset, RL_OK, db, key, keylen, -1, value, valuelen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, -1, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	rl_close(db);
	PASS();
}

TEST basic_test_ltrim(int _commit)
{
	int retval;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	long i, size;
	unsigned char **values;
	long *valueslen;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, 200, _commit, 1);
	RL_CALL_VERBOSE(rl_ltrim, RL_OK, db, key, keylen, 50, -50);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	EXPECT_LONG(size, 101);
	for (i = 0; i < size; i++) {
		EXPECT_LONG(valueslen[i], 2);
		EXPECT_INT(values[i][0], ((50 + i) % CHAR_MAX));
		EXPECT_INT(values[i][1], 0);
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);

	RL_CALL_VERBOSE(rl_ltrim, RL_DELETED, db, key, keylen, 1, 0);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

SUITE(type_list_test)
{
	int i;
	for (i = 0; i < 3; i++) {
		RUN_TESTp(basic_test_lpush_llen, 100, i);
		RUN_TESTp(basic_test_lpush_lpop, 100, i);
		RUN_TESTp(basic_test_rpush_lpop, 100, i);
		RUN_TESTp(basic_test_lpush_rpop, 500, i);
		RUN_TESTp(basic_test_lpush_lindex, 100, i);
		RUN_TESTp(basic_test_lpush_linsert, i);
		RUN_TESTp(basic_test_lpushx, i);
		RUN_TESTp(basic_test_lrange, i);
		RUN_TESTp(basic_test_lrem, i);
		RUN_TESTp(basic_test_lset, 100, i);
		RUN_TESTp(basic_test_ltrim, i);
	}
}
