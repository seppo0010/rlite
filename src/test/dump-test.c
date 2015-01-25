#include "../../deps/crc64.h"
#include "../../deps/endianconv.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../rlite.h"
#include "test_util.h"

static int test_string()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, UNSIGN("asd"), 3, 0, 0);
	RL_CALL_VERBOSE(rl_dump, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(UNSIGN("\x00\x80\x00\x00\x00\x03\x61sd\x06\x00\xa4\xed\x80\xcb:7\x89\xd7"), 19, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_list()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	unsigned char *values[2] = {UNSIGN("b"), UNSIGN("a")};
	long valueslen[2] = {1, 1};
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 0, 2, values, valueslen, NULL);
	RL_CALL_VERBOSE(rl_dump, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(UNSIGN("\x01\x80\x00\x00\x00\x02\x80\x00\x00\x00\x01\x62\x80\x00\x00\x00\x01\x61\x06\x00\x94\x46\xb5\x94\x1e\x1e_K"), 28, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_set()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	unsigned char *values[2] = {UNSIGN("b"), UNSIGN("a")};
	long valueslen[2] = {1, 1};
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, values, valueslen, NULL);
	RL_CALL_VERBOSE(rl_dump, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(UNSIGN("\x01\x80\x00\x00\x00\x00\x80\x00\x00\x00\x01\x61\x80\x00\x00\x00\x01\x62\x06\x00\xa5\xd1\x37G\xe7\x87\xda\xf0"), 28, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(dump_test)
{
	if (test_string()) {
		return 1;
	}
	if (test_list()) {
		return 1;
	}
	if (test_set()) {
		return 1;
	}
	return 0;
}
RL_TEST_MAIN_END
