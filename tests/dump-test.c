#include "../src/crc64.h"
#include "../src/endianconv.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../src/rlite.h"
#include "test_util.h"

#define PRINT(str, strlen)\
	{\
		long i;\
		for (i = 0; i < strlen; i++) {\
			fprintf(stderr, "\\x%.2x", str[i]);\
		}\
		fprintf(stderr, "\n");\
	}

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
	EXPECT_BYTES(UNSIGN("\x02\x80\x00\x00\x00\x02\x80\x00\x00\x00\x01\x61\x80\x00\x00\x00\x01\x62\x06\x00\xbb\x8c\x8c\xcf\x86{ \xfd"), 28, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_zset()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 1.23, UNSIGN("a"), 1);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 4.5, UNSIGN("b"), 1);
	RL_CALL_VERBOSE(rl_dump, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(UNSIGN("\x03\x80\x00\x00\x00\x02\x80\x00\x00\x00\x01\x61\x08\x31\x2e\x32\x33\x30\x30\x30\x30\x80\x00\x00\x00\x01\x62\x08\x34\x2e\x35\x30\x30\x30\x30\x30\x06\x00\x62\xf2\xc1\x8b\x73\x18\x51\xe6"), 46, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_hash()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, UNSIGN("field"), 5, UNSIGN("value"), 5, NULL, 0);
	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, UNSIGN("field2"), 6, UNSIGN("value2"), 6, NULL, 0);
	RL_CALL_VERBOSE(rl_dump, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(UNSIGN("\x04\x80\x00\x00\x00\x02\x80\x00\x00\x00\x05\x66\x69\x65\x6c\x64\x80\x00\x00\x00\x05\x76\x61\x6c\x75\x65\x80\x00\x00\x00\x06\x66\x69\x65\x6c\x64\x32\x80\x00\x00\x00\x06\x76\x61\x6c\x75\x65\x32\x06\x00\x74\xaf\xd2\x25\x1d\x50\x0c\xee"), 58, testvalue, testvaluelen);
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
	if (test_zset()) {
		return 1;
	}
	if (test_hash()) {
		return 1;
	}
	return 0;
}
RL_TEST_MAIN_END
