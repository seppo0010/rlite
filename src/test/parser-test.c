#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../rlite.h"
#include "test_util.h"

static int test_int8()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\xc0\x01\x06\x00\xb0\x95\x8f\x36$T-o"), 13);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("1", 1, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_int16()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\xc1\x00\x01\x06\x00\x9c\xb3\xbb\x1cX\xe3lx"), 14);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("256", 3, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_int32()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\xc2\x90_\x01\x00\x06\x00\x38\x42\x62\xcf\xcc\xf6\x40s"), 16);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("90000", 5, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_uncompressedstring()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\x0bHello World\x06\x00R\x85||k\x87\xe6\x43"), 23);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("Hello World", 11, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(parser_test)
{
	if (test_int8()) {
		return 1;
	}
	if (test_int16()) {
		return 1;
	}
	if (test_int32()) {
		return 1;
	}
	if (test_uncompressedstring()) {
		return 1;
	}
	return 0;
}
RL_TEST_MAIN_END
