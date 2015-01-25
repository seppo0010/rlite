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

RL_TEST_MAIN_START(dump_test)
{
	if (test_string()) {
		return 1;
	}
	return 0;
}
RL_TEST_MAIN_END
