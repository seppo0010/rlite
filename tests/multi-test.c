#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "util.h"
#include "../src/rlite/page_key.h"
#include "../src/rlite/rlite.h"
#include "../src/rlite/type_zset.h"

TEST basic_multi_test(int _commit)
{
	int retval = 0;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *value = (unsigned char *)"my value";
	long valuelen = strlen((char *)value);
	unsigned char *key2 = (unsigned char *)"my key2";
	long key2len = strlen((char *)key2);
	unsigned char *value2 = (unsigned char *)"my value2";
	long value2len = strlen((char *)value2);
	unsigned char *testvalue;
	long testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key2, key2len, value2, value2len, 0, 0);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key2, key2len, &testvalue, &testvaluelen);
	EXPECT_BYTES(value2, value2len, testvalue, testvaluelen);
	rl_free(testvalue);

	rl_discard(db);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	RL_CALL_VERBOSE(rl_get, RL_NOT_FOUND, db, key2, key2len, &testvalue, &testvaluelen);

	rl_close(db);
	PASS();
}

SUITE(multi_test)
{
	RUN_TEST1(basic_multi_test, 1);
	RUN_TEST1(basic_multi_test, 2);
}
