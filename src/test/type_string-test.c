#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_string.h"
#include "test_util.h"

static int basic_test_set_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_get %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_get\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_delete_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_delete_get %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL);

	fprintf(stderr, "End basic_test_set_delete_get\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_set_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_set_get %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);
	unsigned char *value2 = UNSIGN("my value2");
	long value2len = strlen((char *)value2);
	unsigned char *testvalue;
	long testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value2, value2len, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value2, value2len, testvalue, testvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_set_get\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_getrange(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_getrange %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_getrange, RL_OK, db, key, keylen, 2, 5, &testvalue, &testvaluelen);
	EXPECT_BYTES(&value[2], 4, testvalue, testvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_getrange\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_setrange(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_setrange %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;
	unsigned char *updatevalue = UNSIGN(" new value");
	long updatevaluelen = strlen((char *)updatevalue), newlen;
	unsigned char *expectedvalue = UNSIGN("my new value");
	long expectedvaluelen = strlen((char *)expectedvalue);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_setrange, RL_OK, db, key, keylen, 2, updatevalue, updatevaluelen, &newlen);
	RL_BALANCED();
	EXPECT_LONG(newlen, expectedvaluelen);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(testvalue, testvaluelen, expectedvalue, expectedvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_setrange\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_append(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_append %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;
	long firstchunklen = 2;

	RL_CALL_VERBOSE(rl_append, RL_OK, db, key, keylen, value, firstchunklen, &testvaluelen);
	RL_BALANCED();
	EXPECT_LONG(testvaluelen, firstchunklen);

	RL_CALL_VERBOSE(rl_append, RL_OK, db, key, keylen, &value[firstchunklen], valuelen - firstchunklen, &testvaluelen);
	RL_BALANCED();
	EXPECT_LONG(testvaluelen, valuelen);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_append\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_setnx_setnx_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_setnx_setnx_get %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);
	unsigned char *value2 = UNSIGN("my value2");
	long value2len = strlen((char *)value2);
	unsigned char *testvalue;
	long testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 1, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_set, RL_FOUND, db, key, keylen, value2, value2len, 1, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_setnx_setnx_get\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_expiration(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_expiration %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);
	unsigned long long expiration = rl_mstime() + 12983, testexpiration;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, expiration);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, &testexpiration);
	EXPECT_LLU(expiration, testexpiration);

	fprintf(stderr, "End basic_test_set_expiration\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_strlen(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_strlen %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value), testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, NULL, &testvaluelen);
	EXPECT_LONG(valuelen, testvaluelen);

	fprintf(stderr, "End basic_test_set_strlen\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_incr(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_incr %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	long long testnewvalue;
	long long v1 = 123, v2 = 456;
	long long expectednewvalue = v1 + v2;
	long expectedvaluelen = 3;

	RL_CALL_VERBOSE(rl_incr, RL_OK, db, key, keylen, v1, &testnewvalue);
	RL_BALANCED();
	EXPECT_LL(testnewvalue, v1);

	RL_CALL_VERBOSE(rl_incr, RL_OK, db, key, keylen, v2, &testnewvalue);
	RL_BALANCED();
	EXPECT_LL(testnewvalue, expectednewvalue);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_LONG(testvaluelen, expectedvaluelen);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_incr\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_incrbyfloat(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_incrbyfloat %d\n", _commit);

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	char *testvalue2 = NULL;
	long testvaluelen;
	double testnewvalue;
	double v1 = 7.8, v2 = 2.1;
	double expectednewvalue = v1 + v2;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_incrbyfloat, RL_OK, db, key, keylen, v1, &testnewvalue);
	RL_BALANCED();
	EXPECT_DOUBLE(testnewvalue, v1);

	RL_CALL_VERBOSE(rl_incrbyfloat, RL_OK, db, key, keylen, v2, &testnewvalue);
	RL_BALANCED();
	EXPECT_DOUBLE(testnewvalue, expectednewvalue);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	testvalue2 = malloc(sizeof(char) * (testvaluelen + 1));
	memcpy(testvalue2, testvalue, sizeof(char) * testvaluelen);
	testvalue2[testvaluelen] = 0;
	testnewvalue = strtold(testvalue2, NULL);
	EXPECT_DOUBLE(testnewvalue, expectednewvalue);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_incrbyfloat\n");
	retval = 0;
cleanup:
	free(testvalue2);
	rl_close(db);
	return retval;
}

static int basic_test_set_getbit(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_getbit %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("ab");
	long valuelen = strlen((char *)value);
	int bitvalue;
	long i;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

#define BIT_COUNT 24
	int bits[BIT_COUNT] = {0, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	for (i = 0; i < BIT_COUNT; i++) {
		RL_CALL_VERBOSE(rl_getbit, RL_OK, db, key, keylen, i, &bitvalue);
		EXPECT_INT(bitvalue, bits[i]);
	}

	fprintf(stderr, "End basic_test_set_getbit\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(type_string_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_set_get, i);
		RL_TEST(basic_test_set_delete_get, i);
		RL_TEST(basic_test_set_set_get, i);
		RL_TEST(basic_test_set_getrange, i);
		RL_TEST(basic_test_set_setrange, i);
		RL_TEST(basic_test_append, i);
		RL_TEST(basic_test_setnx_setnx_get, i);
		RL_TEST(basic_test_set_expiration, i);
		RL_TEST(basic_test_set_strlen, i);
		RL_TEST(basic_test_set_incr, i);
		RL_TEST(basic_test_set_incrbyfloat, i);
		RL_TEST(basic_test_set_getbit, i);
	}
}
RL_TEST_MAIN_END
