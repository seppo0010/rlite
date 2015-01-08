#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_string.h"
#include "test_util.h"

#define UNSIGN(str) ((unsigned char *)(str))

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

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	if (testvaluelen != valuelen || memcmp(testvalue, value, valuelen) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", value, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_get\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL);

	fprintf(stderr, "End basic_test_set_delete_get\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value2, value2len);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	if (testvaluelen != value2len || memcmp(testvalue, value2, value2len) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", value2, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_set_get\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

RL_TEST_MAIN_START(type_string_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_set_get, i);
		RL_TEST(basic_test_set_delete_get, i);
		RL_TEST(basic_test_set_set_get, i);
	}
}
RL_TEST_MAIN_END
