#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_list.h"
#include "test_util.h"

#define UNSIGN(str) ((unsigned char *)(str))

static int create(rlite *db, unsigned char *key, long keylen, int maxsize, int _commit)
{
	int i;
	int retval;
	long valuelen = 2, size;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	value[1] = 0;
	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_lpush, RL_OK, db, key, keylen, 1, &value, &valuelen, &size);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		if (size != i + 1) {
			fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, i + 1, __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}

		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		}
	}
	if (size != i) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, i, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	retval = 0;
cleanup:
	free(value);
	return retval;
}

static int basic_test_lpush_llen(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpush_llen %d %d\n", maxsize, _commit);

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	value[1] = 0;
	long size;
	RL_CALL_VERBOSE(create, 0, db, key, keylen, maxsize, _commit);

	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);
	if (size != maxsize) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, maxsize, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_lpush_llen\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lpush_lpop(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpush_lpop %d %d\n", maxsize, _commit);

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, 0, db, key, keylen, maxsize, _commit);

	for (i = maxsize - 1; i >= 0; i--) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_lpop, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
		if (testvaluelen != 2 || memcmp(testvalue, value, 2) != 0) {
			fprintf(stderr, "Expected value to be %d,%d; got %d,%d instead on line %d\n", value[0], value[1], testvalue[0], testvalue[1], __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		rl_free(testvalue);

		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		}
	}

	fprintf(stderr, "End basic_test_lpush_lpop\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lpush_lindex(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpush_lindex %d %d\n", maxsize, _commit);

	rlite *db = NULL;
	unsigned char *value = malloc(sizeof(unsigned char) * 2);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	long testvaluelen;
	value[1] = 0;
	int i;

	RL_CALL_VERBOSE(create, 0, db, key, keylen, maxsize, _commit);

	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, (long)(maxsize - i - 1), &testvalue, &testvaluelen);
		if (testvaluelen != 2 || memcmp(testvalue, value, 2) != 0) {
			fprintf(stderr, "Expected value to be %d,%d; got %d,%d instead on line %d\n", value[0], value[1], testvalue[0], testvalue[1], __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		rl_free(testvalue);

		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		}
	}

	fprintf(stderr, "End basic_test_lpush_lindex\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lpush_linsert(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpush_linsert %d\n", _commit);

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

	RL_CALL_VERBOSE(rl_lpush, RL_OK, db, key, keylen, 3, values, valueslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_linsert, RL_OK, db, key, keylen, 1, UNSIGN("AA"), 2, UNSIGN("AB"), 2, &size);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	if (size != 4) {
		fprintf(stderr, "Expected size to be 4, got %ld instead on line %d\n", size, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, 1, &testvalue, &testvaluelen);

	if (testvaluelen != 2 || memcmp(testvalue, "AB", 2) != 0) {
		fprintf(stderr, "Expected value to be \"AB\"; got \"%s\" (%ld) instead on line %d\n", testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	rl_free(testvalue);

	RL_CALL_VERBOSE(rl_linsert, RL_NOT_FOUND, db, key, keylen, 1, UNSIGN("PP"), 2, UNSIGN("AB"), 2, NULL);

	size = 0;
	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);

	if (size != 4) {
		fprintf(stderr, "Expected size to be 4, got %ld instead on line %d\n", size, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_linsert, RL_NOT_FOUND, db, UNSIGN("non existent key"), 10, 1, UNSIGN("PP"), 2, UNSIGN("AB"), 2, NULL);

	fprintf(stderr, "End basic_test_lpush_linsert\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

RL_TEST_MAIN_START(type_list_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_lpush_llen, 100, i);
		RL_TEST(basic_test_lpush_lpop, 100, i);
		RL_TEST(basic_test_lpush_lindex, 100, i);
		RL_TEST(basic_test_lpush_linsert, i);
	}
}
RL_TEST_MAIN_END
