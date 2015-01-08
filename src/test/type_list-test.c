#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_list.h"
#include "test_util.h"

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
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		if (size != maxsize - i) {
			fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, maxsize - i, __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}

		if (_commit) {
			RL_CALL_VERBOSE(rl_commit, RL_OK, db);
			RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
		}
	}
	if (size != maxsize) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, maxsize, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	retval = RL_OK;
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
	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

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

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_pop, RL_OK, db, key, keylen, &testvalue, &testvaluelen, 1);
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

static int basic_test_rpush_lpop(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_rpush_lpop %d %d\n", maxsize, _commit);

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

	fprintf(stderr, "End basic_test_rpush_lpop\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lpush_rpop(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpush_rpop %d %d\n", maxsize, _commit);

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

	fprintf(stderr, "End basic_test_lpush_rpop\n");
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

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	for (i = 0; i < maxsize; i++) {
		value[0] = i % CHAR_MAX;
		RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, i, &testvalue, &testvaluelen);
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

	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 3, values, valueslen, NULL);
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

static int basic_test_lpushx(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lpushx %d\n", _commit);

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
	if (size != 2) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_lpushx\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int test_lrange(rlite *db, unsigned char *key, long keylen, long start, long stop, long cstart, long cstop)
{
	unsigned char testvalue[2];
	long testvaluelen = 2;
	testvalue[1] = 0;

	long i, size = 0, *valueslen = NULL;
	unsigned char **values = NULL;
	long pos;
	int retval;
	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, start, stop, &size, &values, &valueslen);
	if (size != cstop - cstart) {
		fprintf(stderr, "Expected size to be %ld, got %ld instead on line %d for range {%ld,%ld}\n", cstop - cstart, size, __LINE__, start, stop);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	for (i = cstart; i < cstop; i++) {
		testvalue[0] = i;
		pos = i - cstart;
		if (valueslen[pos] != testvaluelen || memcmp(values[pos], testvalue, testvaluelen) != 0) {
			fprintf(stderr, "Expected value to be %d,%d got %d,%d instead on line %d for range {%ld,%ld}\n", testvalue[0], testvalue[1], values[pos][0], values[pos][1], __LINE__, start, stop);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	}

	retval = RL_OK;
cleanup:
	for (i = 0; i < size; i++) {
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);
	return retval;
}

static int basic_test_lrange(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lrange %d\n", _commit);

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

	fprintf(stderr, "End basic_test_lrange\n");
	retval = 0;
cleanup:
	free(value);
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lrem(int _commit)
{
	int retval;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value1 = UNSIGN("AA"), *value2 = UNSIGN("BB");
	long valuelen = 2, deleted;
	long size;
	unsigned char **values;
	long *valueslen;
	fprintf(stderr, "Start basic_test_lrem %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	long i, times = 100;

	for (i = 0; i < times; i++) {
		RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &value2, &valuelen, NULL);
		RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &value1, &valuelen, NULL);
	}

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_lrem, RL_OK, db, key, keylen, 1, 2, value2, valuelen, &deleted);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	if (deleted != 2) {
		fprintf(stderr, "Expected deleted to be 2, got %ld instead on line %d\n", deleted, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

#define TEST_VALUE(index, value)\
	{\
		unsigned char *testvalue;\
		long testvaluelen;\
		RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, index, &testvalue, &testvaluelen);\
		if (testvaluelen != (long)strlen((char *)value) || memcmp(testvalue, value, testvaluelen) != 0) {\
			fprintf(stderr, "Expected value to be \"%s\" (%ld); got \"%s\" (%ld) instead on line %d\n", value, (long)strlen((char *)value), testvalue, testvaluelen, __LINE__);\
			retval = RL_UNEXPECTED;\
			goto cleanup;\
		}\
		rl_free(testvalue);\
	}

	TEST_VALUE(0, value1);
	TEST_VALUE(1, value1);
	TEST_VALUE(2, value1);
	TEST_VALUE(3, value2);
	TEST_VALUE(4, value1);

	RL_CALL_VERBOSE(rl_lrem, RL_OK, db, key, keylen, -1, 200, value1, valuelen, &deleted);

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	if (deleted != 100) {
		fprintf(stderr, "Expected deleted to be 100, got %ld instead on line %d\n", deleted, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	for (i = 0; i < size; i++) {
		if (valueslen[i] != 2 || values[i][0] != 'B' || values[i][1] != 'B') {
			fprintf(stderr, "Expected all elements to be \"BB\", got \"%s\" (%ld) in position %ld on line %d", values[i], valueslen[i], i, __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);

	RL_CALL_VERBOSE(rl_lrem, RL_DELETED, db, key, keylen, 1, 0, value2, valuelen, &deleted);
	if (deleted != 98) {
		fprintf(stderr, "Expected deleted to be 98, got %ld instead on line %d\n", deleted, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_lrange, RL_NOT_FOUND, db, key, keylen, 0, -1, &size, &values, &valueslen);

	fprintf(stderr, "End basic_test_lrem\n");
	retval = RL_OK;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_lset(int maxsize, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_lset %d %d\n", maxsize, _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;
	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, maxsize, _commit, 1);

	RL_CALL_VERBOSE(rl_lset, RL_INVALID_PARAMETERS, db, key, keylen, maxsize, value, valuelen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_lset, RL_OK, db, key, keylen, -1, value, valuelen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_lindex, RL_OK, db, key, keylen, -1, &testvalue, &testvaluelen);

	if (testvaluelen != valuelen || memcmp(value, testvalue, valuelen) != 0) {
		fprintf(stderr, "Expected testvalue to be \"%s\", instead got \"%s\" (%ld) on line %d\n", value, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_lset\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_ltrim(int _commit)
{
	int retval;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	long i, size;
	unsigned char **values;
	long *valueslen;
	fprintf(stderr, "Start basic_test_ltrim %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(create, RL_OK, db, key, keylen, 200, _commit, 1);
	RL_CALL_VERBOSE(rl_ltrim, RL_OK, db, key, keylen, 50, -50);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	if (size != 101) {
		fprintf(stderr, "Expected size to be %d, got %ld instead on line %d\n", 100, size, __LINE__);
	}
	for (i = 0; i < size; i++) {
		if (valueslen[i] != 2 || values[i][0] != (50 + i) % CHAR_MAX || values[i][1] != 0) {
			fprintf(stderr, "Expected value[%ld] to be %ld,%d, got %d,%d instead on line %d\n", i, 50 + i, 0, values[i][0], values[i][1], __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		rl_free(values[i]);
	}
	rl_free(values);
	rl_free(valueslen);

	RL_CALL_VERBOSE(rl_ltrim, RL_DELETED, db, key, keylen, 1, 0);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	fprintf(stderr, "End basic_test_ltrim\n");
	retval = RL_OK;
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
		RL_TEST(basic_test_rpush_lpop, 100, i);
		RL_TEST(basic_test_lpush_rpop, 100, i);
		RL_TEST(basic_test_lpush_lindex, 100, i);
		RL_TEST(basic_test_lpush_linsert, i);
		RL_TEST(basic_test_lpushx, i);
		RL_TEST(basic_test_lrange, i);
		RL_TEST(basic_test_lrem, i);
		RL_TEST(basic_test_lset, 100, i);
		RL_TEST(basic_test_ltrim, i);
	}
}
RL_TEST_MAIN_END
