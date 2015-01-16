#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_string.h"
#include "test_util.h"

#define UNSIGN(str) ((unsigned char *)(str))

#define EXPECTED_INT(v1, v2)\
	if (v1 != v2) {\
		fprintf(stderr, "Expected %d == %d instead on line %d\n", v1, v2, __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECTED_BYTES(s1, l1, s2, l2)\
	if (l1 != l2 || memcmp(s1, s2, l1) != 0) {\
		fprintf(stderr, "Expected \"%s\" (%lu) == \"%s\" (%lu) instead on line %d\n", s1, (size_t)l1, s2, (size_t)l2, __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define RL_BALANCED()\
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);\
	if (_commit) {\
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);\
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);\
	}

#define EXPECTED_STR(s1, s2, l2) EXPECTED_BYTES(s1, strlen((char *)s1), s2, l2)

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

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_BALANCED();

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

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value2, value2len, 0, 0);
	RL_BALANCED();

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
	if (testvaluelen != 4 || memcmp(testvalue, &value[2], 4) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", value, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_getrange\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
	if (newlen != expectedvaluelen) {
		fprintf(stderr, "Expected new length to be %ld, got %ld instead on line %d\n", expectedvaluelen, newlen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	if (testvaluelen != expectedvaluelen || memcmp(expectedvalue, testvalue, sizeof(expectedvalue)) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", expectedvalue, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_setrange\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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

	if (testvaluelen != firstchunklen) {
		fprintf(stderr, "Expected length after append to be %ld, got %ld instead on line %d\n", firstchunklen, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_append, RL_OK, db, key, keylen, &value[firstchunklen], valuelen - firstchunklen, &testvaluelen);
	RL_BALANCED();

	if (testvaluelen != valuelen) {
		fprintf(stderr, "Expected length after append to be %ld, got %ld instead on line %d\n", valuelen, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	if (testvaluelen != valuelen || memcmp(testvalue, value, valuelen) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", value, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_append\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
	if (testvaluelen != valuelen || memcmp(testvalue, value, valuelen) != 0) {
		fprintf(stderr, "Expected value to be \"%s\", got \"%s\" (%ld) instead on line %d\n", value, testvalue, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_setnx_setnx_get\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
	if (expiration != testexpiration) {
		fprintf(stderr, "Expected expiration to be %llu, got %llu instead on line %d\n", expiration, testexpiration, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_set_expiration\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
	if (testvaluelen != valuelen) {
		fprintf(stderr, "Expected length to be %ld, got %ld instead on line %d\n", valuelen, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_set_strlen\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
	if (testnewvalue != v1) {
		fprintf(stderr, "Expected new value to be %lld, got %lld instead on line %d\n", v1, testnewvalue, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_incr, RL_OK, db, key, keylen, v2, &testnewvalue);
	if (testnewvalue != expectednewvalue) {
		fprintf(stderr, "Expected new value to be %lld, got %lld instead on line %d\n", expectednewvalue, testnewvalue, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	if (testvaluelen != expectedvaluelen) {
		fprintf(stderr, "Expected length to be %ld, got %ld instead on line %d\n", expectedvaluelen, testvaluelen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_incr\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_set_incrbyfloat(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_incrbyfloat %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *testvalue;
	char *testvalue2;
	long testvaluelen;
	double testnewvalue;
	double v1 = 7.8, v2 = 2.1;
	double expectednewvalue = v1 + v2;

	RL_CALL_VERBOSE(rl_incrbyfloat, RL_OK, db, key, keylen, v1, &testnewvalue);
	RL_BALANCED();
	if (testnewvalue != v1) {
		fprintf(stderr, "Expected new value to be %lf, got %lf instead on line %d\n", v1, testnewvalue, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_incrbyfloat, RL_OK, db, key, keylen, v2, &testnewvalue);
	if (testnewvalue != expectednewvalue) {
		fprintf(stderr, "Expected new value to be %lf, got %lf instead on line %d\n", expectednewvalue, testnewvalue, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_BALANCED();

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	testvalue2 = malloc(sizeof(char) * (testvaluelen + 1));
	memcpy(testvalue2, testvalue, sizeof(char) * testvaluelen);
	testvalue2[testvaluelen] = 0;
	testnewvalue = strtold(testvalue2, NULL);
	if (testnewvalue != expectednewvalue) {
		fprintf(stderr, "Expected value to be %lf, got %lf instead on line %d\n", expectednewvalue, testnewvalue, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	free(testvalue2);
	rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_incrbyfloat\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
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
		if (bitvalue != bits[i]) {
			fprintf(stderr, "Expected bit at position %ld to be %d, got %d instead on line %d\n", i, bits[i], bitvalue, __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	}

	fprintf(stderr, "End basic_test_set_getbit\n");
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
