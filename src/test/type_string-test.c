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

static int basic_test_set_bitop(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_bitop %d\n", _commit);

	rlite *db = NULL;
	unsigned char *k1 = UNSIGN("key 1");
	long k1len = strlen((char *)k1);
	unsigned char *v1 = UNSIGN("foobar");
	long v1len = strlen((char *)v1);
	unsigned char *k2 = UNSIGN("key 2");
	long k2len = strlen((char *)k2);
	unsigned char *v2 = UNSIGN("abcdef");
	long v2len = strlen((char *)v2);
	unsigned char *targetk = UNSIGN("key 2");
	long targetklen = strlen((char *)targetk);
	unsigned char *expected = UNSIGN("`bc`ab");
	long expectedlen = strlen((char *)expected);
    const unsigned char *keys[2] = {k1, k2};
    long keyslen[2] = {k1len, k2len};
	unsigned char *testvalue;
	long testvaluelen;

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, k1, k1len, v1, v1len, 0, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, k2, k2len, v2, v2len, 0, 0);
	RL_CALL_VERBOSE(rl_bitop, RL_OK, db, BITOP_AND, targetk, targetklen, 2, keys, keyslen);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, targetk, targetklen, &testvalue, &testvaluelen);
    EXPECT_BYTES(testvalue, testvaluelen, expected, expectedlen);
    rl_free(testvalue);

	fprintf(stderr, "End basic_test_set_bitop\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_set_bitcount(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_bitcount %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("foobar");
	long valuelen = strlen((char *)value);
	long bitcount;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_bitcount, RL_OK, db, key, keylen, 0, -1, &bitcount);
	EXPECT_LONG(bitcount, 26);

	RL_CALL_VERBOSE(rl_bitcount, RL_OK, db, key, keylen, 0, 0, &bitcount);
	EXPECT_LONG(bitcount, 4);
	RL_CALL_VERBOSE(rl_bitcount, RL_OK, db, key, keylen, 1, 1, &bitcount);
	EXPECT_LONG(bitcount, 6);

	fprintf(stderr, "End basic_test_set_bitcount\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}
/*
redis> SET mykey "\xff\xf0\x00"
OK
redis> BITPOS mykey 0
(integer) 12
redis> SET mykey "\x00\xff\xf0"
OK
redis> BITPOS mykey 1 0
(integer) 8
redis> BITPOS mykey 1 2
(integer) 16
 */

static int basic_test_set_bitpos(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_bitpos %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("\xff\xf0\x00");
	long valuelen = 3;
	unsigned char *value2 = UNSIGN("\x00\xff\xf0");
	long value2len = 3;
	long bitpos;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_BALANCED();

	//  int rl_bitpos(struct rlite *db, const unsigned char *key, long keylen, int bit, long start, long stop, int end_given, long *position)
	RL_CALL_VERBOSE(rl_bitpos, RL_OK, db, key, keylen, 0, 0, -1, 0, &bitpos);
	EXPECT_LONG(bitpos, 12);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value2, value2len, 0, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_bitpos, RL_OK, db, key, keylen, 1, 0, -1, 0, &bitpos);
	EXPECT_LONG(bitpos, 8);
	RL_CALL_VERBOSE(rl_bitpos, RL_OK, db, key, keylen, 1, 2, -1, 1, &bitpos);
	EXPECT_LONG(bitpos, 16);

	fprintf(stderr, "End basic_test_set_bitpos\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_pfadd(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("asd");
	long valuelen = 3;
	unsigned char *value2 = UNSIGN("qwe");
	long value2len = 3;
	unsigned char *value3 = UNSIGN("zxc");
	long value3len = 3;

	unsigned char *values[] = {value, value2};
	long valueslen[] = {valuelen, value2len};
	int updated = -1;
	unsigned long long expires = rl_mstime() + 100000, testexpires;

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 2, values, valueslen, &updated);
	RL_BALANCED();

	EXPECT_LONG(updated, 1);

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 2, values, valueslen, &updated);
	RL_BALANCED();

	EXPECT_LONG(updated, 0);

	RL_CALL(rl_key_expires, RL_OK, db, key, keylen, expires);
	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 1, &value3, &value3len, &updated);
	RL_BALANCED();

	EXPECT_LONG(updated, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, &testexpires);

	EXPECT_LONG(expires, testexpires);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_CALL_VERBOSE(rl_pfadd, RL_INVALID_STATE, db, key, keylen, 1, &value3, &value3len, &updated);

	fprintf(stderr, "End basic_test_pfadd\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfcount(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfcount %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *value = UNSIGN("asd");
	long valuelen = 3;
	unsigned char *value2 = UNSIGN("qwe");
	long value2len = 3;
	unsigned char *value3 = UNSIGN("zxc");
	long value3len = 3;
	long count;

	unsigned char *values[] = {value, value2};
	long valueslen[] = {valuelen, value2len};
	unsigned char *keys[] = {key, key2};
	long keyslen[] = {keylen, key2len};

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 2, values, valueslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key2, key2len, 1, &value3, &value3len, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfcount, RL_OK, db, 1, (const unsigned char **)&key, &keylen, &count);
	EXPECT_LONG(count, 2);

	RL_CALL_VERBOSE(rl_pfcount, RL_OK, db, 2, (const unsigned char **)keys, keyslen, &count);
	EXPECT_LONG(count, 3);

	fprintf(stderr, "End basic_test_pfadd_pfcount\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfmerge(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfmerge %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *value = UNSIGN("asd");
	long valuelen = 3;
	unsigned char *value2 = UNSIGN("qwe");
	long value2len = 3;
	unsigned char *value3 = UNSIGN("zxc");
	long value3len = 3;
	long count;

	unsigned char *values[] = {value, value2};
	long valueslen[] = {valuelen, value2len};

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 2, values, valueslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key2, key2len, 1, &value3, &value3len, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfmerge, RL_OK, db, key2, key2len, 1, (const unsigned char **)&key, &keylen);

	RL_CALL_VERBOSE(rl_pfcount, RL_OK, db, 1, (const unsigned char **)&key2, &key2len, &count);
	EXPECT_LONG(count, 3);

	fprintf(stderr, "End basic_test_pfadd_pfmerge\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfdebug_getreg(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfdebug_getreg %d\n", _commit);

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("1");
	long valuelen = 1;
	int size, i;
	long *elements = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 1, &value, &valuelen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfdebug_getreg, RL_OK, db, key, keylen, &size, &elements);
	EXPECT_LONG(size, 16384);

	for (i = 0; i < size; i++) {
		EXPECT_LONG(elements[i], i == 7527 ? 1 : 0);
	}

	fprintf(stderr, "End basic_test_pfadd_pfdebug_getreg\n");
	retval = 0;
cleanup:
	free(elements);
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfdebug_decode(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfdebug_decode %d\n", _commit);

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("1");
	long valuelen = 1;
	unsigned char *decode = NULL;
	long decodelen = 0;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 1, &value, &valuelen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfdebug_decode, RL_OK, db, key, keylen, &decode, &decodelen);
	const char *expect = "Z:7527 v:1,1 Z:8856";
	long expectlen = strlen(expect);
	EXPECT_BYTES(expect, expectlen, decode, decodelen);

	fprintf(stderr, "End basic_test_pfadd_pfdebug_decode\n");
	retval = 0;
cleanup:
	free(decode);
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfdebug_encoding(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfdebug_encoding %d\n", _commit);

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("1");
	long valuelen = 1;
	unsigned char *encoding = NULL;
	long encodinglen = 0;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 1, &value, &valuelen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfdebug_encoding, RL_OK, db, key, keylen, &encoding, &encodinglen);
	const char *expect = "sparse";
	long expectlen = strlen(expect);
	EXPECT_BYTES(expect, expectlen, encoding, encodinglen);

	fprintf(stderr, "End basic_test_pfadd_pfdebug_encoding\n");
	retval = 0;
cleanup:
	free(encoding);
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_pfdebug_todense(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_pfdebug_todense %d\n", _commit);

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("1");
	long valuelen = 1;
	unsigned char *encoding = NULL;
	long encodinglen = 0;
	int changed;

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 1, &value, &valuelen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_pfdebug_todense, RL_OK, db, key, keylen, &changed);
	EXPECT_LONG(changed, 1);

	RL_CALL_VERBOSE(rl_pfdebug_encoding, RL_OK, db, key, keylen, &encoding, &encodinglen);
	const char *expect = "dense";
	long expectlen = strlen(expect);
	EXPECT_BYTES(expect, expectlen, encoding, encodinglen);

	RL_CALL_VERBOSE(rl_pfdebug_todense, RL_OK, db, key, keylen, &changed);
	EXPECT_LONG(changed, 0);

	fprintf(stderr, "End basic_test_pfadd_pfdebug_todense\n");
	retval = 0;
cleanup:
	free(encoding);
	rl_close(db);
	return retval;
}

static int basic_test_pfadd_empty(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_pfadd_empty %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	int updated;

	RL_CALL_VERBOSE(rl_pfadd, RL_OK, db, key, keylen, 0, NULL, NULL, &updated);
	RL_BALANCED();

	EXPECT_LONG(updated, 1);

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_pfadd_empty\n");
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
		RL_TEST(basic_test_set_bitop, i);
		RL_TEST(basic_test_set_bitcount, i);
		RL_TEST(basic_test_set_bitpos, i);
		RL_TEST(basic_test_pfadd, i);
		RL_TEST(basic_test_pfadd_pfcount, i);
		RL_TEST(basic_test_pfadd_pfmerge, i);
		RL_TEST(basic_test_pfadd_pfdebug_getreg, i);
		RL_TEST(basic_test_pfadd_pfdebug_decode, i);
		RL_TEST(basic_test_pfadd_pfdebug_encoding, i);
		RL_TEST(basic_test_pfadd_pfdebug_todense, i);
		RL_TEST(basic_test_pfadd_empty, i);
	}
}
RL_TEST_MAIN_END
