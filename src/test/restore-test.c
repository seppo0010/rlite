#include "../crc64.h"
#include "../endianconv.h"

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

static int test_uncompressedstring2()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\x80\x00\x00\x00\x03\x61sd\x06\x00\xa4\xed\x80\xcb:7\x89\xd7"), 19);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("asd", 3, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_compressedstring()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5, testvaluelen;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x00\xc3\x06\x15\x01\x61\x61\xe0\n\x00\x06\x00k\xe1\xdb\xe8\xf2\x14\x03\xfb"), 20);
	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES("aaaaaaaaaaaaaaaaaaaaa", 21, testvalue, testvaluelen);
	rl_free(testvalue);
cleanup:
	rl_close(db);
	return retval;
}

static int test_list()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size;
	unsigned char **values;
	long *valueslen;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x01\x01\x01\x61\x06\x00\x63]:\x13x\x95\xd8\xf1"), 14);
	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	EXPECT_LONG(size, 1);
	EXPECT_BYTES("a", 1, values[0], valueslen[0])
	rl_free(values[0]);
	rl_free(values);
	rl_free(valueslen);
cleanup:
	rl_close(db);
	return retval;
}

static int test_list2()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size;
	unsigned char **values;
	long *valueslen;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x01\x03\xc0\x01\xc0\x02\xc0\x03\x06\x00\x84l\x06\x37\xa4\x61\xd9\x14"), 18);
	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	EXPECT_LONG(size, 3);
	EXPECT_BYTES("1", 1, values[0], valueslen[0])
	rl_free(values[0]);
	EXPECT_BYTES("2", 1, values[1], valueslen[1])
	rl_free(values[1]);
	EXPECT_BYTES("3", 1, values[2], valueslen[2])
	rl_free(values[2]);
	rl_free(values);
	rl_free(valueslen);
cleanup:
	rl_close(db);
	return retval;
}

static int test_set()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x02\x03\x01\x63\x01\x61\x01\x62\x06\x00\x43\xf6\xaapc\xdbUP"), 18);
	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 3);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, UNSIGN("a"), 1);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, UNSIGN("b"), 1);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, UNSIGN("c"), 1);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_zset()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size = 10;
	double score;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x03\x01\x02hi\x12\x31.2345600000000001\x06\x00\xe6ho\xad\x11.\x93\xbd"), 34);
	RL_CALL_VERBOSE(rl_zcard, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 1);
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, UNSIGN("hi"), 2, &score);
	EXPECT_DOUBLE(score, 1.23456);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_hash()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey"), *testvalue;
	long keylen = 5;
	long size = 10, testvaluelen = 0;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x04\x01\x05\x66ield\x05value\x06\x00.,\x91!\xa3\xac\x97:"), 24);
	RL_CALL_VERBOSE(rl_hlen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 1);
	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, UNSIGN("field"), 5, &testvalue, &testvaluelen);
	EXPECT_BYTES("value", 5, testvalue, testvaluelen);
	rl_free(testvalue);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_ziplist()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size = 10;
	unsigned char **values = NULL;
	long *valueslen = NULL;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\n\x0f\x0f\x00\x00\x00\x0c\x00\x00\x00\x02\x00\x00\xf3\x02\xf2\xff\x06\x00\x8d#\x11n'Fl\x80"), 27);
	RL_CALL_VERBOSE(rl_llen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 2);
	RL_CALL_VERBOSE(rl_lrange, RL_OK, db, key, keylen, 0, -1, &size, &values, &valueslen);
	EXPECT_BYTES("2", 1, values[0], valueslen[0])
	rl_free(values[0]);
	EXPECT_BYTES("1", 1, values[1], valueslen[1])
	rl_free(values[1]);
	rl_free(values);
	rl_free(valueslen);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_intset()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size = 10;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x0b\x0c\x02\x00\x00\x00\x02\x00\x00\x00\x01\x00\x02\x00\x06\x00\xf9\x94P1\xc1\xbe\x1c\xbd"), 24);
	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 2);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, UNSIGN("1"), 1);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, UNSIGN("2"), 1);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_zsetziplist()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size = 10;
	double score = 0.0;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\x0c\x1c\x1c\x00\x00\x00\x16\x00\x00\x00\x04\x00\x00\x01\x62\x03\x04\x31.23\x06\x01\x63\x03\x03\x34.5\xff\x06\x00\x05\xb3!\xac$:\x15n"), 40);
	RL_CALL_VERBOSE(rl_zcard, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 2);
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, UNSIGN("b"), 1, &score);
	EXPECT_DOUBLE(score, 1.23);
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, UNSIGN("c"), 1, &score);
	EXPECT_DOUBLE(score, 4.5);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

static int test_hashziplist()
{
	int retval;
	rlite *db = NULL;
	unsigned char *key = UNSIGN("mykey");
	long keylen = 5;
	long size = 10;
	unsigned char *data;
	long datalen;

	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	RL_CALL_VERBOSE(rl_restore, RL_OK, db, key, keylen, 0, UNSIGN("\r))\x00\x00\x00 \x00\x00\x00\x04\x00\x00\x05\x66ield\a\x05value\a\x06\x66ield2\b\x06value2\xff\x06\x00\x1b\xd0\x96\xcb\xa8\x90\xfb\x39"), 53);
	RL_CALL_VERBOSE(rl_hlen, RL_OK, db, key, keylen, &size);
	EXPECT_LONG(size, 2);
	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, UNSIGN("field"), 5, &data, &datalen);
	EXPECT_BYTES("value", 5, data, datalen);
	rl_free(data);
	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, UNSIGN("field2"), 6, &data, &datalen);
	EXPECT_BYTES("value2", 6, data, datalen);
	rl_free(data);
	retval = RL_OK;
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(restore_test)
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
	if (test_uncompressedstring2()) {
		return 1;
	}
	if (test_compressedstring()) {
		return 1;
	}
	if (test_list()) {
		return 1;
	}
	if (test_list2()) {
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
	if (test_ziplist()) {
		return 1;
	}
	if (test_intset()) {
		return 1;
	}
	if (test_zsetziplist()) {
		return 1;
	}
	if (test_hashziplist()) {
		return 1;
	}
	return 0;
}
RL_TEST_MAIN_END
