#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "../src/rlite/rlite.h"
#include "../src/rlite/page_multi_string.h"
#include "../src/rlite/util.h"
#include "util.h"

TEST basic_set_get()
{
	int retval, i, j;
	long size, size2, size3, number;
	unsigned char *data, *data2;
	unsigned char localdata[2000];

	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	for (i = 0; i < 2; i++) {
		srand(1);
		size = i == 0 ? 20 : 1020;
		data = malloc(sizeof(unsigned char) * size);
		for (j = 0; j < size; j++) {
			data[j] = (unsigned char)(rand() / CHAR_MAX);
		}
		RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &number, data, size);
		RL_CALL_VERBOSE(rl_multi_string_get, RL_OK, db, number, &data2, &size2);
		RL_CALL_VERBOSE(rl_multi_string_cpy, RL_OK, db, number, localdata, &size3);
		EXPECT_BYTES(data, size, data2, size2);
		EXPECT_BYTES(data, size, localdata, size3);
		rl_free(data);
		rl_free(data2);
	}

	rl_close(db);
	PASS();
}

TEST empty_set_get()
{
	int retval;
	long size, number;
	unsigned char *data;

	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &number, NULL, 0);
	RL_CALL_VERBOSE(rl_multi_string_get, RL_OK, db, number, &data, &size);
	EXPECT_INT(size, 0);
	EXPECT_PTR(data, NULL);

	rl_close(db);
	PASS();
}

TEST test_sha(long size)
{
	int retval;
	unsigned char *data = malloc(sizeof(unsigned char) * size);
	rlite *db = NULL;
	unsigned char digest1[20], digest2[20];
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	long page, i;
	for (i = 0; i < size; i++) {
		data[i] = i % 123;
	}

	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &page, data, size);
	RL_CALL_VERBOSE(rl_multi_string_sha1, RL_OK, db, digest1, page);
	RL_CALL_VERBOSE(sha1, RL_OK, data, size, digest2);
	EXPECT_BYTES(digest1, 20, digest2, 20);

	rl_free(data);
	rl_close(db);
	PASS();
}

static int assert_cmp(rlite *db, long p1, unsigned char *data, long size, int expected_cmp)
{
	long p2;
	int retval, cmp;
	RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, p1, data, size, &cmp);
	EXPECT_INT(cmp, expected_cmp);
	if (data) {
		RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &p2, data, size);
		RL_CALL_VERBOSE(rl_multi_string_cmp, RL_OK, db, p1, p2, &cmp);
		EXPECT_INT(cmp, expected_cmp);
	}
	PASS();
}
TEST test_cmp_different_length()
{
	int retval;
	unsigned char data[3], data2[3];
	long size = 2, i;
	long p1;
	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	for (i = 0; i < 3; i++) {
		data[i] = data2[i] = 100 + i;
	}
	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &p1, data, size);
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, data2, 0, 1);
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, data2, size, 0);
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, data2, size - 1, 1);
	data2[0]++;
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, data2, size - 1, -1);
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, data2, size, -1);
	RL_CALL_VERBOSE(assert_cmp, 0, db, p1, NULL, 0, 1);

	rl_close(db);
	PASS();
}

#define CMP_SIZE 2000
static int test_cmp_internal(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int retval, cmp;
	long p1, p2;
	rlite *db = NULL;
	if (position % 500 == 0) {
	}
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	for (i = 0; i < CMP_SIZE; i++) {
		data[i] = data2[i] = i % CHAR_MAX;
	}
	if (expected_cmp != 0) {
		if (data[position] == CHAR_MAX && expected_cmp > 0) {
			data2[position]--;
		} else if (data[position] == 0 && expected_cmp < 0) {
			data2[position]++;
		} else if (expected_cmp > 0) {
			data[position]++;
		} else {
			data[position]--;
		}
	}
	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &p1, data, size);
	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &p2, data2, size);
	RL_CALL_VERBOSE(rl_multi_string_cmp, RL_OK, db, p1, p2, &cmp);
	EXPECT_INT(cmp, expected_cmp);
	rl_close(db);
	PASS();
}

TEST test_cmp(int expected_cmp, long position_from, long position_to)
{
	int retval;
	long i;
	for (i = position_from; i < position_to; i++) {
		RL_CALL_VERBOSE(test_cmp_internal, 0, expected_cmp, i);
	}
	PASS();
}

static int test_cmp2_internal(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int retval, cmp;
	long p1;
	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	for (i = 0; i < CMP_SIZE; i++) {
		data[i] = data2[i] = i % CHAR_MAX;
	}
	if (expected_cmp != 0) {
		if (data[position] == CHAR_MAX && expected_cmp > 0) {
			data2[position]--;
		} else if (data[position] == 0 && expected_cmp < 0) {
			data2[position]++;
		} else if (expected_cmp > 0) {
			data[position]++;
		} else {
			data[position]--;
		}
	}
	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &p1, data, size);
	RL_CALL_VERBOSE(rl_multi_string_cmp_str, RL_OK, db, p1, data2, size, &cmp);
	EXPECT_INT(cmp, expected_cmp);
	rl_close(db);
	PASS();
}

TEST test_cmp2(int expected_cmp, long position_from, long position_to)
{
	int retval;
	long i;
	for (i = position_from; i < position_to; i++) {
		RL_CALL_VERBOSE(test_cmp2_internal, 0, expected_cmp, i);
	}
	PASS();
}

TEST test_append(long size, long append_size)
{
	int retval;
	unsigned char *data = malloc(sizeof(unsigned char) * size);
	unsigned char *append_data = malloc(sizeof(unsigned char) * append_size);
	unsigned char *testdata;
	long testdatalen;
	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	long page, i;
	for (i = 0; i < size; i++) {
		data[i] = i % 123;
	}
	for (i = 0; i < append_size; i++) {
		append_data[i] = i % 123;
	}

	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &page, data, size);
	RL_CALL_VERBOSE(rl_multi_string_append, RL_OK, db, page, append_data, append_size, &testdatalen);
	EXPECT_LONG(testdatalen, size + append_size);

	RL_CALL_VERBOSE(rl_multi_string_get, RL_OK, db, page, &testdata, &testdatalen);
	EXPECT_LONG(testdatalen, size + append_size);
	EXPECT_BYTES(testdata, size, data, size);
	EXPECT_BYTES(&testdata[size], append_size, append_data, append_size);
	rl_free(testdata);

	rl_free(data);
	rl_free(append_data);
	rl_close(db);
	PASS();
}

TEST test_substr(long strsize, long start, long stop, long startindex, long expectedsize)
{
	int retval;
	unsigned char *data = malloc(sizeof(unsigned char) * strsize), *data2;
	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	long page, i, size2;
	for (i = 0; i < strsize; i++) {
		data[i] = i % 123;
	}

	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &page, data, strsize);
	RL_CALL_VERBOSE(rl_multi_string_getrange, RL_OK, db, page, &data2, &size2, start, stop);
	EXPECT_BYTES(&data[startindex], size2, data2, expectedsize);
	rl_free(data2);
	free(data);
	rl_close(db);
	PASS();
}

TEST test_setrange(long initialsize, long index, long updatesize)
{
	int retval;
	long finalsize = index + updatesize > initialsize ? index + updatesize : initialsize;
	long newlength, testdatalen;
	unsigned char *testdata;
	unsigned char *finaldata = calloc(finalsize, sizeof(unsigned char));
	unsigned char *initialdata = malloc(sizeof(unsigned char) * initialsize);
	unsigned char *updatedata = malloc(sizeof(unsigned char) * updatesize);
	rlite *db = NULL;
	RL_CALL_VERBOSE(rl_open, RL_OK, ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);

	long page, i;
	for (i = 0; i < initialsize; i++) {
		finaldata[i] = initialdata[i] = i % 123;
	}
	for (i = 0; i < updatesize; i++) {
		finaldata[index + i] = updatedata[i] = i % 151;
	}

	RL_CALL_VERBOSE(rl_multi_string_set, RL_OK, db, &page, initialdata, initialsize);
	RL_CALL_VERBOSE(rl_multi_string_setrange, RL_OK, db, page, updatedata, updatesize, index, &newlength);
	EXPECT_LONG(finalsize, newlength);
	RL_CALL_VERBOSE(rl_multi_string_get, RL_OK, db, page, &testdata, &testdatalen);
	EXPECT_BYTES(finaldata, finalsize, testdata, testdatalen);
	rl_free(testdata);

	free(finaldata);
	free(initialdata);
	free(updatedata);
	rl_close(db);
	PASS();
}

SUITE(multi_string_test)
{
	RUN_TEST(basic_set_get);
	RUN_TEST(empty_set_get);
	RUN_TEST(test_cmp_different_length);
	RUN_TESTp(test_cmp, 0, 0, 1);
	RUN_TESTp(test_sha, 100);
	RUN_TESTp(test_sha, 1000);
	RUN_TESTp(test_append, 10, 20);
	RUN_TESTp(test_append, 10, 1200);
	RUN_TESTp(test_append, 1200, 10);
	RUN_TESTp(test_append, 1000, 2000);
	long i, j;
	for (i = 0; i < 2; i++) {
		for (j = 0; j < CMP_SIZE / 500; j++) {
			RUN_TESTp(test_cmp, i * 2 - 1, j * 500, (j + 1) * 500);
			RUN_TESTp(test_cmp2, i * 2 - 1, j * 500, (j + 1) * 500);
		}
	}
	RUN_TESTp(test_substr, 20, 0, 10, 0, 11);
	RUN_TESTp(test_substr, 2000, 1, -1, 1, 1999);
	RUN_TESTp(test_substr, 2000, -10, -1, 1990, 10);
	RUN_TESTp(test_setrange, 10, 5, 3);
	RUN_TESTp(test_setrange, 10, 5, 20);
	RUN_TESTp(test_setrange, 10, 20, 5);
	RUN_TESTp(test_setrange, 1024, 1024, 1024);
	RUN_TESTp(test_setrange, 1024, 100, 1024);
	RUN_TESTp(test_setrange, 1024, 1024, 100);
}
