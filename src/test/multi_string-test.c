#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "../rlite.h"
#include "../page_multi_string.h"
#include "../util.h"
#include "test_util.h"

int basic_set_get()
{
	int retval, i, j;
	long size, size2, number;
	unsigned char *data, *data2;

	rlite *db = NULL;
	retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}

	for (i = 0; i < 2; i++) {
		srand(1);
		size = i == 0 ? 20 : 1020;
		fprintf(stderr, "Start page_multi_string-test %ld\n", size);
		data = malloc(sizeof(unsigned char) * size);
		for (j = 0; j < size; j++) {
			data[j] = (unsigned char)(rand() / CHAR_MAX);
		}
		retval = rl_multi_string_set(db, &number, data, size);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to set data\n");
			goto cleanup;
		}
		retval = rl_multi_string_get(db, number, &data2, &size2);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to get data\n");
			goto cleanup;
		}
		if (size != size2) {
			fprintf(stderr, "Data size mismatch\n");
			retval = 1;
			goto cleanup;
		}
		if (memcmp(data, data2, size) != 0) {
			fprintf(stderr, "Data mismatch\n");
			retval = 1;
			goto cleanup;
		}
		rl_free(data);
		rl_free(data2);
		fprintf(stderr, "End page_multi_string-test %ld\n", size);
	}

	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_sha(long size)
{
	fprintf(stderr, "Start test_sha %ld\n", size);
	unsigned char *data = malloc(sizeof(unsigned char) * size);
	rlite *db = NULL;
	int retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}

	long page, i;
	for (i = 0; i < size; i++) {
		data[i] = i % 123;
	}

	retval = rl_multi_string_set(db, &page, data, size);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to set multi string, got %d\n", retval);
		goto cleanup;
	}

	unsigned char digest1[20], digest2[20];

	retval = rl_multi_string_sha1(db, digest1, page);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to sha1 multi string, got %d\n", retval);
		goto cleanup;
	}

	retval = sha1(data, size, digest2);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to sha1 char*, got %d\n", retval);
		goto cleanup;
	}

	if (memcmp(digest1, digest2, 20) != 0) {
		fprintf(stderr, "Digest mismatch for multi string\n");
		retval = 1;
		goto cleanup;
	}

	fprintf(stderr, "End test_sha %ld\n", size);
	retval = 0;
cleanup:
	rl_free(data);
	rl_close(db);
	return retval;
}

static int assert_cmp(rlite *db, long p1, unsigned char *data, long size, int expected_cmp)
{
	long p2;
	int cmp;
	int retval = rl_multi_string_cmp_str(db, p1, data, size, &cmp);
	if (retval != 0) {
		fprintf(stderr, "Failed to cmp, got %d on line %d\n", retval, __LINE__);
		goto cleanup;
	}
	if (cmp != expected_cmp) {
		fprintf(stderr, "Expected cmp=%d got %d instead on line %d\n", expected_cmp, cmp, __LINE__);
		goto cleanup;
	}
	if (data) {
		retval = rl_multi_string_set(db, &p2, data, size);
		if (retval != 0) {
			fprintf(stderr, "Failed to set, got %d on line %d\n", retval, __LINE__);
			goto cleanup;
		}
		retval = rl_multi_string_cmp(db, p1, p2, &cmp);
		if (retval != 0) {
			fprintf(stderr, "Failed to cmp, got %d on line %d\n", retval, __LINE__);
			goto cleanup;
		}
		if (cmp != expected_cmp) {
			fprintf(stderr, "Expected cmp=%d got %d instead on line %d\n", expected_cmp, cmp, __LINE__);
			retval = 1;
			goto cleanup;
		}
	}
	retval = 0;
cleanup:
	return retval;
}
static int test_cmp_different_length()
{
	unsigned char data[3], data2[3];
	long size = 2, i;
	long p1;
	rlite *db = NULL;
	fprintf(stderr, "Start test_cmp_different_length\n");
	int retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}
	for (i = 0; i < 3; i++) {
		data[i] = data2[i] = 100 + i;
	}
	retval = rl_multi_string_set(db, &p1, data, size);
	if (retval != 0) {
		goto cleanup;
	}
	retval = assert_cmp(db, p1, data2, 0, 1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = assert_cmp(db, p1, data2, size, 0);
	if (retval != 0) {
		goto cleanup;
	}
	retval = assert_cmp(db, p1, data2, size - 1, 1);
	if (retval != 0) {
		goto cleanup;
	}
	data2[0]++;
	retval = assert_cmp(db, p1, data2, size - 1, -1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = assert_cmp(db, p1, data2, size, -1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = assert_cmp(db, p1, NULL, 0, 1);
	if (retval != 0) {
		goto cleanup;
	}

	rl_close(db);
	fprintf(stderr, "End test_cmp_different_length\n");
	retval = 0;
cleanup:
	return retval;
}

#define CMP_SIZE 2000
static int test_cmp_internal(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int cmp;
	long p1, p2;
	rlite *db = NULL;
	if (position % 500 == 0) {
	}
	int retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}
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
	retval = rl_multi_string_set(db, &p1, data, size);
	if (retval != 0) {
		goto cleanup;
	}
	retval = rl_multi_string_set(db, &p2, data2, size);
	if (retval != 0) {
		goto cleanup;
	}
	retval = rl_multi_string_cmp(db, p1, p2, &cmp);
	if (retval != 0) {
		goto cleanup;
	}
	if (cmp != expected_cmp) {
		fprintf(stderr, "Expected cmp=%d got %d instead\n", expected_cmp, cmp);
		retval = 1;
		goto cleanup;
	}

	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_cmp(int expected_cmp, long position_from, long position_to)
{
	int retval = RL_OK;
	long i;
	fprintf(stderr, "Start page_multi_string-test test_cmp %d %ld %ld\n", expected_cmp, position_from, position_to);
	for (i = position_from; i < position_to; i++) {
		RL_CALL(test_cmp_internal, 0, expected_cmp, i);
	}
	fprintf(stderr, "End page_multi_string-test basic_cmp\n");
cleanup:
	return retval;
}

static int test_cmp2_internal(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int cmp;
	long p1;
	rlite *db = NULL;
	int retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}
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
	retval = rl_multi_string_set(db, &p1, data, size);
	if (retval != 0) {
		goto cleanup;
	}
	retval = rl_multi_string_cmp_str(db, p1, data2, size, &cmp);
	if (retval != 0) {
		goto cleanup;
	}
	if (cmp != expected_cmp) {
		fprintf(stderr, "Expected cmp=%d got %d instead\n", expected_cmp, cmp);
		retval = 1;
		goto cleanup;
	}

	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_cmp2(int expected_cmp, long position_from, long position_to)
{
	int retval = RL_OK;
	long i;
	fprintf(stderr, "Start page_multi_string-test test_cmp2 %d %ld %ld\n", expected_cmp, position_from, position_to);
	for (i = position_from; i < position_to; i++) {
		RL_CALL(test_cmp2_internal, 0, expected_cmp, i);
	}
	fprintf(stderr, "End page_multi_string-test basic_cmp2\n");
cleanup:
	return retval;
}

static int test_append(long size, long append_size)
{
	fprintf(stderr, "Start test_append %ld %ld\n", size, append_size);
	unsigned char *data = malloc(sizeof(unsigned char) * size);
	unsigned char *append_data = malloc(sizeof(unsigned char) * append_size);
	unsigned char *testdata;
	long testdatalen;
	rlite *db = NULL;
	int retval = rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		goto cleanup;
	}

	long page, i;
	for (i = 0; i < size; i++) {
		data[i] = i % 123;
	}
	for (i = 0; i < append_size; i++) {
		append_data[i] = i % 123;
	}

	retval = rl_multi_string_set(db, &page, data, size);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to set multi string, got %d\n", retval);
		goto cleanup;
	}

	retval = rl_multi_string_append(db, page, append_data, append_size);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to append multi string, got %d\n", retval);
		goto cleanup;
	}

	retval = rl_multi_string_get(db, page, &testdata, &testdatalen);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to get multi string, got %d\n", retval);
		goto cleanup;
	}

	if (testdatalen != size + append_size) {
		fprintf(stderr, "Expected size to be %ld, got %ld instead on line %d\n", size + append_size, testdatalen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (memcmp(testdata, data, size) != 0) {
		fprintf(stderr, "Existing value corrupted after append on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (memcmp(&testdata[size], append_data, append_size) != 0) {
		fprintf(stderr, "Appended data corrupted on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(testdata);

	fprintf(stderr, "End test_append %ld\n", size);
	retval = 0;
cleanup:
	rl_free(data);
	rl_free(append_data);
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(multi_string_test)
{
	RL_TEST(basic_set_get, 0);
	RL_TEST(test_cmp_different_length, 0);
	RL_TEST(test_cmp, 0, 0, 1);
	RL_TEST(test_sha, 100);
	RL_TEST(test_sha, 1000);
	RL_TEST(test_append, 10, 20);
	RL_TEST(test_append, 10, 1200);
	RL_TEST(test_append, 1200, 10);
	RL_TEST(test_append, 1000, 2000);
	long i, j;
	for (i = 0; i < 2; i++) {
		for (j = 0; j < CMP_SIZE / 500; j++) {
			RL_TEST(test_cmp, i * 2 - 1, j * 500, (j + 1) * 500);
			RL_TEST(test_cmp2, i * 2 - 1, j * 500, (j + 1) * 500);
		}
	}
}
RL_TEST_MAIN_END
