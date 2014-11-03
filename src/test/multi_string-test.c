#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "../rlite.h"
#include "../page_multi_string.h"

int basic_set_get()
{
	int retval, i, j;
	long size, size2, number;
	unsigned char *data, *data2;

	rlite *db;
	if (rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return 1;
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
			goto cleanup;
		}
		if (memcmp(data, data2, size) != 0) {
			fprintf(stderr, "Data mismatch\n");
			goto cleanup;
		}
		free(data);
		free(data2);
		fprintf(stderr, "End page_multi_string-test %ld\n", size);
	}

	rl_close(db);
	retval = 0;
cleanup:
	return retval;
}

#define CMP_SIZE 2000
static int test_cmp(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int cmp;
	long p1, p2;
	rlite *db;
	if (position % 500 == 0) {
		fprintf(stderr, "Start page_multi_string-test test_cmp %d %ld\n", expected_cmp, position);
	}
	if (rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return 1;
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
	int retval = rl_multi_string_set(db, &p1, data, size);
	if (retval != 0) {
		return 1;
	}
	retval = rl_multi_string_set(db, &p2, data2, size);
	if (retval != 0) {
		return 1;
	}
	retval = rl_multi_string_cmp(db, p1, p2, &cmp);
	if (retval != 0) {
		return 1;
	}
	if (cmp != expected_cmp) {
		fprintf(stderr, "Expected cmp=%d got %d instead\n", expected_cmp, cmp);
		return 1;
	}

	rl_close(db);
	if (position % 500 == 0) {
		fprintf(stderr, "End page_multi_string-test basic_cmp\n");
	}
	return 0;
}

static int test_cmp2(int expected_cmp, long position)
{
	unsigned char data[CMP_SIZE], data2[CMP_SIZE];
	long size = CMP_SIZE, i;
	int cmp;
	long p1;
	rlite *db;
	if (position % 500 == 0) {
		fprintf(stderr, "Start page_multi_string-test test_cmp2 %d %ld\n", expected_cmp, position);
	}
	if (rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return 1;
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
	int retval = rl_multi_string_set(db, &p1, data, size);
	if (retval != 0) {
		return 1;
	}
	retval = rl_multi_string_cmp_str(db, p1, data2, size, &cmp);
	if (retval != 0) {
		return 1;
	}
	if (cmp != expected_cmp) {
		fprintf(stderr, "Expected cmp=%d got %d instead\n", expected_cmp, cmp);
		return 1;
	}

	rl_close(db);
	if (position % 500 == 0) {
		fprintf(stderr, "End page_multi_string-test basic_cmp\n");
	}
	return 0;
}

int main()
{
	int retval = basic_set_get();
	if (retval != 0) {
		return retval;
	}
	retval = test_cmp(0, 0);
	if (retval != 0) {
		return retval;
	}
	long i, j;
	for (i = 0; i < 2; i++) {
		for (j = 0; j < CMP_SIZE; j++) {
			retval = test_cmp(i * 2 - 1, j);
			if (retval != 0) {
				return retval;
			}
			retval = test_cmp2(i * 2 - 1, j);
			if (retval != 0) {
				return retval;
			}
		}
	}
	return retval;
}
