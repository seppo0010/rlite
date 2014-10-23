#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "rlite.h"
#include "status.h"
#include "page_string.h"

int main()
{
	fprintf(stderr, "Start string_test\n");
	srand(1);
	int retval;
	rlite *db;
	if (rl_open(":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return 1;
	}

	unsigned char *data, *data2;
	long number, i;

	retval = rl_string_create(db, &data, &number);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create string\n");
		return 1;
	}

	for (i = 0; i < db->page_size; i++) {
		data[i] = (char)(rand() % CHAR_MAX);
	}

	retval = rl_string_get(db, &data2, number);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to get string\n");
		return 1;
	}

	if (memcmp(data, data2, sizeof(char) * db->page_size) != 0) {
		fprintf(stderr, "data != data2\n");
		return 1;
	}

	rl_close(db);
	fprintf(stderr, "End string_test\n");
	return 0;
}
