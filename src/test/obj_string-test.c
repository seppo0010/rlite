#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "../rlite.h"
#include "../obj_string.h"

int main()
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
		fprintf(stderr, "Start obj_string-test %ld\n", size);
		data = malloc(sizeof(unsigned char) * size);
		for (j = 0; j < size; j++) {
			data[j] = (unsigned char)(rand() / CHAR_MAX);
		}
		retval = rl_obj_string_set(db, &number, data, size);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to set data\n");
			goto cleanup;
		}
		retval = rl_obj_string_get(db, number, &data2, &size2);
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
		fprintf(stderr, "End obj_string-test %ld\n", size);
	}
	rl_close(db);
cleanup:
	return retval;
}
