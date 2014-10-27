#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../page_skiplist.h"
#include "../obj_string.h"

int basic_skiplist_test(int sign)
{
	fprintf(stderr, "Start basic_skiplist_test %d\n", sign);

	rlite *db = setup_db(0, 1);
	rl_skiplist *skiplist;
	if (rl_skiplist_create(db, &skiplist) != RL_OK) {
		return 1;
	}

	int retval;
	long i, page;
	unsigned char *data = malloc(sizeof(unsigned char) * 1);
	for (i = 0; i < 200; i++) {
		data[0] = i;
		retval = rl_obj_string_set(db, &page, data, 1);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to set %ld string, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_add(db, skiplist, 5.2 * i * sign, page);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to add item %ld to skiplist, got %d\n", i, retval);
			return 1;
		}
		retval = rl_skiplist_is_balanced(db, skiplist);
		if (retval != RL_OK) {
			fprintf(stderr, "Skiplist is not balanced after item %ld\n", i);
			return 1;
		}
	}
	rl_skiplist_destroy(db, skiplist);
	free(data);
	rl_close(db);
	fprintf(stderr, "End basic_skiplist_test\n");
	return 0;
}

int main()
{
	int retval = 0;
	retval = basic_skiplist_test(0);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_skiplist_test(1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_skiplist_test(-1);
	if (retval != 0) {
		goto cleanup;
	}
cleanup:
	return retval;
}
