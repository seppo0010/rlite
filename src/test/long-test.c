#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../rlite.h"
#include "../status.h"
#include "../page_long.h"

static int do_long_test(int commit)
{
	fprintf(stderr, "Start do_long_test %d\n", commit);
	int retval;
	rlite *db = NULL;
	RL_CALL(setup_db, RL_OK, &db, commit, 1);

	long data = 123, data2;
	long number;

	retval = rl_long_create(db, data, &number);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to create long\n");
		goto cleanup;
	}

	if (commit) {
		RL_CALL(rl_commit, RL_OK, db);
	}

	retval = rl_long_get(db, &data2, number);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to get long\n");
		goto cleanup;
	}

	if (data != data2) {
		fprintf(stderr, "data (%ld) != data2 (%ld)\n", data, data2);
		retval = 1;
		goto cleanup;
	}

	fprintf(stderr, "End do_long_test\n");
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(long_test)
{
	RL_TEST(do_long_test, 0);
	RL_TEST(do_long_test, 1);
}
RL_TEST_MAIN_END
