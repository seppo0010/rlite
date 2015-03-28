#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "util.h"
#include "../src/rlite.h"
#include "../src/rlite/status.h"
#include "../src/rlite/page_long.h"

TEST do_long_test(int _commit)
{
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	long data = 123, data2;
	long number;

	RL_CALL_VERBOSE(rl_long_create, RL_OK, db, data, &number);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_long_get, RL_OK, db, &data2, number);
	EXPECT_LONG(data, data2);

	rl_close(db);
	PASS();
}

SUITE(long_test)
{
	RUN_TEST1(do_long_test, 0);
	RUN_TEST1(do_long_test, 1);
	RUN_TEST1(do_long_test, 2);
}
