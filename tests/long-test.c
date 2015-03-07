#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../src/rlite.h"
#include "../src/status.h"
#include "../src/page_long.h"

static int do_long_test(int _commit)
{
	fprintf(stderr, "Start do_long_test %d\n", _commit);
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	long data = 123, data2;
	long number;

	RL_CALL_VERBOSE(rl_long_create, RL_OK, db, data, &number);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_long_get, RL_OK, db, &data2, number);
	EXPECT_LONG(data, data2);

	fprintf(stderr, "End do_long_test\n");
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(long_test)
{
	RL_TEST(do_long_test, 0);
	RL_TEST(do_long_test, 1);
	RL_TEST(do_long_test, 2);
}
RL_TEST_MAIN_END
