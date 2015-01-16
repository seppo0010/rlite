#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "test_util.h"
#include "../rlite.h"
#include "../status.h"
#include "../page_string.h"

static int do_string_test()
{
	srand(1);
	fprintf(stderr, "Start do_string_test\n");
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	unsigned char *data, *data2;
	long number, i;

	RL_CALL_VERBOSE(rl_string_create, RL_OK, db, &data, &number);

	for (i = 0; i < db->page_size; i++) {
		data[i] = (char)(rand() % CHAR_MAX);
	}

	RL_CALL_VERBOSE(rl_string_get, RL_OK, db, &data2, number);

	EXPECT_BYTES(data, db->page_size, data2, db->page_size);
	fprintf(stderr, "End do_string_test\n");
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(string_test)
{
	RL_TEST(do_string_test, 0);
}
RL_TEST_MAIN_END
