#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "util.h"
#include "../src/rlite.h"
#include "../src/rlite/status.h"
#include "../src/rlite/page_string.h"

TEST do_string_test()
{
	srand(1);
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
	rl_close(db);
	PASS();
}

SUITE(string_test)
{
	RUN_TEST(do_string_test);
}
