#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "greatest.h"
#include "../src/rlite/rlite.h"
#include "../src/rlite/flock.h"

TEST basic_flock_check()
{
	const char *path = "flock-test";

	remove(path);

	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_SH), RL_NOT_FOUND);
	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_EX), RL_NOT_FOUND);

	FILE *fp = fopen(path, "w");

	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_SH), RL_NOT_FOUND);
	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_EX), RL_NOT_FOUND);

	rl_flock(fp, RLITE_FLOCK_SH);

	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_SH), RL_FOUND);
	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_EX), RL_NOT_FOUND);

	rl_flock(fp, RLITE_FLOCK_EX);

	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_SH), RL_NOT_FOUND);
	ASSERT_EQ(rl_is_flocked(path, RLITE_FLOCK_EX), RL_FOUND);

	fclose(fp);
	remove(path);

	PASS();
}

SUITE(flock_test)
{
	RUN_TEST(basic_flock_check);
}

