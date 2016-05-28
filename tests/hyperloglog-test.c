#include "../src/rlite/hyperloglog.h"
#include "util.h"
#include "../src/rlite/rlite.h"

int test_hyperloglog_selftest()
{
	return rl_str_pfselftest();
}

int test_hyperloglog_add_count()
{
	int retval;
	unsigned char *str = NULL;
	long strlen = -1, card = -1;
	unsigned char *argv[] = {UNSIGN("a"), UNSIGN("b"), UNSIGN("c")};
	long argvlen[] = {1, 1, 1};
	RL_CALL_VERBOSE(rl_str_pfadd, 1, NULL, 0, 3, argv, argvlen, &str, &strlen);
	RL_CALL_VERBOSE(rl_str_pfcount, 0, 1, &str, &strlen, &card, NULL, NULL);
	EXPECT_LONG(card, 3);
	free(str);
	PASS();
}

int test_hyperloglog_add_merge()
{
	int retval;
	unsigned char *str = NULL, *str2 = NULL, *str3;
	long strlen = -1, strlen2, strlen3, card = -1;

	unsigned char *argv[] = {UNSIGN("a"), UNSIGN("b"), UNSIGN("c")};
	long argvlen[] = {1, 1, 1};
	unsigned char *argv2[] = {UNSIGN("aa"), UNSIGN("ba"), UNSIGN("ca")};
	long argvlen2[] = {2, 2, 2};

	RL_CALL_VERBOSE(rl_str_pfadd, 1, NULL, 0, 3, argv, argvlen, &str, &strlen);
	RL_CALL_VERBOSE(rl_str_pfadd, 1, NULL, 0, 3, argv2, argvlen2, &str2, &strlen2);
	unsigned char *strs[] = {str, str2};
	long strslen[] = {strlen, strlen2};
	RL_CALL_VERBOSE(rl_str_pfmerge, 0, 2, strs, strslen, &str3, &strlen3);
	RL_CALL_VERBOSE(rl_str_pfcount, 0, 1, &str3, &strlen3, &card, NULL, NULL);
	EXPECT_LONG(card, 6);
	free(str);
	free(str2);
	free(str3);
	PASS();
}


SUITE(hyperloglog_test)
{
	RUN_TEST(test_hyperloglog_selftest);
	RUN_TEST(test_hyperloglog_add_count);
	RUN_TEST(test_hyperloglog_add_merge);
}
