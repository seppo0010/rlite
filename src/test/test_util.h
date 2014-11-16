#ifndef _RL_TEST_UTIL_H
#define _RL_TEST_UTIL_H
#include "../util.h"
struct rlite;

int setup_db(struct rlite **db, int file, int del);

#define RL_TEST_MAIN_END return retval; }

#ifdef DEBUG
#define RL_TEST(func, ...)\
	if (*passed_tests >= *skip_tests) {\
		had_oom = expect_fail();\
		retval = func(__VA_ARGS__);\
		if (!had_oom && expect_fail()) {\
			return retval == RL_OUT_OF_MEMORY ? 0 : retval;\
		}\
		else if (retval != RL_OK) return retval;\
	}\
	*passed_tests += 1;\
	fprintf(stderr, "passed_tests %d\n", *passed_tests);

#define RL_TEST_MAIN_DECL(name) int name(int *skip_tests, int *passed_tests)

#define RL_TEST_MAIN_START(name)\
	int name(int *skip_tests, int *passed_tests) {\
		int had_oom, retval = 0;

#else

#define RL_TEST(func, ...)\
	retval = func(__VA_ARGS__);\
	if (retval != RL_OK) return retval;

#define RL_TEST_MAIN_DECL(name) int name()
#define RL_TEST_MAIN_START(name)\
	int name() { int retval = 0;

#endif

#endif
