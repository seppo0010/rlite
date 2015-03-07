#ifndef _RL_TEST_UTIL_H
#define _RL_TEST_UTIL_H

#include "../src/util.h"
struct rlite;

int setup_db(struct rlite **db, int file, int del);

#define RL_TEST_MAIN_END return retval; }

#define RL_CALL_VERBOSE(func, expected, ...)\
	retval = func(__VA_ARGS__);\
	if (expected != retval) { fprintf(stderr, "Failed to %s, expected %d but got %d instead on line %d\n", #func, expected, retval, __LINE__); goto cleanup; }

#define RL_CALL2_VERBOSE(func, expected, expected2, ...)\
	retval = func(__VA_ARGS__);\
	if (expected != retval && expected2 != retval) { fprintf(stderr, "Failed to %s, expected %d or %d but got %d instead on line %d\n", #func, expected, expected2, retval, __LINE__); goto cleanup; }

#define UNSIGN(str) ((unsigned char *)(str))

#define EXPECT_PTR(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %p == %p instead on line %d\n", (void *)(v1), (void *)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_INT(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %d == %d instead on line %d\n", (int)(v1), (int)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_LONG(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %ld == %ld instead on line %d\n", (long)(v1), (long)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_DOUBLE(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %lf == %lf instead on line %d\n", (double)(v1), (double)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_LL(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %lld == %lld instead on line %d\n", (long long)(v1), (long long)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_LLU(v1, v2)\
	if ((v1) != (v2)) {\
		fprintf(stderr, "Expected %llu == %llu instead on line %d\n", (long long unsigned)(v1), (long long unsigned)(v2), __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define EXPECT_BYTES(s1, l1, s2, l2)\
	if ((l1) != (l2) || memcmp(s1, s2, l1) != 0) {\
		fprintf(stderr, "Expected \"%s\" (%lu) == \"%s\" (%lu) instead on line %d\n", s1, (size_t)l1, s2, (size_t)l2, __LINE__);\
		retval = RL_UNEXPECTED;\
		goto cleanup;\
	}

#define RL_COMMIT()\
	if (_commit) {\
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);\
	}

#define RL_BALANCED()\
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);\
	if (_commit) {\
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);\
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);\
	}

#define EXPECT_STR(s1, s2, l2) EXPECT_BYTES(s1, strlen((char *)s1), s2, l2)

#ifdef RL_DEBUG
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
