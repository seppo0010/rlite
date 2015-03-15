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

#define EXPECT_REPLY_NO_ERROR(reply)\
	if (reply->type == RLITE_REPLY_ERROR) {\
		fprintf(stderr, "Unexpected error \"%s\" on %s:%d\n", reply->str, __FILE__, __LINE__);\
		return 1;\
	}\

#define EXPECT_REPLY_OBJ(expectedtype, reply, string, strlen)\
	if (expectedtype != RLITE_REPLY_ERROR) {\
		EXPECT_REPLY_NO_ERROR(reply);\
	}\
	if (reply->type != expectedtype) {\
		fprintf(stderr, "Expected reply to be %d, got %d instead on %s:%d\n", expectedtype, reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->len != strlen || memcmp(reply->str, string, strlen) != 0) {\
		fprintf(stderr, "Expected reply to be \"%s\" (%d), got \"%s\" (%d) instead on %s:%d\n", string, strlen, reply->str, reply->len, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_REPLY_STATUS(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_STATUS, reply, string, strlen)
#define EXPECT_REPLY_STR(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_STRING, reply, string, strlen)
#define EXPECT_REPLY_ERROR_STR(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_ERROR, reply, string, strlen)

#define EXPECT_REPLY_ERROR(reply)\
	if (reply->type != RLITE_REPLY_ERROR) {\
		fprintf(stderr, "Expected error got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\

#define EXPECT_REPLY_INTEGER(reply, expectedinteger)\
	EXPECT_REPLY_NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_INTEGER) {\
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->integer != (expectedinteger)) {\
		fprintf(stderr, "Expected reply to be %lld, got %lld instead on %s:%d\n", (long long)(expectedinteger), reply->integer, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_REPLY_INTEGER_LTE(reply, expectedinteger)\
	EXPECT_REPLY_NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_INTEGER) {\
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->integer > expectedinteger) {\
		fprintf(stderr, "Expected reply to be at most %lld, got %lld instead on %s:%d\n", (long long)expectedinteger, reply->integer, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_REPLY_NIL(reply)\
	EXPECT_REPLY_NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_NIL) {\
		fprintf(stderr, "Expected reply to be NIL, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}
#define EXPECT_REPLY_LEN(reply, expectedlen)\
	EXPECT_REPLY_NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_ARRAY) {\
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->elements != expectedlen) {\
		fprintf(stderr, "Expected reply length to be %ld got %ld instead on %s:%d\n", (long)expectedlen, reply->elements, __FILE__, __LINE__);\
		return 1;\
	}

#endif
