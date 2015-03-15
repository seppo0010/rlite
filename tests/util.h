#ifndef _RL_TEST_UTIL_H
#define _RL_TEST_UTIL_H

#include "greatest.h"
#include "../src/util.h"
struct rlite;

int setup_db(struct rlite **db, int file, int del);

static inline int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

#define RL_CALL_VERBOSE(func, expected, ...)\
	retval = func(__VA_ARGS__);\
	ASSERT_EQ(expected, retval);

#define RL_CALL2_VERBOSE(func, expected, expected2, ...)\
	retval = func(__VA_ARGS__);\
	ASSERT(expected == retval || expected2 == retval);

#define UNSIGN(str) ((unsigned char *)(str))

#define EXPECT_PTR(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_INT(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_LONG(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_DOUBLE(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_LL(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_LLU(v1, v2)\
	ASSERT_EQ(v1, v2);

#define EXPECT_BYTES(s1, l1, s2, l2)\
	ASSERT((l1) == (l2) && memcmp(s1, s2, l1) == 0);

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

#define EXPECT_REPLY_NO_ERROR(reply)\
	ASSERT(reply->type != RLITE_REPLY_ERROR);

#define EXPECT_REPLY_OBJ(expectedtype, reply, string, strlen)\
	if (expectedtype != RLITE_REPLY_ERROR) {\
		EXPECT_REPLY_NO_ERROR(reply);\
	}\
	ASSERT_EQ(reply->type, expectedtype);\
	ASSERT_EQ(reply->len, strlen);\
	ASSERT(memcmp(reply->str, string, strlen) == 0);\

#define EXPECT_REPLY_STATUS(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_STATUS, reply, string, strlen)
#define EXPECT_REPLY_STR(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_STRING, reply, string, strlen)
#define EXPECT_REPLY_ERROR_STR(reply, string, strlen) EXPECT_REPLY_OBJ(RLITE_REPLY_ERROR, reply, string, strlen)

#define EXPECT_REPLY_ERROR(reply)\
	ASSERT_EQ(reply->type, RLITE_REPLY_ERROR);

#define EXPECT_REPLY_INTEGER(reply, expectedinteger)\
	EXPECT_REPLY_NO_ERROR(reply);\
	ASSERT_EQ(reply->type, RLITE_REPLY_INTEGER);\
	ASSERT_EQ(reply->integer, expectedinteger);

#define EXPECT_REPLY_INTEGER_LTE(reply, expectedinteger)\
	EXPECT_REPLY_NO_ERROR(reply);\
	ASSERT_EQ(reply->type, RLITE_REPLY_INTEGER);\
	ASSERT(reply->integer <= expectedinteger);

#define EXPECT_REPLY_NIL(reply)\
	EXPECT_REPLY_NO_ERROR(reply);\
	ASSERT_EQ(reply->type, RLITE_REPLY_NIL);

#define EXPECT_REPLY_LEN(reply, expectedlen)\
	EXPECT_REPLY_NO_ERROR(reply);\
	ASSERT_EQ(reply->type, RLITE_REPLY_ARRAY);\
	ASSERT_EQ(reply->elements, expectedlen);

#endif
