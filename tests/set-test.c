#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "hirlite.h"
#include "util.h"

static void sadd(rliteContext* context, char *key, char *element) {
	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"sadd", key, element, NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);
}

static int randomSadd(rliteContext* context, char *key, int elements) {
	rliteReply* reply;
	if (elements == 0) {
		elements = 3;
	}
	char* argv[100] = {"sadd", key, NULL};
	int i, j, len;
	for (i = 0; i < elements; i++) {
		len = 5 + floor(((float)rand() / RAND_MAX) * 10);
		argv[2 + i] = malloc(sizeof(char) * len);
		for (j = 0; j < len - 1; j++) {
			argv[2 + i][j] = 'a' + (int)floor(((float)rand() / RAND_MAX) * 25);
		}
		argv[2 + i][len - 1] = 0;
	}

	size_t argvlen[100];
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, elements);
	rliteFreeReplyObject(reply);
	for (i = 0; i < elements; i++) {
		free(argv[2 + i]);
	}
	return 0;
}

TEST test_sadd() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"sadd", "myset", "member1", "member2", "anothermember", "member1", NULL};
	size_t argvlen[100];
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

TEST test_scard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	if (randomSadd(context, "myset", 5) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"scard", "myset", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 5);
	rliteFreeReplyObject(reply);

	if (randomSadd(context, "myset", 3) != 0) {
		return 1;
	}

	char* argv2[100] = {"scard", "myset", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 8);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

TEST test_sismember() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	sadd(context, "myset", "mymember");

	rliteReply* reply;
	char* argv[100] = {"sismember", "myset", "mymember", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"sismember", "myset", "not a member", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

TEST test_smove() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	sadd(context, "myset", "mymember");

	rliteReply* reply;
	{
		char* argv[100] = {"smove", "myset", "otherset", "mymember", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "myset", "mymember", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "otherset", "mymember", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"smove", "otherset", "myset", "member2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "myset", "member2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_spop() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"spop", "myset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		ASSERT_EQ(reply->type, RLITE_REPLY_STRING);
		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			FAIL();
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"spop", "myset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		ASSERT_EQ(reply->type, RLITE_REPLY_STRING);
		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			FAIL();
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"spop", "myset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_srandmember_nocount() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		ASSERT_EQ(reply->type, RLITE_REPLY_STRING);
		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			FAIL();
		}
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

TEST test_srandmember_1() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		if (!(reply->element[0]->len == (int)strlen(m1) && memcmp(reply->element[0]->str, m1, reply->element[0]->len) == 0) &&
			!(reply->element[0]->len == (int)strlen(m2) && memcmp(reply->element[0]->str, m2, reply->element[0]->len) == 0)) {
			FAIL();
		}
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

TEST test_srandmember_10_unique() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "10", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 2);

		if (!(reply->element[0]->len == (int)strlen(m1) && memcmp(reply->element[0]->str, m1, reply->element[0]->len) == 0) &&
			!(reply->element[0]->len == (int)strlen(m2) && memcmp(reply->element[0]->str, m2, reply->element[0]->len) == 0)) {
			FAIL();
		}

		if (!(reply->element[1]->len == (int)strlen(m1) && memcmp(reply->element[1]->str, m1, reply->element[1]->len) == 0) &&
			!(reply->element[1]->len == (int)strlen(m2) && memcmp(reply->element[1]->str, m2, reply->element[1]->len) == 0)) {
			FAIL();
		}

		if (reply->element[0]->len == reply->element[1]->len) {
			FAIL();
		}

		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

TEST test_srandmember_10_non_unique() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	long i;
	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "-10", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 10);

		for (i = 0; i < 10; i++) {
			if (!(reply->element[i]->len == (int)strlen(m1) && memcmp(reply->element[i]->str, m1, reply->element[i]->len) == 0) &&
				!(reply->element[i]->len == (int)strlen(m2) && memcmp(reply->element[i]->str, m2, reply->element[i]->len) == 0)) {
				FAIL();
			}
		}

		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

TEST test_srem() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srem", "myset", m1, "other", m2, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "mykey", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_smembers() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"smembers", "myset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sinter() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sinter", s1, s2, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], m1, strlen(m1));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sinterstore() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2", *t = "target";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sinterstore", t, s1, s2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"smembers", t, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], m1, strlen(m1));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sunion() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sunion", s1, s2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sunionstore() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2", *t = "target";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sunionstore", t, s1, s2, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"smembers", t, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sdiff() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sdiff", s1, s2, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], m2, strlen(m2));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

TEST test_sdiffstore() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2", *s1 = "myset", *s2 = "myset2", *t = "target";
	sadd(context, s1, m1);
	sadd(context, s1, m2);
	sadd(context, s2, m1);
	sadd(context, s2, "meh");

	rliteReply* reply;
	{
		char* argv[100] = {"sdiffstore", t, s1, s2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"smembers", t, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], m2, strlen(m2));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

SUITE(set_test)
{
	RUN_TEST(test_sadd);
	RUN_TEST(test_scard);
	RUN_TEST(test_sismember);
	RUN_TEST(test_smove);
	RUN_TEST(test_spop);
	RUN_TEST(test_srandmember_nocount);
	RUN_TEST(test_srandmember_1);
	RUN_TEST(test_srandmember_10_unique);
	RUN_TEST(test_srandmember_10_non_unique);
	RUN_TEST(test_srem);
	RUN_TEST(test_smembers);
	RUN_TEST(test_sinter);
	RUN_TEST(test_sinterstore);
	RUN_TEST(test_sunion);
	RUN_TEST(test_sunionstore);
	RUN_TEST(test_sdiff);
	RUN_TEST(test_sdiffstore);
}
