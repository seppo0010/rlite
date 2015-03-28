#include <string.h>
#include <unistd.h>
#include "rlite/hirlite.h"
#include "util.h"

TEST test_sort_alpha() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"sadd", "key", "31", "12", "41", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "12", 2);
		EXPECT_REPLY_STR(reply->element[1], "31", 2);
		EXPECT_REPLY_STR(reply->element[2], "41", 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_sort_numeric() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"rpush", "key", "b", "a", "c", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", "alpha", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "a", 1);
		EXPECT_REPLY_STR(reply->element[1], "b", 1);
		EXPECT_REPLY_STR(reply->element[2], "c", 1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_sortby_hash() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"hset", "wobj_a", "weight", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"hset", "wobj_b", "weight", "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"hset", "wobj_c", "weight", "3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"rpush", "key", "b", "a", "c", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", "by", "wobj_*->weight", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "a", 1);
		EXPECT_REPLY_STR(reply->element[1], "b", 1);
		EXPECT_REPLY_STR(reply->element[2], "c", 1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_sort_get_pound() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"mset", "weight_3", "30", "weight_1", "10", "weight_2", "20", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"rpush", "key", "3", "1", "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", "GET", "#", "GET", "weight_*", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 6);
		EXPECT_REPLY_STR(reply->element[0], "1", 1);
		EXPECT_REPLY_STR(reply->element[1], "10", 2);
		EXPECT_REPLY_STR(reply->element[2], "2", 1);
		EXPECT_REPLY_STR(reply->element[3], "20", 2);
		EXPECT_REPLY_STR(reply->element[4], "3", 1);
		EXPECT_REPLY_STR(reply->element[5], "30", 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_sort_get_pattern_get_pound() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"rpush", "key", "3", "1", "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", "GET", "#", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "1", 1);
		EXPECT_REPLY_STR(reply->element[1], "2", 1);
		EXPECT_REPLY_STR(reply->element[2], "3", 1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_sort_limit() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"rpush", "key", "2", "1", "3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sort", "key", "limit", "1", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], "2", 1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

SUITE(hsort_test)
{
	RUN_TEST(test_sort_alpha);
	RUN_TEST(test_sort_numeric);
	RUN_TEST(test_sortby_hash);
	RUN_TEST(test_sort_get_pound);
	RUN_TEST(test_sort_get_pattern_get_pound);
	RUN_TEST(test_sort_limit);
}
