#include <string.h>
#include <unistd.h>
#include "rlite/hirlite.h"
#include "util.h"

TEST test_pubsub() {
	rliteContext *context = rliteConnect("rlite-test.rld", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"subscribe", "channel", "channel2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "subscribe", 9);
		EXPECT_REPLY_STR(reply->element[1], "channel", 7);
		EXPECT_REPLY_INTEGER(reply->element[2], 1);
		rliteFreeReplyObject(reply);

		rliteGetReply(context, (void **)&reply);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "subscribe", 9);
		EXPECT_REPLY_STR(reply->element[1], "channel2", 8);
		EXPECT_REPLY_INTEGER(reply->element[2], 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_pubsub_unsubscribe_all() {
	rliteContext *context = rliteConnect("rlite-test.rld", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"subscribe", "channel", "channel2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "subscribe", 9);
		EXPECT_REPLY_STR(reply->element[1], "channel", 7);
		EXPECT_REPLY_INTEGER(reply->element[2], 1);
		rliteFreeReplyObject(reply);

		rliteGetReply(context, (void **)&reply);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "subscribe", 9);
		EXPECT_REPLY_STR(reply->element[1], "channel2", 8);
		EXPECT_REPLY_INTEGER(reply->element[2], 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"unsubscribe", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "unsubscribe", 11);
		EXPECT_REPLY_STR(reply->element[1], "channel2", 8);
		EXPECT_REPLY_INTEGER(reply->element[2], 1);
		rliteFreeReplyObject(reply);

		rliteGetReply(context, (void **)&reply);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "unsubscribe", 11);
		EXPECT_REPLY_STR(reply->element[1], "channel", 7);
		EXPECT_REPLY_INTEGER(reply->element[2], 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_pubsub_punsubscribe_all() {
	rliteContext *context = rliteConnect("rlite-test.rld", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"psubscribe", "channel", "channel2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "psubscribe", 10);
		EXPECT_REPLY_STR(reply->element[1], "channel", 7);
		EXPECT_REPLY_INTEGER(reply->element[2], 1);
		rliteFreeReplyObject(reply);

		rliteGetReply(context, (void **)&reply);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "psubscribe", 10);
		EXPECT_REPLY_STR(reply->element[1], "channel2", 8);
		EXPECT_REPLY_INTEGER(reply->element[2], 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"punsubscribe", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "punsubscribe", 12);
		EXPECT_REPLY_STR(reply->element[1], "channel2", 8);
		EXPECT_REPLY_INTEGER(reply->element[2], 1);
		rliteFreeReplyObject(reply);

		rliteGetReply(context, (void **)&reply);
		EXPECT_REPLY_LEN(reply, 3);
		EXPECT_REPLY_STR(reply->element[0], "punsubscribe", 12);
		EXPECT_REPLY_STR(reply->element[1], "channel", 7);
		EXPECT_REPLY_INTEGER(reply->element[2], 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_pubsub_memory() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"subscribe", "channel", "channel2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

SUITE(hpubsub_test)
{
	RUN_TEST(test_pubsub);
	RUN_TEST(test_pubsub_memory);
	RUN_TEST(test_pubsub_unsubscribe_all);
	RUN_TEST(test_pubsub_punsubscribe_all);
}
