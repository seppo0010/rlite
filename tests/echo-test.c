#include <string.h>
#include "rlite/hirlite.h"
#include "util.h"
#include "greatest.h"

#ifdef RL_DEBUG
TEST test_ping_oom() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	rliteContext *context = rliteConnect(":memory:", 0);
	int i;
	for (i = 1;;i++) {
		test_mode = 1;
		test_mode_counter = i;
		reply = rliteCommandArgv(context, 1, argv, argvlen);
		if (reply != NULL) {
			if (i == 1) {
				fprintf(stderr, "No OOM triggered\n");
				test_mode = 0;
				FAIL();
			}
			EXPECT_REPLY_STATUS(reply, "PONG", 4);
			break;
		}
	}

	test_mode = 0;
	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_echo_oom() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	argvlen[1] = 11;
	argv[1] = "hello world";
	rliteContext *context = rliteConnect(":memory:", 0);
	int i;
	for (i = 1;;i++) {
		test_mode = 1;
		test_mode_counter = i;
		reply = rliteCommandArgv(context, 2, argv, argvlen);
		if (reply != NULL) {
			if (i == 1) {
				fprintf(stderr, "No OOM triggered\n");
				test_mode = 0;
				FAIL();
			}
			EXPECT_REPLY_STR(reply, argv[1], argvlen[1]);
			break;
		}
	}

	test_mode = 0;
	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_echo_wrong_arity_oom() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	rliteContext *context = rliteConnect(":memory:", 0);
	int i;
	for (i = 1;;i++) {
		test_mode = 1;
		test_mode_counter = i;
		reply = rliteCommandArgv(context, 1, argv, argvlen);
		if (reply != NULL) {
			if (i == 1) {
				fprintf(stderr, "No OOM triggered\n");
				test_mode = 0;
				FAIL();
			}
			EXPECT_REPLY_ERROR(reply);
			const char *err = "wrong number of arguments for 'echo' command";
			ASSERT_EQ(memcmp(reply->str, err, strlen(err)), 0);
			break;
		}
	}

	test_mode = 0;
	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}
#endif

TEST test_ping() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, argv, argvlen);
	EXPECT_REPLY_STATUS(reply, "PONG", 4);

	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_ping_str() {
	size_t argvlen[3];
	char* argv[3];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	argvlen[1] = 11;
	argv[1] = "hello world";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 2, argv, argvlen);
	EXPECT_REPLY_STATUS(reply, argv[1], argvlen[1]);

	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_echo() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	argvlen[1] = 11;
	argv[1] = "hello world";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 2, argv, argvlen);
	EXPECT_REPLY_STR(reply, argv[1], argvlen[1]);

	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_echo_wrong_arity() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, argv, argvlen);
	EXPECT_REPLY_ERROR(reply);
	const char *err = "wrong number of arguments for 'echo' command";
	ASSERT_EQ(memcmp(reply->str, err, strlen(err)), 0);

	rliteFreeReplyObject(reply);
	rliteFree(context);
	PASS();
}

TEST test_not_null_terminated_long() {
	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argvlen[0] = 6;
	argv[0] = "lrange";
	argvlen[1] = 3;
	argv[1] = "key";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = malloc(sizeof(char) * 4);
	argv[3][0] = '-';
	argv[3][1] = '1';
	argv[3][2] = 'z';

	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_ARRAY);

	rliteFreeReplyObject(reply);
	rliteFree(context);
	free(argv[3]);
	PASS();
}

SUITE(echo_test)
{
#ifdef RL_DEBUG
	RUN_TEST(test_ping_oom);
	RUN_TEST(test_echo_oom);
	RUN_TEST(test_echo_wrong_arity_oom);
#endif
	RUN_TEST(test_ping);
	RUN_TEST(test_ping_str);
	RUN_TEST(test_echo);
	RUN_TEST(test_echo_wrong_arity);
	RUN_TEST(test_not_null_terminated_long);
}
