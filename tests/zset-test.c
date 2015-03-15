#include <string.h>
#include "hirlite.h"
#include "util.h"

static int _zadd(rliteContext *context) {
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", NULL};
	size_t argvlen[100];
	rliteReply* reply;
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);

	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);
	PASS();
}
TEST test_zadd() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}
	rliteFree(context);
	PASS();
}

TEST test_zrange() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[5];
	char* argv[5];
	rliteReply* reply;
	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	argvlen[4] = 10;
	argv[4] = "WITHSCORES";
	reply = rliteCommandArgv(context, 5, argv, argvlen);
	EXPECT_REPLY_LEN(reply, 4);
	EXPECT_REPLY_STR(reply->element[0], "one", 3);
	EXPECT_REPLY_STR(reply->element[2], "two", 3);
	EXPECT_REPLY_STR(reply->element[1], "1", 1);
	EXPECT_REPLY_STR(reply->element[3], "2", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrevrange() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[5];
	char* argv[5];
	rliteReply* reply;
	argv[0] = "ZREVRANGE";
	argvlen[0] = strlen(argv[0]);
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	argvlen[4] = 10;
	argv[4] = "WITHSCORES";
	reply = rliteCommandArgv(context, 5, argv, argvlen);
	EXPECT_REPLY_LEN(reply, 4);
	EXPECT_REPLY_STR(reply->element[2], "one", 3);
	EXPECT_REPLY_STR(reply->element[0], "two", 3);
	EXPECT_REPLY_STR(reply->element[3], "1", 1);
	EXPECT_REPLY_STR(reply->element[1], "2", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrem() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZREM";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 3;
	argv[2] = "two";
	argvlen[3] = 3;
	argv[3] = "three";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_LEN(reply, 1);
	EXPECT_REPLY_STR(reply->element[0], "one", 3);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zremrangebyrank() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argv[0] = "ZREMRANGEBYRANK";
	argvlen[0] = strlen(argv[0]);
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 1;
	argv[3] = "3";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zremrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argv[0] = "ZREMRANGEBYscore";
	argvlen[0] = strlen(argv[0]);
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 1;
	argv[3] = "3";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_REPLY_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zcard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	char* argv[100] = {"ZCARD", "mykey", NULL};
	size_t argvlen[100];
	rliteReply* reply;
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zinterstore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "key1", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZADD", "key2", "1", "one", "2", "two", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"ZINTERSTORE", "out", "2", "key1", "key2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zunionstore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "key1", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZADD", "key2", "1", "one", "2", "two", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"ZUNIONSTORE", "out", "2", "key1", "key2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_INTEGER(reply, 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYSCORE", "mykey", "0", "2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	EXPECT_REPLY_STR(reply->element[0], "one", 3);
	EXPECT_REPLY_STR(reply->element[1], "two", 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrevrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYSCORE", "mykey", "2", "0", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	EXPECT_REPLY_STR(reply->element[0], "two", 3);
	EXPECT_REPLY_STR(reply->element[1], "one", 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYLEX", "mykey", "-", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	EXPECT_REPLY_STR(reply->element[0], "a", 1);
	EXPECT_REPLY_STR(reply->element[1], "b", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrevrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYLEX", "mykey", "[b", "-", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	EXPECT_REPLY_STR(reply->element[0], "b", 1);
	EXPECT_REPLY_STR(reply->element[1], "a", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zlexcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZLEXCOUNT", "mykey", "-", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZSCORE", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_STR(reply, "1", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zrevrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZREVRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_zcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"Zcount", "mykey", "-inf", "inf", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_exists() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"exists", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_del() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"del", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"exists", "mykey", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_debug() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"debug", "object", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

SUITE(zset_test) {
	RUN_TEST(test_zadd);
	RUN_TEST(test_zrange);
	RUN_TEST(test_zrevrange);
	RUN_TEST(test_zrem);
	RUN_TEST(test_zremrangebyrank);
	RUN_TEST(test_zremrangebyscore);
	RUN_TEST(test_zcard);
	RUN_TEST(test_zinterstore);
	RUN_TEST(test_zunionstore);
	RUN_TEST(test_zrangebyscore);
	RUN_TEST(test_zrevrangebyscore);
	RUN_TEST(test_zrangebylex);
	RUN_TEST(test_zrevrangebylex);
	RUN_TEST(test_zlexcount);
	RUN_TEST(test_zscore);
	RUN_TEST(test_zrank);
	RUN_TEST(test_zrevrank);
	RUN_TEST(test_zcount);
	RUN_TEST(test_exists);
	RUN_TEST(test_del);
	RUN_TEST(test_debug);
}
