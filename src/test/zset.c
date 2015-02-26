#include <string.h>
#include "hirlite.h"
#include "test/test_hirlite.h"

static int _zadd(rliteContext *context) {
	size_t argvlen[6];
	char* argv[6];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZADD";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "1";
	argvlen[3] = 3;
	argv[3] = "one";
	argvlen[4] = 1;
	argv[4] = "2";
	argvlen[5] = 3;
	argv[5] = "two";
	reply = rliteCommandArgv(context, 6, argv, argvlen);
	EXPECT_INTEGER(reply, 2);

	rliteFreeReplyObject(reply);
	return 0;
}
int test_zadd() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}
	rliteFree(context);
	return 0;
}

int test_zrange() {
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
	EXPECT_LEN(reply, 4);
	EXPECT_STR(reply->element[0], "one", 3);
	EXPECT_STR(reply->element[2], "two", 3);
	EXPECT_STR(reply->element[1], "1", 1);
	EXPECT_STR(reply->element[3], "2", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrange() {
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
	EXPECT_LEN(reply, 4);
	EXPECT_STR(reply->element[2], "one", 3);
	EXPECT_STR(reply->element[0], "two", 3);
	EXPECT_STR(reply->element[3], "1", 1);
	EXPECT_STR(reply->element[1], "2", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrem() {
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
	EXPECT_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_LEN(reply, 1);
	EXPECT_STR(reply->element[0], "one", 3);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zremrangebyrank() {
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
	EXPECT_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zremrangebyscore() {
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
	EXPECT_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, argv, argvlen);
	EXPECT_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

int test_zcard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	char* argv[100] = {"ZCARD", "mykey", NULL};
	size_t argvlen[100];
	rliteReply* reply;
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zinterstore() {
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
	EXPECT_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zunionstore() {
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
	EXPECT_INTEGER(reply, 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYSCORE", "mykey", "0", "2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_LEN(reply, 2);
	EXPECT_STR(reply->element[0], "one", 3);
	EXPECT_STR(reply->element[1], "two", 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYSCORE", "mykey", "2", "0", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_LEN(reply, 2);
	EXPECT_STR(reply->element[0], "two", 3);
	EXPECT_STR(reply->element[1], "one", 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYLEX", "mykey", "-", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_LEN(reply, 2);
	EXPECT_STR(reply->element[0], "a", 1);
	EXPECT_STR(reply->element[1], "b", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYLEX", "mykey", "[b", "-", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_LEN(reply, 2);
	EXPECT_STR(reply->element[0], "b", 1);
	EXPECT_STR(reply->element[1], "a", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zlexcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"ZLEXCOUNT", "mykey", "-", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_INTEGER(reply, 2);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZSCORE", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_STR(reply, "1", 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 0);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZREVRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"Zcount", "mykey", "-inf", "inf", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 2);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_exists() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"exists", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 1);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_del() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"del", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"exists", "mykey", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_debug() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"debug", "object", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	NO_ERROR(reply);

	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int run_zset() {
	if (test_zadd() != 0) {
		return 1;
	}
	if (test_zrange() != 0) {
		return 1;
	}
	if (test_zrevrange() != 0) {
		return 1;
	}
	if (test_zrem() != 0) {
		return 1;
	}
	if (test_zremrangebyrank() != 0) {
		return 1;
	}
	if (test_zremrangebyscore() != 0) {
		return 1;
	}
	if (test_zcard() != 0) {
		return 1;
	}
	if (test_zinterstore() != 0) {
		return 1;
	}
	if (test_zunionstore() != 0) {
		return 1;
	}
	if (test_zrangebyscore() != 0) {
		return 1;
	}
	if (test_zrevrangebyscore() != 0) {
		return 1;
	}
	if (test_zrangebylex() != 0) {
		return 1;
	}
	if (test_zrevrangebylex() != 0) {
		return 1;
	}
	if (test_zlexcount() != 0) {
		return 1;
	}
	if (test_zscore() != 0) {
		return 1;
	}
	if (test_zrank() != 0) {
		return 1;
	}
	if (test_zrevrank() != 0) {
		return 1;
	}
	if (test_zcount() != 0) {
		return 1;
	}
	if (test_exists() != 0) {
		return 1;
	}
	if (test_del() != 0) {
		return 1;
	}
	if (test_debug() != 0) {
		return 1;
	}
	return 0;
}
