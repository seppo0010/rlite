#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "rlite/hirlite.h"
#include "util.h"

static void lpush(rliteContext* context, char *key, char *element) {
	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"lpush", key, element, NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);
}

static int randomLpush(rliteContext* context, char *key, int elements) {
	rliteReply* reply;
	if (elements == 0) {
		elements = 3;
	}
	char* argv[100] = {"lpush", key, NULL};
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
	PASS();
}

TEST test_lpush() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"lpush", "mylist", "member1", "member2", "member3", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_lpushx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];
	{
		char* argv[100] = {"lpushx", key, "member1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}
	lpush(context, key, "member0");
	{
		char* argv[100] = {"lpushx", key, "member1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_rpush() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"rpush", "mylist", "member1", "member2", "member3", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 3);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_rpushx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];
	{
		char* argv[100] = {"rpushx", key, "member1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"rpush", key, "member0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"rpushx", key, "member1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_llen() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];
	int len = 50;
	if (randomLpush(context, key, len) != 0) {
		return 1;
	}
	{
		char* argv[100] = {"llen", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, len);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_lpop() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"lpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member0", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member1", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_rpop() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"rpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member1", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"rpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member0", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"rpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_lindex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"lindex", key, "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member0", 7);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lindex", key, "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member1", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lindex", key, "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lindex", key, "-2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member0", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lindex", key, "-1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "member1", 7);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"lindex", key, "-3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_linsert() {
	rliteContext *context = rliteConnect(":memory:", 0);

	long i;
	char *values[3] = {"value1", "value2", "othervalue"};
	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[1]);

	{
		char* argv[100] = {"linsert", key, "AFTER", values[1], values[2], NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"linsert", key, "BEFORE", values[1], values[0], NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	for (i = 0; i < 3; i++) {
		char* argv[100] = {"lpop", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, values[i], strlen(values[i]));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_lrange() {
	rliteContext *context = rliteConnect(":memory:", 0);

	long i;
	char *values[3] = {"value1", "value2", "othervalue"};
	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[2]);
	lpush(context, key, values[1]);
	lpush(context, key, values[0]);

	{
		char* argv[100] = {"lrange", key, "0", "-1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 3);
		for (i = 0; i < 3; i++) {
			EXPECT_REPLY_STR(reply->element[i], values[i], strlen(values[i]));
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrange", key, "1", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 1);
		EXPECT_REPLY_STR(reply->element[0], values[1], strlen(values[1]));
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrange", key, "1", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_LEN(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_lrem() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *values[2] = {"value1", "othervalue"};
	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[1]);
	lpush(context, key, values[0]);
	lpush(context, key, values[1]);
	lpush(context, key, values[0]);
	lpush(context, key, values[1]);
	lpush(context, key, values[0]);

	{
		char* argv[100] = {"lrem", key, "1", values[0], NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lindex", key, "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, values[1], strlen(values[1]));
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrem", key, "-2", values[1], NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"llen", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_lset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *values[3] = {"value1", "value2", "othervalue"};
	char *anothervalue = "yet other value";
	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[2]);
	lpush(context, key, values[1]);
	lpush(context, key, values[0]);

	{
		char* argv[100] = {"lset", key, "1", anothervalue, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lindex", key, "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, anothervalue, strlen(anothervalue));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_ltrim() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *values[3] = {"value1", "value2", "othervalue"};
	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[2]);
	lpush(context, key, values[1]);
	lpush(context, key, values[0]);

	{
		char* argv[100] = {"ltrim", key, "1", "-2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"llen", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lindex", key, "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, values[1], strlen(values[1]));
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

TEST test_rpoplpush() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *values[2] = {"value", "othervalue"};
	char *key = "mylist";
	char *key2 = "otherlist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, values[1]);
	lpush(context, key, values[0]);

	{
		char* argv[100] = {"rpoplpush", key, key2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, values[1], strlen(values[1]));
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"rpoplpush", key, key2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, values[0], strlen(values[0]));
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"rpoplpush", key, key2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"llen", key, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"llen", key2, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	PASS();
}

SUITE(hlist_test)
{
	RUN_TEST(test_lpush);
	RUN_TEST(test_lpushx);
	RUN_TEST(test_llen);
	RUN_TEST(test_lpop);
	RUN_TEST(test_rpop);
	RUN_TEST(test_lindex);
	RUN_TEST(test_linsert);
	RUN_TEST(test_lrange);
	RUN_TEST(test_lrem);
	RUN_TEST(test_lset);
	RUN_TEST(test_ltrim);
	RUN_TEST(test_rpoplpush);
	RUN_TEST(test_rpush);
	RUN_TEST(test_rpushx);
}
