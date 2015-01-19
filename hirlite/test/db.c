#include <string.h>
#include <unistd.h>
#include "hirlite.h"
#include "test/test.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

int test_keys() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "otherdata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"keys", "*", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 2);
		EXPECT_STR(reply->element[0], "key1", 4);
		EXPECT_STR(reply->element[1], "key2", 4);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"keys", "*1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 1);
		EXPECT_STR(reply->element[0], "key1", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_dbsize() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"dbsize", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "otherdata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"dbsize", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_expire(char *command, char *time) {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {command, "key1", time, NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_rename() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"rename", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mydata", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_renamenx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"renamenx", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mydata", 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key1", "mydata2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"renamenx", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mydata2", 7);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mydata", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_ttl_pttl() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"setex", "key1", "5", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"ttl", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		EXPECT_INTEGER_LTE(reply, 5);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"pttl", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER_LTE(reply, 5000);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_persist() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"persist", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"setex", "key1", "5", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"persist", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"persist", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"ttl", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, -1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_select() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"select", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"select", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mydata", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_move() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"move", "key1", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"move", "key2", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"move", "key1", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_type() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "str", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "str", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "string", 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lpush", "list", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "list", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "list", 4);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sadd", "set", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "set", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "set", 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"zadd", "zset", "0", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "zset", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "zset", 4);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"hset", "hash", "field", "value", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		NO_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "hash", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "hash", 4);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"type", "none", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "none", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int run_db() {
	if (test_keys() != 0) {
		return 1;
	}
	if (test_dbsize() != 0) {
		return 1;
	}
	if (test_expire("expire", "-1") != 0) {
		return 1;
	}
	if (test_expire("pexpire", "-1") != 0) {
		return 1;
	}
	if (test_expire("expireat", "1000") != 0) {
		return 1;
	}
	if (test_expire("pexpireat", "1000") != 0) {
		return 1;
	}
	if (test_rename() != 0) {
		return 1;
	}
	if (test_renamenx() != 0) {
		return 1;
	}
	if (test_ttl_pttl() != 0) {
		return 1;
	}
	if (test_persist() != 0) {
		return 1;
	}
	if (test_select() != 0) {
		return 1;
	}
	if (test_move() != 0) {
		return 1;
	}
	if (test_type() != 0) {
		return 1;
	}
	return 0;
}
