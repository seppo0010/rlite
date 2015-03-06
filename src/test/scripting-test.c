// eval 'return {"1","2"}' 0'
// eval 'return 1' 0
#include <string.h>
#include <unistd.h>
#include "hirlite.h"
#include "test/test_hirlite.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

static int test_types()
{
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"eval", "return 1", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return '1'", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "1", 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return true", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return false", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return {err='bad'}", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR_STR(reply, "bad", 3);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return {ok='good'}", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "good", 4);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return {'ok','err'}", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 2);
		EXPECT_STR(reply->element[0], "ok", 2);
		EXPECT_STR(reply->element[1], "err", 3);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_call_ok()
{
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key", "value", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('get', 'key')", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "value", 5);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('get', KEYS[1])", "1", "key", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "value", 5);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('ping', ARGV[1])", "0", "value", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "value", 5);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('rpush', KEYS[1], ARGV[1], ARGV[2]) == 2", "1", "list", "1", "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('set', KEYS[1], ARGV[1])['ok'] == 'OK'", "1", "string", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval",
			"local items = redis.call('lrange', KEYS[1], '0', '-1')\n"
			"for i=1,#items do\n"
			"if items[i] ~= string.format('%d', i) then\n"
				"return {err=string.format('Element value is not what expected at position %d, got %s', i, items[i])}\n"
			"end\n"
			"if i > 2 then\n"
				"return {err='More than two elements'}\n"
			"end\n"
			"end\n"
			"return 0\n"
			"", "1", "list", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"type", "list", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "list", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_call_err()
{
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];
	{
		char* argv[100] = {"lpush", "list", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.call('get', KEYS[1])['err']", "1", "list", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return redis.pcall('get', KEYS[1])['err']", "1", "list", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "WRONGTYPE Operation against a key holding the wrong kind of value", 65);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"eval", "return {err='bad code'}", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR_STR(reply, "bad code", 8);
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

static int test_script_evalsha()
{
	rliteContext *context = rliteConnect(":memory:", 0);
	char *sha = "30dc9ef2a8d563dced9a243ecd0cc449c2ea0144";
	char *invalidSha = "30dc9ef2a8d563dced9a243ecd0cc449c2ea0145";

	rliteReply* reply;
	size_t argvlen[100];
	{
		char* argv[100] = {"script", "load", "return 123", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, sha, 40);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"evalsha", sha, "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 123);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"evalsha", invalidSha, "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

int run_scripting_test()
{
	if (test_types()) {
		return 1;
	}
	if (test_call_ok()) {
		return 1;
	}
	if (test_call_err()) {
		return 1;
	}
	if (test_script_evalsha()) {
		return 1;
	}
	return 0;
}
