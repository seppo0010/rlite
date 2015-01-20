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

int test_set() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "mykey", "mydata", "NX", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "mydata", "NX", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "mydata2", "XX", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "mydata3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_setnx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"setnx", "mykey", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"setnx", "mykey", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

int test_setex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"setex", "mykey", "1", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	sleep(1);

	{
		char* argv[100] = {"exists", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_psetex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"psetex", "mykey", "100", "mydata", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"exists", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	usleep(100 * 1000);

	{
		char* argv[100] = {"exists", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_get() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "myvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "myvalue", 7);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_append() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"append", "mykey", "myvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 7);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"append", "mykey", "appended", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 15);
		rliteFreeReplyObject(reply);
	}


	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "myvalueappended", 15);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_getset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"getset", "mykey", "val1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"getset", "mykey", "val2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "val1", 4);
		rliteFreeReplyObject(reply);
	}


	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "val2", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_mget() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "val1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "val2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"mget", "key1", "key2", "key3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 3);
		EXPECT_STR(reply->element[0], "val1", 4);
		EXPECT_STR(reply->element[1], "val2", 4);
		EXPECT_NIL(reply->element[2]);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lpush", "key3", "val3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"mget", "key1", "key2", "key3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 3);
		EXPECT_STR(reply->element[0], "val1", 4);
		EXPECT_STR(reply->element[1], "val2", 4);
		EXPECT_NIL(reply->element[2]);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_mset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"mset", "key2", "val2", "key1", "val1", "unexpected", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"mset", "key2", "val2", "key1", "val1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"mget", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 2);
		EXPECT_STR(reply->element[0], "val1", 4);
		EXPECT_STR(reply->element[1], "val2", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_msetnx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"msetnx", "key2", "val2", "key1", "val1", "unexpected", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"msetnx", "key2", "val2", "key1", "val1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"msetnx", "key2", "val2", "key3", "val3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"mget", "key1", "key2", "key3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_LEN(reply, 3);
		EXPECT_STR(reply->element[0], "val1", 4);
		EXPECT_STR(reply->element[1], "val2", 4);
		EXPECT_STR(reply->element[2], "val3", 4);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_getrange() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"getrange", "mykey", "0", "-1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "myvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"getrange", "mykey", "1", "-2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "yvalu", 5);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_setrange() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"setrange", "mykey", "0", "myvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 7);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"setrange", "mykey", "2", "newvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 10);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "mynewvalue", 10);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_strlen() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "mykey", "myvalue", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"strlen", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 7);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_incr() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"incr", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"incr", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"decr", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"incrby", "mykey", "1000", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1001);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"decrby", "mykey", "100", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 901);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "901", 3);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_incrbyfloat() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"incrbyfloat", "mykey", "1.2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "1.2", 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"incrbyfloat", "mykey", "2.3", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "3.5", 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "3.500000", 8);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"bitcount", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitcount", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 26);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitcount", "mykey", "0", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 4);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitcount", "mykey", "1", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitopand() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "abcdef", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "AND", "dest", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "dest", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "`bc`ab", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitopor() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "abcdef", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "OR", "dest", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "dest", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "goofev", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitopxor() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key2", "abcdef", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "XOR", "dest", "key1", "key2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "dest", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "\a\r\f\x0006\x0004\x0014", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitopnot() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "key1", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "NOT", "dest", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 6);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "dest", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "\x99\x90\x90\x9D\x9E\x8D", 6);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitopwrongtype() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"lpush", "key1", "foobar", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "NOT", "dest", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "AND", "dest", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "OR", "dest", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitop", "XOR", "dest", "key1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_ERROR(reply);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int test_bitpos() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"bitpos", "mykey", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, -1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "\xff\xf0\x00", NULL};
		populateArgvlen(argv, argvlen);
		argvlen[2] = 3;
		reply = rliteCommandArgv(context, 3, argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 12);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "\x00\xff\xf0", NULL};
		populateArgvlen(argv, argvlen);
		argvlen[2] = 3;
		reply = rliteCommandArgv(context, 3, argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"strlen", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 3);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "1", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 8);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "1", "2", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 16);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "\x00\xff\x00", NULL};
		populateArgvlen(argv, argvlen);
		argvlen[2] = 3;
		reply = rliteCommandArgv(context, 3, argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "0", "1", "-1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 16);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "mykey", "\x00\x00\x00", NULL};
		populateArgvlen(argv, argvlen);
		argvlen[2] = 3;
		reply = rliteCommandArgv(context, 3, argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"bitpos", "mykey", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, -1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_getbit() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"set", "mykey", "a", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"getbit", "mykey", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"getbit", "mykey", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"getbit", "mykey", "100", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_setbit() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"setbit", "mykey", "7", "1", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 0);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"setbit", "mykey", "7", "0", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_INTEGER(reply, 1);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"get", "mykey", NULL};
		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "\0", 1);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int run_string() {
	if (test_set() != 0) {
		return 1;
	}
	if (test_setnx() != 0) {
		return 1;
	}
	if (test_setex() != 0) {
		return 1;
	}
	if (test_psetex() != 0) {
		return 1;
	}
	if (test_get() != 0) {
		return 1;
	}
	if (test_append() != 0) {
		return 1;
	}
	if (test_getset() != 0) {
		return 1;
	}
	if (test_mget() != 0) {
		return 1;
	}
	if (test_mset() != 0) {
		return 1;
	}
	if (test_msetnx() != 0) {
		return 1;
	}
	if (test_getrange() != 0) {
		return 1;
	}
	if (test_setrange() != 0) {
		return 1;
	}
	if (test_strlen() != 0) {
		return 1;
	}
	if (test_incr() != 0) {
		return 1;
	}
	if (test_incrbyfloat() != 0) {
		return 1;
	}
	if (test_bitcount() != 0) {
		return 1;
	}
	if (test_bitopand() != 0) {
		return 1;
	}
	if (test_bitopor() != 0) {
		return 1;
	}
	if (test_bitopxor() != 0) {
		return 1;
	}
	if (test_bitopnot() != 0) {
		return 1;
	}
	if (test_bitopwrongtype() != 0) {
		return 1;
	}
	if (test_bitpos() != 0) {
		return 1;
	}
	if (test_getbit() != 0) {
		return 1;
	}
	if (test_setbit() != 0) {
		return 1;
	}
	return 0;
}
