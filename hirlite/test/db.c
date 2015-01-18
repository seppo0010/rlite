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

int run_db() {
	if (test_keys() != 0) {
		return 1;
	}
	return 0;
}
