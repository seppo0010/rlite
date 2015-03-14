#include <string.h>
#include <unistd.h>
#include "hirlite.h"
#include "test_hirlite.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

int test_simple_concurrency() {
	const char *filepath = "rlite-test.rld";
	if (access(filepath, F_OK) == 0) {
		unlink(filepath);
	}
	rliteContext *context1 = rliteConnect(filepath, 0);
	rliteContext *context2 = rliteConnect(filepath, 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"GET", "key", NULL};
		reply = rliteCommandArgv(context1, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key", "value", NULL};
		reply = rliteCommandArgv(context2, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"GET", "key", NULL};
		reply = rliteCommandArgv(context1, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_STR(reply, "value", 5);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context1);
	rliteFree(context2);
	unlink(filepath);
	return 0;
}

int run_concurrency() {
	if (test_simple_concurrency() != 0) {
		return 1;
	}
	return 0;
}
