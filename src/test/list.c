#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "hirlite.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

static int test_lpush() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"lpush", "mylist", "member1", "member2", "member3", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 3) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 3, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int run_list() {
	if (test_lpush() != 0) {
		return 1;
	}
	return 0;
}
