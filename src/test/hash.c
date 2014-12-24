#include <string.h>
#include "hirlite.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

int test_hset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 0) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 0, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hget() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hget", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->len != 6 || memcmp(reply->str, "mydata", 6) != 0) {
		fprintf(stderr, "Expected reply to be \"mydata\", got \"%s\" (%d) instead on line %d\n", reply->str, reply->len, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int run_hash() {
	if (test_hset() != 0) {
		return 1;
	}
	if (test_hget() != 0) {
		return 1;
	}
	return 0;
}
