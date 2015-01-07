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
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != elements) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", elements, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);
	for (i = 0; i < elements; i++) {
		free(argv[2 + i]);
	}
	return 0;
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

static int test_lpushx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];
	{
		char* argv[100] = {"lpushx", key, "member1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
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
	}
	lpush(context, key, "member0");
	{
		char* argv[100] = {"lpushx", key, "member1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 2) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 2, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_llen() {
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
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != len) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", len, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_lpop() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"lpop", key, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
			return 1;
		}
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->len != 7 || memcmp(reply->str, "member0", reply->len) != 0) {
			fprintf(stderr, "Expected reply to be \"member0\", got \"%s\" (%d) instead on line %d\n", reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lpop", key, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
			return 1;
		}
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->len != 7 || memcmp(reply->str, "member1", reply->len) != 0) {
			fprintf(stderr, "Expected reply to be \"member1\", got \"%s\" (%d) instead on line %d\n", reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lpop", key, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
			return 1;
		}
		if (reply->type != RLITE_REPLY_NIL) {
			fprintf(stderr, "Expected reply to be NIL, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

int run_list() {
	if (test_lpush() != 0) {
		return 1;
	}
	if (test_lpushx() != 0) {
		return 1;
	}
	if (test_llen() != 0) {
		return 1;
	}
	if (test_lpop() != 0) {
		return 1;
	}
	return 0;
}
