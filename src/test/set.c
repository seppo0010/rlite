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

static int sadd(rliteContext* context, char *key, int elements) {
	rliteReply* reply;
	if (elements == 0) {
		elements = 3;
	}
	char* argv[100] = {"sadd", key, NULL};
	int i, j, len;
	for (i = 0; i < elements; i++) {
		len = 5 + floor(((float)rand() / RAND_MAX) * 10);
		argv[2 + i] = malloc(sizeof(char) * len);
		for (j = 0; j < len - 1; j++) {
			argv[2 + i][j] = 'a' + (int)floor(((float)rand() / RAND_MAX) * 25);
		}
		argv[2 + i][len] = 0;
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
	return 0;
}

static int test_sadd() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"sadd", "myset", "member1", "member2", "anothermember", "member1", NULL};
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

static int test_scard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	if (sadd(context, "myset", 5) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"scard", "myset", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 5) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 5, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	if (sadd(context, "myset", 3) != 0) {
		return 1;
	}

	char* argv2[100] = {"scard", "myset", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 8) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 8, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int run_set() {
	if (test_sadd() != 0) {
		return 1;
	}
	if (test_scard() != 0) {
		return 1;
	}
	return 0;
}
