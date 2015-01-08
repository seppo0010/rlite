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

static int test_rpop() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"rpop", key, NULL};

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
		char* argv[100] = {"rpop", key, NULL};

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
		char* argv[100] = {"rpop", key, NULL};

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

static int test_lindex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	char *key = "mylist";

	rliteReply* reply;
	size_t argvlen[100];

	lpush(context, key, "member1");
	lpush(context, key, "member0");

	{
		char* argv[100] = {"lindex", key, "0", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
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
		char* argv[100] = {"lindex", key, "1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
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
		char* argv[100] = {"lindex", key, "2", NULL};

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

	{
		char* argv[100] = {"lindex", key, "-2", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
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
		char* argv[100] = {"lindex", key, "-1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
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
		char* argv[100] = {"lindex", key, "-3", NULL};

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

static int test_linsert() {
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
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
			return 1;
		}
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

	{
		char* argv[100] = {"linsert", key, "BEFORE", values[1], values[0], NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type == RLITE_REPLY_ERROR) {
			fprintf(stderr, "Expected reply not to be ERROR, got %s on line %d\n", reply->str, __LINE__);
			return 1;
		}
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 3) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 3, reply->integer, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	for (i = 0; i < 3; i++) {
		char* argv[100] = {"lpop", key, NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->len != (long)strlen(values[i]) || memcmp(reply->str, values[i], reply->len) != 0) {
			fprintf(stderr, "Expected reply %ld to be \"%s\", got \"%s\" (%d) instead on line %d\n", i, values[i], reply->str, reply->len, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_lrange() {
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
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->elements != 3) {
			fprintf(stderr, "Expected reply to be %d, got %lu instead on line %d\n", 3, reply->elements, __LINE__);
			return 1;
		}

		for (i = 0; i < 3; i++) {
			if (reply->element[i]->type != RLITE_REPLY_STRING) {
				fprintf(stderr, "Expected element %ld to be a string, got %d instead on line %d\n", i, reply->element[i]->type, __LINE__);
				return 1;
			}
			if (reply->element[i]->len != (long)strlen(values[i]) || memcmp(reply->element[i]->str, values[i], reply->element[i]->len) != 0) {
				fprintf(stderr, "Expected element %ld to be \"%s\", got \"%s\" (%d) instead on line %d\n", i, values[i], reply->element[i]->str, reply->element[i]->len, __LINE__);
				return 1;
			}
		}

		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrange", key, "1", "1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->elements != 1) {
			fprintf(stderr, "Expected reply to be %d, got %lu instead on line %d\n", 1, reply->elements, __LINE__);
			return 1;
		}

		if (reply->element[0]->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected element %ld to be a string, got %d instead on line %d\n", i, reply->element[0]->type, __LINE__);
			return 1;
		}
		if (reply->element[0]->len != (long)strlen(values[0]) || memcmp(reply->element[0]->str, values[1], reply->element[0]->len) != 0) {
			fprintf(stderr, "Expected element %ld to be \"%s\", got \"%s\" (%d) instead on line %d\n", i, values[1], reply->element[0]->str, reply->element[i]->len, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrange", key, "1", "0", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->elements != 0) {
			fprintf(stderr, "Expected reply to be %d, got %lu instead on line %d\n", 1, reply->elements, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_lrem() {
	rliteContext *context = rliteConnect(":memory:", 0);

	long i;
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
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 1) {
			fprintf(stderr, "Expected reply to be %d, got %lu instead on line %d\n", 1, reply->elements, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lindex", key, "0", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->len != (long)strlen(values[1]) || memcmp(reply->str, values[1], reply->len) != 0) {
			fprintf(stderr, "Expected reply to be \"%s\", got \"%s\" (%d) instead on line %d\n", values[1], reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"lrem", key, "-2", values[1], NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 2) {
			fprintf(stderr, "Expected reply to be %d, got %lu instead on line %d\n", 2, reply->elements, __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"llen", key, NULL};

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
	if (test_rpop() != 0) {
		return 1;
	}
	if (test_lindex() != 0) {
		return 1;
	}
	if (test_linsert() != 0) {
		return 1;
	}
	if (test_lrange() != 0) {
		return 1;
	}
	if (test_lrem() != 0) {
		return 1;
	}
	return 0;
}
