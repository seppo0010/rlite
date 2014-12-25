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

int test_hsetnx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hsetnx", "mykey", "myfield", "mydata", NULL};
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

	char* argv2[100] = {"hsetnx", "mykey", "myfield", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
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

	char *argv3[100] = {"hget", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
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

	char *argv3[100] = {"hget", "mykey", "non existing field", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_NIL) {
		fprintf(stderr, "Expected reply to be NIL, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hexists() {
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

	char *argv2[100] = {"hexists", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be 1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hexists", "mykey", "non existing field", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 0) {
		fprintf(stderr, "Expected reply to be 0, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hdel() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;

	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hset", "mykey", "myfield2", "mydata2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hdel", "mykey", "myfield", "nonexistentfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be 1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hexists", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 0) {
		fprintf(stderr, "Expected reply to be 0, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv5[100] = {"hexists", "mykey", "myfield2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be 1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hlen() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;

	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be 1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hset", "mykey", "myfield2", "mydata2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 2) {
		fprintf(stderr, "Expected reply to be 2, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv5[100] = {"hdel", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be 1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv6[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv6, argvlen), argv6, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hmset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 2) {
		fprintf(stderr, "Expected reply to be 2, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hmset", "mykey", "myfield3", "mydata3", "field2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hget", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
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

int test_hincrby() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hincrby", "mykey", "myfield", "123", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 123) {
		fprintf(stderr, "Expected reply to be 123, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hincrby", "mykey", "myfield", "345", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 468) {
		fprintf(stderr, "Expected reply to be 468, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hincrby", "mykey", "myfield", "not a number", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hset", "mykey", "myfield", "not a number", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv5[100] = {"hincrby", "mykey", "myfield", "1", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hincrbyfloat() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hincrbyfloat", "mykey", "myfield", "123.4", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, "123.4", 5) != 0) {
		fprintf(stderr, "Expected reply to be 123.4, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hincrbyfloat", "mykey", "myfield", "345.7", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, "469.1", 5)) {
		fprintf(stderr, "Expected reply to be 469.1, got %lld instead on line %d\n", reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hincrbyfloat", "mykey", "myfield", "not a number", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hset", "mykey", "myfield", "not a number", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv5[100] = {"hincrbyfloat", "mykey", "myfield", "1.2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hgetall() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hgetall", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 0) {
		fprintf(stderr, "Expected reply size to be 0, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hgetall", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 4) {
		fprintf(stderr, "Expected reply size to be 4, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING ||
			 reply->element[2]->type != RLITE_REPLY_STRING || reply->element[3]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d,%d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				reply->element[2]->type,
				reply->element[3]->type,
				__LINE__);
		return 1;
	}
	if (reply->element[2]->len != 7 || memcmp(reply->element[2]->str, "myfield", 7) != 0) {
		fprintf(stderr, "Expected reply->element[2] to be \"myfield\", got \"%s\" (%d) instead on line %d\n", reply->element[2]->str, reply->element[2]->len, __LINE__);
		return 1;
	}
	if (reply->element[3]->len != 6 || memcmp(reply->element[3]->str, "mydata", 6) != 0) {
		fprintf(stderr, "Expected reply->element[0] to be \"mydata\", got \"%s\" (%d) instead on line %d\n", reply->element[3]->str, reply->element[3]->len, __LINE__);
		return 1;
	}
	if (reply->element[0]->len != 8 || memcmp(reply->element[0]->str, "myfield2", 8) != 0) {
		fprintf(stderr, "Expected reply->element[0] to be \"myfield2\", got \"%s\" (%d) instead on line %d\n", reply->element[0]->str, reply->element[2]->len, __LINE__);
		return 1;
	}
	if (reply->element[1]->len != 7 || memcmp(reply->element[1]->str, "mydata2", 7) != 0) {
		fprintf(stderr, "Expected reply->element[0] to be \"mydata2\", got \"%s\" (%d) instead on line %d\n", reply->element[1]->str, reply->element[1]->len, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hkeys() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hkeys", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 0) {
		fprintf(stderr, "Expected reply size to be 0, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hkeys", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply size to be 2, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				__LINE__);
		return 1;
	}
	if (reply->element[1]->len != 7 || memcmp(reply->element[1]->str, "myfield", 7) != 0) {
		fprintf(stderr, "Expected reply->element[1] to be \"myfield\", got \"%s\" (%d) instead on line %d\n", reply->element[1]->str, reply->element[1]->len, __LINE__);
		return 1;
	}
	if (reply->element[0]->len != 8 || memcmp(reply->element[0]->str, "myfield2", 8) != 0) {
		fprintf(stderr, "Expected reply->element[0] to be \"myfield2\", got \"%s\" (%d) instead on line %d\n", reply->element[0]->str, reply->element[2]->len, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_hvals() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hvals", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 0) {
		fprintf(stderr, "Expected reply size to be 0, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hvals", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply size to be 2, got %lu instead on line %d\n", reply->elements, __LINE__);
		return 1;
	}
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				__LINE__);
		return 1;
	}
	if (reply->element[1]->len != 6 || memcmp(reply->element[1]->str, "mydata", 6) != 0) {
		fprintf(stderr, "Expected reply->element[1] to be \"mydata\", got \"%s\" (%d) instead on line %d\n", reply->element[1]->str, reply->element[1]->len, __LINE__);
		return 1;
	}
	if (reply->element[0]->len != 7 || memcmp(reply->element[0]->str, "mydata2", 7) != 0) {
		fprintf(stderr, "Expected reply->element[0] to be \"mydata2\", got \"%s\" (%d) instead on line %d\n", reply->element[0]->str, reply->element[2]->len, __LINE__);
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
	if (test_hexists() != 0) {
		return 1;
	}
	if (test_hdel() != 0) {
		return 1;
	}
	if (test_hlen() != 0) {
		return 1;
	}
	if (test_hmset() != 0) {
		return 1;
	}
	if (test_hincrby() != 0) {
		return 1;
	}
	if (test_hincrbyfloat() != 0) {
		return 1;
	}
	if (test_hgetall() != 0) {
		return 1;
	}
	if (test_hkeys() != 0) {
		return 1;
	}
	if (test_hvals() != 0) {
		return 1;
	}
	return 0;
}
