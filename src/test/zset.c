#include <string.h>
#include "hirlite.h"

static int _zadd(rliteContext *context) {
	size_t argvlen[6];
	char* argv[6];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZADD";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "1";
	argvlen[3] = 3;
	argv[3] = "one";
	argvlen[4] = 1;
	argv[4] = "2";
	argvlen[5] = 3;
	argv[5] = "two";
	reply = rliteCommandArgv(context, 6, (const char **)argv, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
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

	freeReplyObject(reply);
	return 0;
}
int test_zadd() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}
	rliteFree(context);
	return 0;
}

int test_zrange() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[5];
	char* argv[5];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZRANGE";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	argvlen[4] = 10;
	argv[4] = "WITHSCORES";
	reply = rliteCommandArgv(context, 5, (const char **)argv, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 4) {
		fprintf(stderr, "Expected reply size to be %d, got %lld instead on line %d\n", 2, reply->integer, __LINE__);
		return 1;
	}

	int i;
	for (i = 0; i < 4; i++) {
		if (reply->element[i]->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected element[%d] type to be %d, got %d instead on line %d\n", i, RLITE_REPLY_STRING, reply->element[i]->type, __LINE__);
			return 1;
		}
	}
	char *expected[4] = {"one", "1", "two", "2"};
	for (i = 0; i < 4; i++) {
		if ((size_t)reply->element[i]->len != strlen(expected[i])) {
			fprintf(stderr, "Expected element[%d] length to be %lu, got %d instead on line %d\n", i, strlen(expected[i]), reply->element[i]->len, __LINE__);
			return 1;
		}
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}

	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrange() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[5];
	char* argv[5];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZREVRANGE";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	argvlen[4] = 10;
	argv[4] = "WITHSCORES";
	reply = rliteCommandArgv(context, 5, (const char **)argv, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 4) {
		fprintf(stderr, "Expected reply size to be %d, got %lld instead on line %d\n", 2, reply->integer, __LINE__);
		return 1;
	}

	int i;
	for (i = 0; i < 4; i++) {
		if (reply->element[i]->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected element[%d] type to be %d, got %d instead on line %d\n", i, RLITE_REPLY_STRING, reply->element[i]->type, __LINE__);
			return 1;
		}
	}
	char *expected[4] = {"two", "2", "one", "1"};
	for (i = 0; i < 4; i++) {
		if ((size_t)reply->element[i]->len != strlen(expected[i])) {
			fprintf(stderr, "Expected element[%d] length to be %lu, got %d instead on line %d\n", i, strlen(expected[i]), reply->element[i]->len, __LINE__);
			return 1;
		}
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}

	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int run_zset() {
	if (test_zadd() != 0) {
		return 1;
	}
	if (test_zrange() != 0) {
		return 1;
	}
	if (test_zrevrange() != 0) {
		return 1;
	}
	return 0;
}
