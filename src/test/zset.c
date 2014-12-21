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
	argvlen[0] = 6;
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
	argv[0] = "ZREVRANGE";
	argvlen[0] = strlen(argv[0]);
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

int test_zrem() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZREM";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 3;
	argv[2] = "two";
	argvlen[3] = 3;
	argv[3] = "three";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 1) {
		fprintf(stderr, "Expected reply size to be %d, got %lu instead on line %d\n", 1, reply->elements, __LINE__);
		return 1;
	}
	if (reply->element[0]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply element[0] to be %d, got %d instead on line %d\n", RLITE_REPLY_STRING, reply->element[0]->type, __LINE__);
		return 1;
	}
	if (reply->element[0]->len != 3) {
		fprintf(stderr, "Expected reply element[0] length to be %d, got %d instead on line %d\n", 3, reply->element[0]->len, __LINE__);
		return 1;
	}

	if (memcmp(reply->element[0]->str, "one", 3) != 0) {
		fprintf(stderr, "Expected reply element[0] to be \"%s\", got \"%s\" instead on line %d\n", "one", reply->element[0]->str, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zremrangebyrank() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argv[0] = "ZREMRANGEBYRANK";
	argvlen[0] = strlen(argv[0]);
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 1;
	argv[3] = "3";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
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

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 0) {
		fprintf(stderr, "Expected reply size to be %d, got %lu instead on line %d\n", 0, reply->elements, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zremrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	size_t argvlen[4];
	char* argv[4];
	rliteReply* reply;
	argv[0] = "ZREMRANGEBYscore";
	argvlen[0] = strlen(argv[0]);
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 1;
	argv[3] = "3";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
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

	argvlen[0] = 6;
	argv[0] = "ZRANGE";
	argvlen[2] = 1;
	argv[2] = "0";
	argvlen[3] = 2;
	argv[3] = "-1";
	reply = rliteCommandArgv(context, 4, (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 0) {
		fprintf(stderr, "Expected reply size to be %d, got %lu instead on line %d\n", 0, reply->elements, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

int test_zcard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	if (_zadd(context) != 0) {
		return 1;
	}

	char* argv[100] = {"ZCARD", "mykey", NULL};
	size_t argvlen[100];
	rliteReply* reply;
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 2) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 2, reply->integer, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zinterstore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "key1", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZADD", "key2", "1", "one", "2", "two", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv3[100] = {"ZINTERSTORE", "out", "2", "key1", "key2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), (const char **)argv3, (const size_t*)argvlen);
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

	rliteFree(context);
	return 0;
}

int test_zunionstore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "key1", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZADD", "key2", "1", "one", "2", "two", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv3[100] = {"ZUNIONSTORE", "out", "2", "key1", "key2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), (const char **)argv3, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
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
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYSCORE", "mykey", "0", "2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply elements to be %d, got %lu instead on line %d\n", 2, reply->elements, __LINE__);
		return 1;
	}
	int i;
	char *expected[2] = {"one", "two"};
	for (i = 0; i < 2; i++) {
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrangebyscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "1", "one", "2", "two", "3", "three", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYSCORE", "mykey", "2", "0", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply elements to be %d, got %lu instead on line %d\n", 2, reply->elements, __LINE__);
		return 1;
	}
	int i;
	char *expected[2] = {"two", "one"};
	for (i = 0; i < 2; i++) {
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZRANGEBYLEX", "mykey", "-inf", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply elements to be %d, got %lu instead on line %d\n", 2, reply->elements, __LINE__);
		return 1;
	}
	int i;
	char *expected[2] = {"a", "b"};
	for (i = 0; i < 2; i++) {
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrangebylex() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZREVRANGEBYLEX", "mykey", "[b", "-inf", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_ARRAY) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->elements != 2) {
		fprintf(stderr, "Expected reply elements to be %d, got %lu instead on line %d\n", 2, reply->elements, __LINE__);
		return 1;
	}
	int i;
	char *expected[2] = {"b", "a"};
	for (i = 0; i < 2; i++) {
		if (memcmp(reply->element[i]->str, expected[i], reply->element[i]->len)) {
			fprintf(stderr, "Expected element[%d] to be %s, got %s instead on line %d\n", i, expected[i], reply->element[i]->str, __LINE__);
			return 1;
		}
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zlexcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"ZADD", "mykey", "0", "a", "0", "b", "0", "c", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	freeReplyObject(reply);

	char *argv2[100] = {"ZLEXCOUNT", "mykey", "-inf", "[b", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 2) {
		fprintf(stderr, "Expected reply elements to be %d, got %lld instead on line %d\n", 2, reply->integer, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zscore() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZSCORE", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->len != 1 || reply->str[0] != '1') {
		fprintf(stderr, "Expected reply string to be \"%s\", got \"%s\" instead on line %d\n", "1", reply->str, __LINE__);
		return 1;
	}
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zrevrank() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"ZREVRANK", "mykey", "one", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_zcount() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"Zcount", "mykey", "-inf", "inf", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
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

	rliteFree(context);
	return 0;
}

int test_exists() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"exists", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_del() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"del", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	char* argv2[100] = {"exists", "mykey", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), (const char **)argv2, (const size_t*)argvlen);
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
	freeReplyObject(reply);

	rliteFree(context);
	return 0;
}

int test_debug() {
	rliteContext *context = rliteConnect(":memory:", 0);

	if (_zadd(context) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"debug", "object", "mykey", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), (const char **)argv, (const size_t*)argvlen);
	if (reply->type == RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply not to be ERROR, got \"%s\" instead on line %d\n", reply->str, __LINE__);
		return 1;
	}
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
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
	if (test_zrem() != 0) {
		return 1;
	}
	if (test_zremrangebyrank() != 0) {
		return 1;
	}
	if (test_zremrangebyscore() != 0) {
		return 1;
	}
	if (test_zcard() != 0) {
		return 1;
	}
	if (test_zinterstore() != 0) {
		return 1;
	}
	if (test_zunionstore() != 0) {
		return 1;
	}
	if (test_zrangebyscore() != 0) {
		return 1;
	}
	if (test_zrevrangebyscore() != 0) {
		return 1;
	}
	if (test_zrangebylex() != 0) {
		return 1;
	}
	if (test_zrevrangebylex() != 0) {
		return 1;
	}
	if (test_zlexcount() != 0) {
		return 1;
	}
	if (test_zscore() != 0) {
		return 1;
	}
	if (test_zrank() != 0) {
		return 1;
	}
	if (test_zrevrank() != 0) {
		return 1;
	}
	if (test_zcount() != 0) {
		return 1;
	}
	if (test_exists() != 0) {
		return 1;
	}
	if (test_del() != 0) {
		return 1;
	}
	if (test_debug() != 0) {
		return 1;
	}
	return 0;
}
