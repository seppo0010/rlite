#include <string.h>
#include "hirlite.h"

int test_ping() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, argv, argvlen);
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, "PONG", 4) != 0) {
		fprintf(stderr, "Expected reply to be %s, got %s instead on line %d\n", "PONG", reply->str, __LINE__);
		return 1;
	}

	rliteFreeReplyObject(reply);
	rliteFree(context);
	return 0;
}

int test_ping_str() {
	size_t argvlen[3];
	char* argv[3];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	argvlen[1] = 11;
	argv[1] = "hello world";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 2, argv, argvlen);
	if (reply->type != RLITE_REPLY_STATUS) {
		fprintf(stderr, "Expected reply to be STATUS, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, argv[1], argvlen[1]) != 0) {
		fprintf(stderr, "Expected reply to be %s, got %s instead on line %d\n", argv[1], reply->str, __LINE__);
		return 1;
	}

	rliteFreeReplyObject(reply);
	rliteFree(context);
	return 0;
}

int test_echo() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	argvlen[1] = 11;
	argv[1] = "hello world";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 2, argv, argvlen);
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, argv[1], argvlen[1]) != 0) {
		fprintf(stderr, "Expected reply to be %s, got %s instead on line %d\n", argv[1], reply->str, __LINE__);
		return 1;
	}

	rliteFreeReplyObject(reply);
	rliteFree(context);
	return 0;
}

int test_echo_wrong_arity() {
	size_t argvlen[2];
	char* argv[2];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "echo";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, argv, argvlen);
	if (reply->type != RLITE_REPLY_ERROR) {
		fprintf(stderr, "Expected reply to be ERROR, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	const char *err = "wrong number of arguments for 'echo' command";
	if (memcmp(reply->str, err, strlen(err)) != 0) {
		fprintf(stderr, "Expected reply to be \"%s\", got \"%s\" instead on line %d\n", err, reply->str, __LINE__);
		return 1;
	}

	rliteFreeReplyObject(reply);
	rliteFree(context);
	return 0;
}

int run_echo() {
	if (test_ping() != 0) {
		return 1;
	}
	if (test_ping_str() != 0) {
		return 1;
	}
	if (test_echo() != 0) {
		return 1;
	}
	if (test_echo_wrong_arity() != 0) {
		return 1;
	}
	return 0;
}
