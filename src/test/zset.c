#include <string.h>
#include "hirlite.h"

int test_zadd() {
	size_t argvlen[6];
	char* argv[6];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "ZADD";
	argvlen[1] = 5;
	argv[1] = "mykey";
	argvlen[2] = 1;
	argv[2] = "1";
	argvlen[3] = 1;
	argv[3] = "one";
	argvlen[4] = 1;
	argv[4] = "2";
	argvlen[5] = 3;
	argv[5] = "two";
	rliteContext *context = rliteConnect(":memory:", 0);
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
	rliteFree(context);
	return 0;
}


int run_zset() {
	if (test_zadd() != 0) {
		return 1;
	}
	return 0;
}
