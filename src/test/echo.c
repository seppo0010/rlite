#include "hirlite.h"

int main() {
	size_t argvlen[100];
	char* argv[100];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (reply->integer != 1234) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1234, reply->integer, __LINE__);
		return 1;
	}

	freeReplyObject(reply);
	rliteFree(context);
	return 0;
}
