#include "hirlite.h"

int main() {
	size_t argvlen[100];
	char* argv[100];
	rliteReply* reply;
	argvlen[0] = 4;
	argv[0] = "PING";
	rliteContext *context = rliteConnect(":memory:", 0);
	reply = rliteCommandArgv(context, 1, (const char **)argv, (const size_t*)argvlen);
	if (reply->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}
	if (memcmp(reply->str, "PONG", 4) != 0) {
		fprintf(stderr, "Expected reply to be %s, got %s instead on line %d\n", "PONG", reply->str, __LINE__);
		return 1;
	}

	freeReplyObject(reply);
	rliteFree(context);
	return 0;
}
