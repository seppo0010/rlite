#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "greatest.h"
#include "hirlite.h"
#include "util.h"

#define FILEPATH "rlite-test.rld"
#define INCREMENT_LIMIT 1000

static void *increment(void *UNUSED(arg)) {
	rliteContext *context = rliteConnect(FILEPATH, 0);
	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"INCR", "key", NULL};
	int argc = populateArgvlen(argv, argvlen);
	long long val = 0;
	do {
		reply = rliteCommandArgv(context, argc, argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER || reply->integer < val) {
			fprintf(stderr, "Expected incremented value to be an integer greater than %lld, got %d (%lld) instead\n", val, reply->type, reply->integer);
			val = INCREMENT_LIMIT; // break after free
		} else {
			val = reply->integer;
		}
		rliteFreeReplyObject(reply);
	} while (val < INCREMENT_LIMIT);
	rliteFree(context);
	return NULL;
}

static void delete_file() {
	if (access(FILEPATH, F_OK) == 0) {
		unlink(FILEPATH);
	}
}

TEST simple_concurrency() {
	delete_file();
	rliteContext *context1 = rliteConnect(FILEPATH, 0);
	rliteContext *context2 = rliteConnect(FILEPATH, 0);

	rliteReply* reply;
	size_t argvlen[100];

	{
		char* argv[100] = {"GET", "key", NULL};
		reply = rliteCommandArgv(context1, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_NIL(reply);
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"set", "key", "value", NULL};
		reply = rliteCommandArgv(context2, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STATUS(reply, "OK", 2);
		rliteFreeReplyObject(reply);
	}
	{
		char* argv[100] = {"GET", "key", NULL};
		reply = rliteCommandArgv(context1, populateArgvlen(argv, argvlen), argv, argvlen);
		EXPECT_REPLY_STR(reply, "value", 5);
		rliteFreeReplyObject(reply);
	}

	rliteFree(context1);
	rliteFree(context2);
	unlink(FILEPATH);
	PASS();
}

TEST threads_concurrency() {
	delete_file();
	rliteContext *context = rliteConnect(FILEPATH, 0);

	pthread_t thread;
	pthread_create(&thread, NULL, increment, NULL);

	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"GET", "key", NULL};
	int argc = populateArgvlen(argv, argvlen);
	long long val;
	char tmp[40];

	do {
		reply = rliteCommandArgv(context, argc, argv, argvlen);
		if (reply->type == RLITE_REPLY_NIL) {
			val = 0;
		} else  if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected incremented value to be a string, got %d instead on line %d\n", reply->type, __LINE__);
			rliteFreeReplyObject(reply);
			break;
		} else {
			memcpy(tmp, reply->str, reply->len);
			tmp[reply->len] = 0;
			val = strtoll(tmp, NULL, 10);
		}
		rliteFreeReplyObject(reply);
	} while (val < INCREMENT_LIMIT);

	rliteFree(context);
	PASS();
}

TEST multiple_writing_threads_concurrency() {
	delete_file();
	rliteContext *context = rliteConnect(FILEPATH, 0);

	pthread_t thread1;
	pthread_t thread2;
	pthread_create(&thread1, NULL, increment, NULL);
	pthread_create(&thread2, NULL, increment, NULL);

	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"GET", "key", NULL};
	int argc = populateArgvlen(argv, argvlen);
	long long val;
	char tmp[40];

	do {
		reply = rliteCommandArgv(context, argc, argv, argvlen);
		if (reply->type == RLITE_REPLY_NIL) {
			val = 0;
		} else  if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected incremented value to be a string, got %d instead on line %d\n", reply->type, __LINE__);
			rliteFreeReplyObject(reply);
			break;
		} else {
			memcpy(tmp, reply->str, reply->len);
			tmp[reply->len] = 0;
			val = strtoll(tmp, NULL, 10);
		}
		rliteFreeReplyObject(reply);
	} while (val < INCREMENT_LIMIT);

	rliteFree(context);
	PASS();
}

SUITE(concurrency_test) {
	RUN_TEST(simple_concurrency);
	RUN_TEST(threads_concurrency);
	RUN_TEST(multiple_writing_threads_concurrency);
}
