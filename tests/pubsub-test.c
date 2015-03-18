#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "greatest.h"
#include "util.h"
#include "../src/rlite.h"
#include "../src/pubsub.h"

#define CHANNEL "mychannel"

struct buf {
	const char *data;
	size_t datalen;
};

static void* publish(void* _buffer) {
	sleep(1);
	struct buf *buffer = _buffer;

	rlite *db = NULL;
	if (setup_db(&db, 1, 0)) {
		fprintf(stderr, "Failed to open database\n");
		return NULL;
	}
	if (rl_publish(db, (unsigned char *)CHANNEL, strlen(CHANNEL), buffer->data, buffer->datalen)) {
		fprintf(stderr, "Failed to publish\n");
		return NULL;
	}
	return NULL;
}

TEST basic_subscribe_publish()
{
	int retval;
	static const char *data = "hello world!";
	size_t datalen = strlen(data);
	char *testdata = NULL;
	size_t testdatalen = 0;
	pthread_t thread;

	struct buf buffer;
	buffer.data = data;
	buffer.datalen = datalen;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	pthread_create(&thread, NULL, publish, &buffer);
	RL_CALL_VERBOSE(rl_subscribe, RL_OK, db, (unsigned char *)CHANNEL, strlen(CHANNEL), &testdata, &testdatalen);

	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	PASS();
}

SUITE(pubsub_test)
{
	RUN_TEST(basic_subscribe_publish);
}
