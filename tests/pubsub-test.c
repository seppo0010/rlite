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
	int read;
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
	rl_close(db);
	return NULL;
}

static void* subscribe(void* _buffer) {
	struct buf *buffer = _buffer;
	char *testdata = NULL;
	size_t testdatalen = 0;

	rlite *db = NULL;
	if (setup_db(&db, 1, 0)) {
		fprintf(stderr, "Failed to open database\n");
		return NULL;
	}
	if (rl_subscribe(db, (unsigned char *)CHANNEL, strlen(CHANNEL), &testdata, &testdatalen)) {
		fprintf(stderr, "Failed to subscribe\n");
		return NULL;
	}
	if (buffer->datalen == testdatalen && memcmp(buffer->data, testdata, testdatalen) == 0) {
		buffer->read = 1;
	} else {
		fprintf(stderr, "Data mismatch on secondary subscriber");
	}
	rl_free(testdata);
	rl_close(db);
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
	rl_close(db);

	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	rl_free(testdata);
	PASS();
}

TEST basic_publish_two_subscribers()
{
	int retval;
	static const char *data = "hello world!";
	size_t datalen = strlen(data);
	char *testdata = NULL;
	size_t testdatalen = 0;
	pthread_t thread_w;
	pthread_t thread_r;

	struct buf buffer;
	buffer.data = data;
	buffer.datalen = datalen;
	buffer.read = 0;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	pthread_create(&thread_w, NULL, publish, &buffer);
	pthread_create(&thread_r, NULL, subscribe, &buffer);
	RL_CALL_VERBOSE(rl_subscribe, RL_OK, db, (unsigned char *)CHANNEL, strlen(CHANNEL), &testdata, &testdatalen);
	rl_close(db);

	ASSERT_EQ(buffer.read, 1);
	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	rl_free(testdata);
	PASS();
}

TEST basic_publish_no_subscriber()
{
	int retval;
	static const char *data = "hello world!";
	size_t datalen = strlen(data);

	struct buf buffer;
	buffer.data = data;
	buffer.datalen = datalen;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	rl_close(db);
	publish(&buffer);

	PASS();
}

SUITE(pubsub_test)
{
	RUN_TEST(basic_subscribe_publish);
	RUN_TEST(basic_publish_two_subscribers);
	RUN_TEST(basic_publish_no_subscriber);
}
