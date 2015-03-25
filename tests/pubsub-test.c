#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "greatest.h"
#include "util.h"
#include "../src/rlite.h"
#include "../src/pubsub.h"

#define DATA "hello world!"
#define CHANNEL "mychannel"
#define CHANNEL2 "other channel"

struct buf {
	const char *data;
	size_t datalen;
	const char *channel;
	size_t channellen;
	int read;
	long recipients;
	int sleep; // in seconds
	int timeout; // in seconds
};

static void init_buffer(struct buf* buffer, const char *data, size_t datalen, const char *channel, size_t channellen)
{
	if (data == NULL) {
		buffer->data = DATA;
		buffer->datalen = (size_t)strlen(DATA);
	} else {
		buffer->data = data;
		buffer->datalen = datalen;
	}
	if (channel == NULL) {
		buffer->channel = CHANNEL;
		buffer->channellen = (size_t)strlen(CHANNEL);
	} else {
		buffer->channel = channel;
		buffer->channellen = channellen;
	}
	buffer->read = 0;
	buffer->recipients = 0;
	buffer->sleep = 0;
	buffer->timeout = 0;
}

static void do_publish(rlite *db, struct buf *buffer) {
	if (rl_publish(db, UNSIGN(buffer->channel), buffer->channellen, buffer->data, buffer->datalen, &buffer->recipients)) {
		fprintf(stderr, "Failed to publish\n");
		return;
	}
	if (rl_commit(db)) {
		fprintf(stderr, "Failed to commit\n");
		return;
	}
}

static void* publish(void* _buffer) {
	struct buf *buffer = _buffer;

	if (buffer->sleep) {
		sleep(buffer->sleep);
	}

	rlite *db = NULL;
	if (setup_db(&db, 1, 0)) {
		fprintf(stderr, "Failed to open database\n");
		return NULL;
	}
	do_publish(db, buffer);
	rl_close(db);
	return NULL;
}

static void poll(rlite *db, struct buf *buffer) {
	int i, elementc;
	unsigned char **elements;
	long *elementslen;
	char *testdata = NULL, *testchannel = NULL;
	size_t testdatalen = 0, testchannellen = 0;
	if (buffer->timeout == 0) {
		while (rl_poll(db, &elementc, &elements, &elementslen) == RL_NOT_FOUND) {
			// need to discard to release lock on file
			rl_discard(db);
			sleep(1);
			rl_refresh(db);
		}
	} else {
		int retval;
		if (buffer->timeout == -1) {
			retval = rl_poll_wait(db, &elementc, &elements, &elementslen, NULL);
		} else {
			struct timeval timeout;
			timeout.tv_sec = buffer->timeout;
			timeout.tv_usec = 0;
			retval = rl_poll_wait(db, &elementc, &elements, &elementslen, &timeout);
		}
		if (retval != RL_OK) {
			return;
		}
	}

	testchannel = (char *)elements[elementc - 2];
	testchannellen = (size_t)elementslen[elementc - 2];
	testdata = (char *)elements[elementc - 1];
	testdatalen = (size_t)elementslen[elementc - 1];

	if (buffer->channellen == testchannellen && memcmp(buffer->channel, testchannel, testchannellen) == 0 &&
			buffer->datalen == testdatalen && memcmp(buffer->data, testdata, testdatalen) == 0) {
		buffer->read = 1;
	} else {
		fprintf(stderr, "Data mismatch on secondary subscriber");
	}
	for (i = 0; i < elementc; i++) {
		rl_free(elements[i]);
	}
	rl_free(elements);
	rl_free(elementslen);
}

/**
 * This function intentionally lives data around to test clean up
 */
static void subscribe_broken() {

	rlite *db = NULL;
	if (setup_db(&db, 1, 0)) {
		fprintf(stderr, "Failed to open database\n");
		return;
	}
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		return;
	}

	rl_free(db->subscriber_id);
	db->subscriber_id = NULL;
	fclose(db->subscriber_lock_fp);
	db->subscriber_lock_fp = NULL;
	rl_close(db);
}

static void* subscribe(void* _buffer) {
	struct buf *buffer = _buffer;

	rlite *db = NULL;
	if (setup_db(&db, 1, 0)) {
		fprintf(stderr, "Failed to open database\n");
		return NULL;
	}
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		return NULL;
	}

	poll(db, buffer);
	rl_close(db);

	return NULL;
}

TEST basic_subscribe_publish()
{
	int retval;
	size_t testdatalen = 0;
	long recipients;

	const char *channel = CHANNEL;
	long channellen = strlen(channel);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	do_publish(db, &buffer);
	rl_refresh(db);
	poll(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.read, 1);
	ASSERT_EQ(buffer.recipients, 1);
	PASS();
}

TEST basic_subscribe_publish_newdb()
{
	int retval;
	size_t testdatalen = 0;
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	rl_discard(db);
	publish(&buffer);
	rl_refresh(db);
	poll(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.read, 1);
	ASSERT_EQ(buffer.recipients, 1);
	PASS();
}

TEST basic_publish_no_subscriber()
{
	int retval;

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	rl_close(db);
	publish(&buffer);

	ASSERT_EQ(buffer.recipients, 0);

	PASS();
}

TEST basic_publish_two_subscribers()
{
	int retval;
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);

	pthread_t thread_w;
	struct buf buffer_w;
	init_buffer(&buffer_w, NULL, 0, NULL, 0);

	pthread_t thread_r;
	struct buf buffer_r;
	init_buffer(&buffer_r, NULL, 0, NULL, 0);

	pthread_t thread_r2;
	struct buf buffer_r2;
	init_buffer(&buffer_r2, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	rl_close(db);
	pthread_create(&thread_r, NULL, subscribe, &buffer_r);
	pthread_create(&thread_r2, NULL, subscribe, &buffer_r2);
	sleep(1); // need to make sure both subscribers are ready
	pthread_create(&thread_w, NULL, publish, &buffer_w);

	pthread_join(thread_w, NULL);
	pthread_join(thread_r, NULL);
	pthread_join(thread_r2, NULL);

	ASSERT_EQ(buffer_w.recipients, 2);
	ASSERT_EQ(buffer_r.read, 1);
	ASSERT_EQ(buffer_r2.read, 1);
	PASS();
}

TEST basic_subscribe2_publish(int publish_channel)
{
	int retval;
	static const char *data = "hello world!";
	size_t datalen = strlen(data);
	pthread_t thread;
	unsigned char *channels[2] = {UNSIGN(CHANNEL), UNSIGN(CHANNEL2)};
	long channelslen[2] = {strlen(CHANNEL), strlen(CHANNEL2)};

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, (char *)channels[publish_channel], channelslen[publish_channel]);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	pthread_create(&thread, NULL, publish, &buffer);
	RL_CALL_VERBOSE(rl_subscribe, RL_OK, db, 2, channels, channelslen);
	poll(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.read, 1);
	PASS();
}

TEST basic_subscribe_timeout_publish(int timeout)
{
	int retval;
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);

	pthread_t thread_w;
	struct buf buffer_w;
	init_buffer(&buffer_w, NULL, 0, NULL, 0);
	buffer_w.sleep = 1;

	struct buf buffer_r;
	init_buffer(&buffer_r, NULL, 0, NULL, 0);
	buffer_r.timeout = timeout;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);

	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	rl_discard(db);
	unsigned long long before = rl_mstime();
	pthread_create(&thread_w, NULL, publish, &buffer_w);
	rl_refresh(db);
	poll(db, &buffer_r);
	rl_close(db);

	unsigned long long elapsed = rl_mstime() - before;
	ASSERT(elapsed >= 1000);
	// if we waited more than 5 secs, we just timeouted, it should be close to 1s
	ASSERT(elapsed < 5000);
	ASSERT_EQ(buffer_r.read, 1);
	ASSERT_EQ(buffer_w.recipients, 1);
	PASS();
}

TEST basic_subscribe_timeout()
{
	int retval;
	char *channel = CHANNEL;
	long channellen = strlen(CHANNEL);

	struct buf buffer_r;
	init_buffer(&buffer_r, NULL, 0, NULL, 0);
	buffer_r.timeout = 1;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);

	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	rl_refresh(db);
	unsigned long long before = rl_mstime();
	poll(db, &buffer_r);
	rl_close(db);

	ASSERT_EQ(buffer_r.read, 0);
	PASS();
}

TEST basic_subscribe_unsubscribe_publish()
{
	int retval;
	size_t testdatalen = 0;

	const char *channel = CHANNEL;
	long channellen = strlen(channel);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	if (rl_unsubscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to unsubscribe\n");
		FAIL();
	}
	do_publish(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.recipients, 0);
	PASS();
}

TEST basic_subscribe_unsubscribe_all_publish()
{
	int retval;
	size_t testdatalen = 0;

	const char *channel = CHANNEL;
	long channellen = strlen(channel);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	if (rl_subscribe(db, 1, (unsigned char **)&channel, &channellen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	if (rl_unsubscribe_all(db)) {
		fprintf(stderr, "Failed to unsubscribe all\n");
		FAIL();
	}
	do_publish(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.recipients, 0);
	PASS();
}

TEST basic_publish_cleans_broken_subscriber()
{
	int retval;
	size_t testdatalen = 0;

	const char *channel = CHANNEL;
	long channellen = strlen(channel);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	rl_close(db);
	subscribe_broken();

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 0);
	do_publish(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.recipients, 0);
	PASS();
}

TEST basic_psubscribe_publish()
{
	int retval;
	size_t testdatalen = 0;
	long recipients;

	const char *pattern = "*";
	long patternlen = strlen(pattern);

	struct buf buffer;
	init_buffer(&buffer, NULL, 0, NULL, 0);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	if (rl_psubscribe(db, 1, (unsigned char **)&pattern, &patternlen)) {
		fprintf(stderr, "Failed to subscribe\n");
		FAIL();
	}
	do_publish(db, &buffer);
	rl_refresh(db);
	poll(db, &buffer);
	rl_close(db);

	ASSERT_EQ(buffer.read, 1);
	ASSERT_EQ(buffer.recipients, 1);
	PASS();
}

TEST basic_subscribe_pubsub_channels()
{
	int retval;
	size_t testdatalen = 0;
	long recipients;
	const char *channel = CHANNEL;
	long channellen = strlen(channel);
	const char *channel2 = CHANNEL2;
	long channel2len = strlen(channel2);

	long channelc;
	unsigned char **channelv = NULL;
	long *channelvlen = NULL;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 1, 1);
	RL_CALL_VERBOSE(rl_pubsub_channels, RL_OK, db, NULL, 0, &channelc, &channelv, &channelvlen);
	ASSERT_EQ(channelc, 0);

	RL_CALL_VERBOSE(rl_subscribe, RL_OK, db, 1, (unsigned char **)&channel, (long*)&channellen);

	RL_CALL_VERBOSE(rl_pubsub_channels, RL_OK, db, NULL, 0, &channelc, &channelv, &channelvlen);
	ASSERT_EQ(channelc, 1);
	ASSERT_EQ(channelvlen[0], channellen);
	ASSERT_EQ(memcmp(channelv[0], channel, channellen), 0);
	rl_free(channelv[0]);
	rl_free(channelvlen);
	rl_free(channelv);

	RL_CALL_VERBOSE(rl_subscribe, RL_OK, db, 1, (unsigned char **)&channel2, (long*)&channel2len);

	RL_CALL_VERBOSE(rl_pubsub_channels, RL_OK, db, NULL, 0, &channelc, &channelv, &channelvlen);
	ASSERT_EQ(channelc, 2);
	ASSERT_EQ(channelvlen[0], channellen);
	ASSERT_EQ(channelvlen[1], channel2len);
	ASSERT_EQ(memcmp(channelv[0], channel, channellen), 0);
	ASSERT_EQ(memcmp(channelv[1], channel2, channel2len), 0);
	rl_free(channelv[0]);
	rl_free(channelv[1]);
	rl_free(channelvlen);
	rl_free(channelv);

	RL_CALL_VERBOSE(rl_pubsub_channels, RL_OK, db, (unsigned char *)"other *", 7, &channelc, &channelv, &channelvlen);
	ASSERT_EQ(channelc, 1);
	ASSERT_EQ(channelvlen[0], channel2len);
	ASSERT_EQ(memcmp(channelv[0], channel2, channel2len), 0);
	rl_free(channelv[0]);
	rl_free(channelvlen);
	rl_free(channelv);

	rl_close(db);

	PASS();
}

SUITE(pubsub_test)
{
	RUN_TEST(basic_subscribe_publish);
	RUN_TEST(basic_subscribe_publish_newdb);
	RUN_TEST(basic_publish_two_subscribers);
	RUN_TEST(basic_publish_no_subscriber);
	RUN_TEST1(basic_subscribe2_publish, 0);
	RUN_TEST1(basic_subscribe2_publish, 1);
	RUN_TEST1(basic_subscribe_timeout_publish, 5);
	RUN_TEST1(basic_subscribe_timeout_publish, -1);
	RUN_TEST(basic_subscribe_timeout);
	RUN_TEST(basic_subscribe_unsubscribe_publish);
	RUN_TEST(basic_subscribe_unsubscribe_all_publish);
	RUN_TEST(basic_publish_cleans_broken_subscriber);
	RUN_TEST(basic_psubscribe_publish);
	RUN_TEST(basic_subscribe_pubsub_channels);
}
