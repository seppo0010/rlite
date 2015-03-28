#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "greatest.h"
#include "rlite.h"
#include "../src/rlite/signal.h"

#define FILEPATH "myfifo"

struct buf {
	const char *data;
	size_t datalen;
	int sleep;
};

static void delete_file() {
	if (access(FILEPATH, F_OK) == 0) {
		unlink(FILEPATH);
	}
}

static void* write_signal(void* _buffer) {
	struct buf *buffer = _buffer;
	if (buffer->sleep) {
		sleep(buffer->sleep);
	}
	rl_write_signal(FILEPATH, buffer->data, buffer->datalen);
	return NULL;
}

TEST basic_read_write()
{
	static const char *data = "hello world!";
	size_t datalen = strlen(data);
	char *testdata = NULL;
	size_t testdatalen = 0;
	pthread_t thread;

	struct buf buffer;
	buffer.data = data;
	buffer.datalen = datalen;
	buffer.sleep = 0;

	delete_file();
	rl_create_signal(FILEPATH);
	pthread_create(&thread, NULL, write_signal, &buffer);
	rl_read_signal(FILEPATH, NULL, &testdata, &testdatalen);
	unlink(FILEPATH);

	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	rl_free(testdata);
	PASS();
}

TEST basic_read_timeout()
{
	char *testdata = NULL;
	size_t testdatalen = 0;

	struct timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;

	delete_file();
	rl_create_signal(FILEPATH);
	rl_read_signal(FILEPATH, &timeout, &testdata, &testdatalen);

	ASSERT_EQ(testdata, NULL);
	PASS();
}

TEST basic_read_timeout_write()
{
	static const char *data = "hello world!";
	size_t datalen = strlen(data);
	char *testdata = NULL;
	size_t testdatalen = 0;
	pthread_t thread;

	struct buf buffer;
	buffer.data = data;
	buffer.datalen = datalen;
	buffer.sleep = 1;

	struct timeval timeout;
	timeout.tv_sec = 10;
	timeout.tv_usec = 0;

	delete_file();
	rl_create_signal(FILEPATH);
	pthread_create(&thread, NULL, write_signal, &buffer);
	rl_read_signal(FILEPATH, NULL, &testdata, &testdatalen);
	unlink(FILEPATH);

	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	rl_free(testdata);
	PASS();
}

SUITE(signal_test)
{
	RUN_TEST(basic_read_write);
	RUN_TEST(basic_read_timeout);
	RUN_TEST(basic_read_timeout_write);
}
