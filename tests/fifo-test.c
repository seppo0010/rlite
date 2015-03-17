#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "greatest.h"
#include "../src/fifo.h"

#define FILEPATH "myfifo"

struct buf {
	const char *data;
	size_t datalen;
};

static void delete_file() {
	if (access(FILEPATH, F_OK) == 0) {
		unlink(FILEPATH);
	}
}

static void* write_fifo(void* _buffer) {
	struct buf *buffer = _buffer;
	rl_write_fifo(FILEPATH, buffer->data, buffer->datalen);
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

	delete_file();
	rl_create_fifo(FILEPATH);
	pthread_create(&thread, NULL, write_fifo, &buffer);
	rl_read_fifo(FILEPATH, &testdata, &testdatalen);
	unlink(FILEPATH);

	ASSERT_EQ(datalen, testdatalen);
	ASSERT_EQ(memcmp(data, testdata, datalen), 0);
	PASS();
}

SUITE(fifo_test)
{
	RUN_TEST(basic_read_write);
}
