#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <unistd.h>
#include "rlite/rlite.h"
#include "rlite/util.h"
#include "rlite/crc64.h"
#include "rlite/endianconv.h"

// header is 4-bytes size and 8-bytes crc
#define FIFO_HEADER_SIZE 12

int rl_create_signal(const char *signal_name) {
	return mkfifo(signal_name, 0777) == 0 ? RL_OK : RL_UNEXPECTED;
}

int rl_delete_signal(const char *signal_name) {
	unlink(signal_name);
	return RL_OK;
}

int rl_read_signal(const char *signal_name, struct timeval *timeout, char **_data, size_t *_datalen)
{
	char header[FIFO_HEADER_SIZE];
	uint64_t crc;
	size_t readbytes;
	size_t datalen;
	int fd;
	int retval;
	char *data = NULL;
	fd_set rfds;
	int oflag = O_RDONLY;

	if (timeout) {
		// select will block, we don't want open to block
		oflag |= O_NONBLOCK;
	}

	fd = open(signal_name, oflag);
	if (fd == -1) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (timeout) {
		FD_ZERO(&rfds);
		FD_SET(fd, &rfds);
		retval = select(fd + 1, &rfds, NULL, NULL, timeout);
		if (retval == -1) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		} else if (retval != 0) {
			retval = RL_TIMEOUT;
			goto cleanup;
		}
	}

	readbytes = read(fd, header, FIFO_HEADER_SIZE);
	if (readbytes != FIFO_HEADER_SIZE) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	datalen = (size_t)get_4bytes((unsigned char *)header);
	RL_MALLOC(data, sizeof(char) * datalen);

	readbytes = read(fd, data, datalen);
	if (readbytes != datalen) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	crc = rl_crc64(0, (unsigned char *)data, datalen);
	memrev64ifbe(&crc);
	memcpy(&header[4], &crc, 8);
	if (memcmp(&crc, &header[4], 8) != 0) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	if (_data) {
		*_data = data;
	}
	if (_datalen) {
		*_datalen = datalen;
	}
	retval = RL_OK;
cleanup:
	if (fd >= 0) {
		close(fd);
	}
	if (retval != RL_OK || _data == NULL) {
		rl_free(data);
	}
	return retval;
}

int rl_write_signal(const char *signal_name, const char *data, size_t datalen) {
	char header[FIFO_HEADER_SIZE];
	put_4bytes((unsigned char *)header, datalen);

	uint64_t crc = rl_crc64(0, (unsigned char *)data, datalen);
	memrev64ifbe(&crc);
	memcpy(&header[4], &crc, 8);

	int fd = open(signal_name, O_WRONLY | O_NONBLOCK);
	if (fd == -1) {
		// fifo may not always exist on our code
		// it is a way to signal between processes, but it is fire and forget
		return RL_OK;
	}
	write(fd, header, FIFO_HEADER_SIZE);
	write(fd, data, datalen);
	close(fd);
	return RL_OK;
}
