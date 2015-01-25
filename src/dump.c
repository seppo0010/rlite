#include "rlite.h"
#include "util.h"
#include "../deps/crc64.h"
#include "../deps/endianconv.h"

int rl_dump(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	uint64_t crc;
	unsigned char type;
	unsigned char *value = NULL;
	unsigned char *buf = NULL;
	long buflen;
	long valuelen;
	uint32_t length;
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, NULL, NULL);
	if (type == RL_TYPE_STRING) {
		RL_CALL(rl_get, RL_OK, db, key, keylen, &value, &valuelen);
		RL_MALLOC(buf, sizeof(unsigned char) * (16 + valuelen));
		buf[0] = REDIS_RDB_TYPE_STRING;
		buf[1] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(valuelen);
		memcpy(&buf[2], &length, 4);
		memcpy(&buf[6], value, valuelen);
		buflen = valuelen + 6;
	}
	buf[buflen++] = REDIS_RDB_VERSION;
	buf[buflen++] = REDIS_RDB_VERSION >> 8;

	crc = rl_crc64(0, buf, buflen);
	memrev64ifbe(&crc);
	memcpy(&buf[buflen], &crc, 8);
	buflen += 8;

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	rl_free(value);
	return retval;
}
