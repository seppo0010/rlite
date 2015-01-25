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
	unsigned char **values = NULL;
	long i = -1, *valueslen = NULL;
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
	} else if (type == RL_TYPE_LIST) {
		RL_CALL(rl_lrange, RL_OK, db, key, keylen, 0, -1, &valuelen, &values, &valueslen);
		buflen = 16;
		for (i = 0; i < valuelen; i++) {
			buflen += 5 + valueslen[i];
		}
		RL_MALLOC(buf, sizeof(unsigned char) * buflen);
		buf[0] = REDIS_RDB_TYPE_LIST;
		buf[1] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(valuelen);
		memcpy(&buf[2], &length, 4);
		buflen = 6;
		for (i = 0; i < valuelen; i++) {
			buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
			length = htonl(valueslen[i]);
			memcpy(&buf[buflen], &length, 4);
			buflen += 4;
			memcpy(&buf[buflen], values[i], valueslen[i]);
			buflen += valueslen[i];
		}
	} else {
		retval = RL_UNEXPECTED;
		goto cleanup;
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
	if (values) {
		for (i = 0; i < valuelen; i++) {
			rl_free(values[i]);
		}
		rl_free(values);
	}
	rl_free(valueslen);
	rl_free(value);
	return retval;
}
