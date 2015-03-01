#include <arpa/inet.h>
#include "rlite.h"
#include "util.h"
#include "crc64.h"
#include "endianconv.h"

int rl_dump(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	uint64_t crc;
	unsigned char type;
	unsigned char *value = NULL, *value2 = NULL;
	unsigned char *buf = NULL;
	long buflen;
	long valuelen, value2len;
	unsigned char **values = NULL;
	long i = -1, *valueslen = NULL;
	uint32_t length;
	double score;
	char f[40];

	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, NULL, NULL, NULL);
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
	} else if (type == RL_TYPE_SET) {
		rl_set_iterator *iterator;
		RL_CALL(rl_smembers, RL_OK, db, &iterator, key, keylen);
		buflen = 16;
		length = 0;
		while ((retval = rl_set_iterator_next(iterator, NULL, &valuelen)) == RL_OK) {
			buflen += 5 + valuelen;
			length++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}

		RL_MALLOC(buf, sizeof(unsigned char) * buflen);
		buf[0] = REDIS_RDB_TYPE_SET;
		buf[1] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(length);
		memcpy(&buf[2], &length, 4);
		buflen = 6;

		RL_CALL(rl_smembers, RL_OK, db, &iterator, key, keylen);
		while ((retval = rl_set_iterator_next(iterator, &value, &valuelen)) == RL_OK) {
			buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
			length = htonl(valuelen);
			memcpy(&buf[buflen], &length, 4);
			buflen += 4;
			memcpy(&buf[buflen], value, valuelen);
			buflen += valuelen;
			rl_free(value);
			value = NULL;
		}
		if (retval != RL_END) {
			goto cleanup;
		}
	} else if (type == RL_TYPE_ZSET) {
		rl_zset_iterator *iterator;
		RL_CALL(rl_zrange, RL_OK, db, key, keylen, 0, -1, &iterator);
		buflen = 16;
		length = 0;
		while ((retval = rl_zset_iterator_next(iterator, &score, NULL, &valuelen)) == RL_OK) {
			buflen += 6 + valuelen + snprintf(f, 40, "%lf", score);
			length++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}

		RL_MALLOC(buf, sizeof(unsigned char) * buflen);
		buf[0] = REDIS_RDB_TYPE_ZSET;
		buf[1] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(length);
		memcpy(&buf[2], &length, 4);
		buflen = 6;

		RL_CALL(rl_zrange, RL_OK, db, key, keylen, 0, -1, &iterator);
		while ((retval = rl_zset_iterator_next(iterator, &score, &value, &valuelen)) == RL_OK) {
			buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
			length = htonl(valuelen);
			memcpy(&buf[buflen], &length, 4);
			buflen += 4;
			memcpy(&buf[buflen], value, valuelen);
			buflen += valuelen;
			rl_free(value);
			value = NULL;

			valuelen = snprintf(f, 40, "%lf", score);
			buf[buflen++] = valuelen;
			memcpy(&buf[buflen], f, valuelen);
			buflen += valuelen;
		}
		if (retval != RL_END) {
			goto cleanup;
		}
	} else if (type == RL_TYPE_HASH) {
		rl_hash_iterator *iterator;
		RL_CALL(rl_hgetall, RL_OK, db, &iterator, key, keylen);
		buflen = 16;
		length = 0;
		while ((retval = rl_hash_iterator_next(iterator, NULL, &value2len, NULL, &valuelen)) == RL_OK) {
			buflen += 10 + valuelen + value2len;
			length++;
		}
		if (retval != RL_END) {
			goto cleanup;
		}

		RL_MALLOC(buf, sizeof(unsigned char) * buflen);
		buf[0] = REDIS_RDB_TYPE_HASH;
		buf[1] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(length);
		memcpy(&buf[2], &length, 4);
		buflen = 6;

		RL_CALL(rl_hgetall, RL_OK, db, &iterator, key, keylen);
		while ((retval = rl_hash_iterator_next(iterator, &value, &valuelen, &value2, &value2len)) == RL_OK) {
			buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
			length = htonl(valuelen);
			memcpy(&buf[buflen], &length, 4);
			buflen += 4;
			memcpy(&buf[buflen], value, valuelen);
			buflen += valuelen;
			rl_free(value);
			value = NULL;

			buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
			length = htonl(value2len);
			memcpy(&buf[buflen], &length, 4);
			buflen += 4;
			memcpy(&buf[buflen], value2, value2len);
			buflen += value2len;
			rl_free(value2);
			value2 = NULL;
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
	rl_free(value2);
	return retval;
}
