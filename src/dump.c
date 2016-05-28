#include <arpa/inet.h>
#include "rlite/rlite.h"
#include "rlite/util.h"
#include "rlite/crc64.h"
#include "rlite/endianconv.h"

static int rl_dump_string(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	long valuelen;
	unsigned char *buf = NULL;
	long buflen;
	uint32_t length;

	RL_CALL(rl_get, RL_OK, db, key, keylen, NULL, &valuelen);
	RL_MALLOC(buf, sizeof(unsigned char) * (16 + valuelen));
	buf[0] = REDIS_RDB_TYPE_STRING;
	buf[1] = (REDIS_RDB_32BITLEN << 6);
	length = htonl(valuelen);
	memcpy(&buf[2], &length, 4);
	RL_CALL(rl_get_cpy, RL_OK, db, key, keylen, &buf[6], NULL);
	buflen = valuelen + 6;

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(buf);
	}
	return retval;
}

static int rl_dump_list(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	unsigned char *buf = NULL;
	long buflen;
	uint32_t length;
	long valuelen = 0;
	rl_list_iterator *iterator = NULL;
	void *tmp;
	long page, size;

	RL_CALL(rl_lrange_iterator, RL_OK, db, key, keylen, 0, -1, &size, &iterator);
	buflen = 16;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		page = *(long *)tmp;
		rl_free(tmp);
		RL_CALL(rl_multi_string_get, RL_OK, db, page, NULL, &valuelen);
		buflen += 5 + valuelen;
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_MALLOC(buf, sizeof(unsigned char) * buflen);
	buf[0] = REDIS_RDB_TYPE_LIST;
	buf[1] = (REDIS_RDB_32BITLEN << 6);
	length = htonl(size);
	memcpy(&buf[2], &length, 4);
	buflen = 6;

	RL_CALL(rl_lrange_iterator, RL_OK, db, key, keylen, 0, -1, &size, &iterator);
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		page = *(long *)tmp;
		rl_free(tmp);
		buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
		RL_CALL(rl_multi_string_get, RL_OK, db, page, NULL, &valuelen);
		length = htonl(valuelen);
		memcpy(&buf[buflen], &length, 4);
		buflen += 4;
		RL_CALL(rl_multi_string_cpy, RL_OK, db, page, &buf[buflen], NULL);
		buflen += valuelen;
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_list_iterator_destroy(db, iterator);
		}
		rl_free(buf);
	}
	return retval;
}

static int rl_dump_set(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	long valuelen;
	unsigned char *buf = NULL;
	long buflen;
	long page;
	uint32_t length;

	rl_set_iterator *iterator = NULL;
	RL_CALL(rl_smembers, RL_OK, db, &iterator, key, keylen);
	buflen = 16;
	length = 0;
	while ((retval = rl_set_iterator_next(iterator, NULL, NULL, &valuelen)) == RL_OK) {
		buflen += 5 + valuelen;
		length++;
	}
	iterator = NULL;
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
	while ((retval = rl_set_iterator_next(iterator, &page, NULL, &valuelen)) == RL_OK) {
		buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(valuelen);
		memcpy(&buf[buflen], &length, 4);
		buflen += 4;
		RL_CALL(rl_multi_string_cpy, RL_OK, db, page, &buf[buflen], NULL);
		buflen += valuelen;
	}
	iterator = NULL;
	if (retval != RL_END) {
		goto cleanup;
	}

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_set_iterator_destroy(iterator);
		}
		rl_free(buf);
	}
	return retval;
}

static int rl_dump_zset(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	long valuelen;
	unsigned char *buf = NULL;
	long buflen, page;
	uint32_t length;
	double score;
	char f[40];

	rl_zset_iterator *iterator = NULL;
	RL_CALL(rl_zrange, RL_OK, db, key, keylen, 0, -1, &iterator);
	buflen = 16;
	length = 0;
	while ((retval = rl_zset_iterator_next(iterator, NULL, &score, NULL, &valuelen)) == RL_OK) {
		buflen += 6 + valuelen + snprintf(f, 40, "%lf", score);
		length++;
	}
	iterator = NULL;
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
	while ((retval = rl_zset_iterator_next(iterator, &page, &score, NULL, &valuelen)) == RL_OK) {
		buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(valuelen);
		memcpy(&buf[buflen], &length, 4);
		buflen += 4;
		RL_CALL(rl_multi_string_cpy, RL_OK, db, page, &buf[buflen], NULL);
		buflen += valuelen;

		valuelen = snprintf(f, 40, "%lf", score);
		buf[buflen++] = valuelen;
		memcpy(&buf[buflen], f, valuelen);
		buflen += valuelen;
	}
	iterator = NULL;
	if (retval != RL_END) {
		goto cleanup;
	}

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_zset_iterator_destroy(iterator);
		}
		rl_free(buf);
	}
	return retval;
}

static int rl_dump_hash(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	long valuelen, value2len;
	unsigned char *buf = NULL;
	long buflen;
	uint32_t length;
	long valuepage, value2page;

	rl_hash_iterator *iterator = NULL;
	RL_CALL(rl_hgetall, RL_OK, db, &iterator, key, keylen);
	buflen = 16;
	length = 0;
	while ((retval = rl_hash_iterator_next(iterator, NULL, NULL, &value2len, NULL, NULL, &valuelen)) == RL_OK) {
		buflen += 10 + valuelen + value2len;
		length++;
	}
	iterator = NULL;
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
	while ((retval = rl_hash_iterator_next(iterator, &valuepage, NULL, &valuelen, &value2page, NULL, &value2len)) == RL_OK) {
		buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(valuelen);
		memcpy(&buf[buflen], &length, 4);
		buflen += 4;
		RL_CALL(rl_multi_string_cpy, RL_OK, db, valuepage, &buf[buflen], NULL);
		buflen += valuelen;

		buf[buflen++] = (REDIS_RDB_32BITLEN << 6);
		length = htonl(value2len);
		memcpy(&buf[buflen], &length, 4);
		buflen += 4;
		RL_CALL(rl_multi_string_cpy, RL_OK, db, value2page, &buf[buflen], NULL);
		buflen += value2len;
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	*data = buf;
	*datalen = buflen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_hash_iterator_destroy(iterator);
		}
		rl_free(buf);
	}
	return retval;
}

int rl_dump(struct rlite *db, const unsigned char *key, long keylen, unsigned char **data, long *datalen)
{
	int retval;
	uint64_t crc;
	unsigned char type;
	unsigned char *buf = NULL;
	long buflen;

	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, NULL, NULL, NULL);
	if (type == RL_TYPE_STRING) {
		RL_CALL(rl_dump_string, RL_OK, db, key, keylen, &buf, &buflen);
	} else if (type == RL_TYPE_LIST) {
		RL_CALL(rl_dump_list, RL_OK, db, key, keylen, &buf, &buflen);
	} else if (type == RL_TYPE_SET) {
		RL_CALL(rl_dump_set, RL_OK, db, key, keylen, &buf, &buflen);
	} else if (type == RL_TYPE_ZSET) {
		RL_CALL(rl_dump_zset, RL_OK, db, key, keylen, &buf, &buflen);
	} else if (type == RL_TYPE_HASH) {
		RL_CALL(rl_dump_hash, RL_OK, db, key, keylen, &buf, &buflen);
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
	return retval;
}
