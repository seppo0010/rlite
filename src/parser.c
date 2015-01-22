#include "page_key.h"
#include "rlite.h"
#include "util.h"
#include "../deps/crc64.h"
#include "../deps/endianconv.h"

#define REDIS_RDB_VERSION 6

#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3

#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
#define REDIS_RDB_OPCODE_EXPIRETIME 253
#define REDIS_RDB_OPCODE_SELECTDB 254
#define REDIS_RDB_OPCODE_EOF 255

#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST 1
#define REDIS_RDB_TYPE_SET 2
#define REDIS_RDB_TYPE_ZSET 3
#define REDIS_RDB_TYPE_HASH 4
#define REDIS_RDB_TYPE_HASH_ZIPMAP 9
#define REDIS_RDB_TYPE_LIST_ZIPLIST 10
#define REDIS_RDB_TYPE_SET_INTSET 11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST 12
#define REDIS_RDB_TYPE_HASH_ZIPLIST 13

#define REDIS_RDB_ENC_INT8 0
#define REDIS_RDB_ENC_INT16 1
#define REDIS_RDB_ENC_INT32 2
#define REDIS_RDB_ENC_LZF 3

static int verify(unsigned char *data, long datalen) {
	unsigned char *footer;
	uint16_t rdbver;
	uint64_t crc;

	/* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
	if (datalen < 10) return RL_INVALID_PARAMETERS;
	footer = data+(datalen-10);

	/* Verify RDB version */
	rdbver = (footer[1] << 8) | footer[0];
	if (rdbver != REDIS_RDB_VERSION) return RL_INVALID_PARAMETERS;

	/* Verify CRC64 */
	crc = rl_crc64(0,data,datalen-8);
	memrev64ifbe(&crc);
	return (memcmp(&crc,footer+2,8) == 0) ? RL_OK : RL_INVALID_PARAMETERS;
}

static unsigned char *read_length_with_encoding(unsigned char *f, long *length, int *is_encoded)
{
	int enc_type = (f[0] & 0xC0) >> 6;
	if (enc_type == REDIS_RDB_ENCVAL) {
		if (is_encoded) {
			*is_encoded = 1;
		}
		*length = f[0] & 0x3F;
		return f + 1;
	} else if (enc_type == REDIS_RDB_6BITLEN) {
		if (is_encoded) {
			*is_encoded = 0;
		}
		*length = f[0] & 0x3F;
		return f + 1;
	} else if (enc_type == REDIS_RDB_14BITLEN) {
		if (is_encoded) {
			*is_encoded = 0;
		}
		*length = ((f[0] & 0x3F) << 8 ) | f[1];
		return f + 2;
	} else {
		if (is_encoded) {
			*is_encoded = 0;
		}
		*length = ntohl((uint32_t)f);
		return f + 5;
	}
}

int rl_restore(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, unsigned char *_data, long datalen)
{
	int retval;
	unsigned char type;
	long length;
	int is_encoded;
	unsigned char *strdata = NULL;
	unsigned char *data = _data;
	long strdatalen, cdatalen;

	RL_CALL(verify, RL_OK, data, datalen);
	RL_CALL(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	type = data[0];
	data++;

	if (type == REDIS_RDB_TYPE_STRING) {
		data = read_length_with_encoding(data, &length, &is_encoded);
		if (is_encoded && length == REDIS_RDB_ENC_INT8) {
			strdatalen = 5;
			strdata = rl_malloc(strdatalen * sizeof(unsigned char));
			strdatalen = snprintf((char *)strdata, strdatalen, "%d", (signed char)data[0]);
			if (strdatalen < 0) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		} else if (is_encoded && length == REDIS_RDB_ENC_INT16) {
			strdatalen = 7;
			strdata = rl_malloc(strdatalen * sizeof(unsigned char));
			strdatalen = snprintf((char *)strdata, strdatalen, "%d", (int16_t)(data[0] | (data[1] << 8)));
			if (strdatalen < 0) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		} else if (is_encoded && length == REDIS_RDB_ENC_INT32) {
			strdatalen = 12;
			strdata = rl_malloc(strdatalen * sizeof(unsigned char));
			strdatalen = snprintf((char *)strdata, strdatalen, "%d", (int32_t)(data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24)));
			if (strdatalen < 0) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		} else if (is_encoded && length == REDIS_RDB_ENC_LZF) {
			retval = RL_NOT_IMPLEMENTED;
			goto cleanup;
		} else if (!is_encoded) {
			strdatalen = length;
			strdata = rl_malloc(strdatalen * sizeof(unsigned char));
			memcpy(strdata, data, strdatalen);
		} else {
			retval = RL_NOT_IMPLEMENTED;
			goto cleanup;
		}
		RL_CALL(rl_set, RL_OK, db, key, keylen, strdata, strdatalen, 1, expires);
	}
cleanup:
	rl_free(strdata);
	return retval;
}
