#include <arpa/inet.h>
#include "page_key.h"
#include "rlite.h"
#include "util.h"
#include "../deps/crc64.h"
#include "../deps/endianconv.h"
#include "../deps/lzf.h"

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

static unsigned char *read_signed_short(unsigned char *data, long *value) {
	*value = (char)data[0] | (char)(data[1] << 8);
	return data + 2;
}

static unsigned char *read_signed_int(unsigned char *data, long *value) {
	*value = (long)data[0] | (long)(data[1] << 8) | (long)(data[2] << 16) | (long)(data[3] << 24);
	return data + 4;
}

static unsigned char *read_signed_long(unsigned char *data, long *value) {
	long tmp;
	*value = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
	tmp = data[4];
	*value |= tmp << 32;
	tmp = data[5];
	*value |= tmp << 40;
	tmp = data[6];
	*value |= tmp << 48;
	tmp = data[7];
	*value |= tmp << 56;
	return data + 8;
}

static unsigned char *read_unsigned_short(unsigned char *data, unsigned long *value) {
	*value = data[0] | (data[1] << 8);
	return data + 2;
}

static unsigned char *read_unsigned_int(unsigned char *data, unsigned long *value) {
	*value = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
	return data + 4;
}

static unsigned char *read_unsigned_long(unsigned char *data, unsigned long *value) {
	unsigned long tmp;
	*value = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
	tmp = data[4];
	*value |= tmp << 32;
	tmp = data[5];
	*value |= tmp << 40;
	tmp = data[6];
	*value |= tmp << 48;
	tmp = data[7];
	*value |= tmp << 56;
	return data + 8;
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
		*length = ntohl(*(uint32_t*)(f + 1));
		return f + 5;
	}
}

static unsigned char *read_ziplist_entry(unsigned char *data, unsigned char **_entry, long *_length)
{
	long length = 0;
	unsigned char *entry;
	unsigned long prev_length = data[0];
	data++;
	if (prev_length == 254) {
		data = read_unsigned_int(data, &prev_length);
	}
	unsigned long entry_header = data[0];
	if ((entry_header >> 6) == 0) {
		length = entry_header & 0x3F;
		data++;
	}
	else if ((entry_header >> 6) == 1) {
		length = ((entry_header & 0x3F) << 8) | data[0];
		data++;
	}
	else if ((entry_header >> 6) == 2) {
		// TODO: length = read_big_endian_unsigned_int(f)
		data = NULL;
	} else if ((entry_header >> 4) == 12) {
		data = read_signed_short(data, &length);
	} else if ((entry_header >> 4) == 13) {
		data = read_signed_int(data, &length);
	} else if ((entry_header >> 4) == 14) {
		data = read_signed_long(data, &length);
	} else if (entry_header == 240) {
		unsigned char tmp[5];
		tmp[0] = tmp[4] = 0;
		memcpy(&tmp[1], data, 3);
		data = read_signed_int(data, &length);
	} else if (entry_header == 254) {
		length = data[0];
		data++;
	} else if (entry_header >= 241 && entry_header <= 253) {
		entry = rl_malloc(sizeof(unsigned char) * 2);
		if (!entry) {
			return NULL;
		}
		length = snprintf((char *)entry, 2, "%lu", entry_header - 241);
		data++;
		goto ret;
	}
	entry = rl_malloc(sizeof(unsigned char) * length);
	if (!entry) {
		return NULL;
	}
	memcpy(entry, data, length);
	data += length;
ret:
	*_entry = entry;
	*_length = length;
	return data;
}

static int read_string(unsigned char *data, unsigned char **str, long *strlen, unsigned char **newdata)
{
	int retval = RL_OK;
	long length, strdatalen, cdatalen;
	unsigned char *strdata;
	int is_encoded;
	data = read_length_with_encoding(data, &length, &is_encoded);
	if (is_encoded && length == REDIS_RDB_ENC_INT8) {
		strdatalen = 5;
		RL_MALLOC(strdata, strdatalen * sizeof(unsigned char));
		strdatalen = snprintf((char *)strdata, strdatalen, "%d", (signed char)data[0]);
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		data++;
	} else if (is_encoded && length == REDIS_RDB_ENC_INT16) {
		strdatalen = 7;
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		strdatalen = snprintf((char *)strdata, strdatalen, "%d", (int16_t)(data[0] | (data[1] << 8)));
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		data += 2;
	} else if (is_encoded && length == REDIS_RDB_ENC_INT32) {
		strdatalen = 12;
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		strdatalen = snprintf((char *)strdata, strdatalen, "%d", (int32_t)(data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24)));
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		data += 4;
	} else if (is_encoded && length == REDIS_RDB_ENC_LZF) {
		data = read_length_with_encoding(data, &cdatalen, NULL);
		data = read_length_with_encoding(data, &strdatalen, NULL);
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		rl_lzf_decompress(data, cdatalen, strdata, strdatalen);
		data += cdatalen;
	} else if (!is_encoded) {
		strdatalen = length;
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		memcpy(strdata, data, strdatalen);
		data += strdatalen;
	} else {
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	}
	*str = strdata;
	*strlen = strdatalen;
	if (newdata) {
		*newdata = data;
	}
cleanup:
	return retval;
}

int rl_restore(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, unsigned char *_data, long datalen)
{
	int retval;
	unsigned char type;
	long i, length, length2;
	unsigned char *strdata = NULL, *strdata2 = NULL, *strdata3 = NULL;
	unsigned char *data = _data, *tmpdata;
	long strdatalen = 0, strdata2len, strdata3len;
	unsigned long j, encoding, numentries, ulvalue;
	void **tmp = NULL;
	char f[40];
	double d;

	RL_CALL(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL(verify, RL_OK, data, datalen);

	type = data[0];
	data++;

	if (type == REDIS_RDB_TYPE_STRING) {
		RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, NULL);
		RL_CALL(rl_set, RL_OK, db, key, keylen, strdata, strdatalen, 1, expires);
	}
	else if (type == REDIS_RDB_TYPE_LIST) {
		data = read_length_with_encoding(data, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
			RL_CALL(rl_push, RL_OK, db, key, keylen, 1, 0, 1, &strdata, &strdatalen, NULL);
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_SET) {
		data = read_length_with_encoding(data, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
			RL_CALL(rl_sadd, RL_OK, db, key, keylen, 1, &strdata, &strdatalen, NULL);
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_ZSET) {
		data = read_length_with_encoding(data, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
			length2 = data[0];
			data++;
			if (length2 > 40 || length2 < 1) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			memcpy(f, data, length2);
			data += length2;
			f[length2] = 0;
			d = strtold(f, NULL);

			RL_CALL(rl_zadd, RL_OK, db, key, keylen, d, strdata, strdatalen);
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_HASH) {
		data = read_length_with_encoding(data, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
			RL_CALL(read_string, RL_OK, data, &strdata2, &length2, &data);

			RL_CALL(rl_hset, RL_OK, db, key, keylen, strdata, strdatalen, strdata2, length2, NULL, 0);
			rl_free(strdata);
			strdata = NULL;
			rl_free(strdata2);
			strdata2 = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_HASH_ZIPMAP) {
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	}
	else if (type == REDIS_RDB_TYPE_LIST_ZIPLIST) {
		RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
		tmpdata = strdata + 10;
		while (*tmpdata != 255) {
			tmpdata = read_ziplist_entry(tmpdata, &strdata2, &strdata2len);
			if (!tmpdata) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			RL_CALL(rl_push, RL_OK, db, key, keylen, 1, 0, 1, &strdata2, &strdata2len, NULL);
			rl_free(strdata2);
			strdata2 = NULL;
		}
		rl_free(strdata);
		strdata = NULL;
	}
	else if (type == REDIS_RDB_TYPE_SET_INTSET) {
		RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
		tmpdata = strdata;
		tmpdata = read_unsigned_int(tmpdata, &encoding);
		tmpdata = read_unsigned_int(tmpdata, &numentries);
		if (encoding != 2 && encoding != 4 && encoding != 8) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		RL_MALLOC(tmp, sizeof(void *));
		for (j = 0; j < numentries; j++) {
			if (encoding == 8) {
				tmpdata = read_unsigned_long(tmpdata, &ulvalue);
			} else if (encoding == 4) {
				tmpdata = read_unsigned_int(tmpdata, &ulvalue);
			} else if (encoding == 2) {
				tmpdata = read_unsigned_short(tmpdata, &ulvalue);
			}
			length2 = snprintf(f, 40, "%lu", ulvalue);
			tmp[0] = f;
			RL_CALL(rl_sadd, RL_OK, db, key, keylen, 1, (unsigned char **)tmp, &length2, NULL);
		}
	}
	else if (type == REDIS_RDB_TYPE_ZSET_ZIPLIST) {
		RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
		tmpdata = strdata + 10;
		while (*tmpdata != 255) {
			tmpdata = read_ziplist_entry(tmpdata, &strdata2, &strdata2len);
			if (!tmpdata) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			tmpdata = read_ziplist_entry(tmpdata, &strdata3, &strdata3len);
			if (!tmpdata) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}

			if (strdata3len > 40 || strdata3len < 1) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			memcpy(f, strdata3, strdata3len);
			f[strdata3len] = 0;
			d = strtold(f, NULL);
			RL_CALL(rl_zadd, RL_OK, db, key, keylen, d, strdata2, strdata2len);
			rl_free(strdata2);
			strdata2 = NULL;
			rl_free(strdata3);
			strdata3 = NULL;
		}
		rl_free(strdata);
		strdata = NULL;
	}
	else if (type == REDIS_RDB_TYPE_HASH_ZIPLIST) {
		RL_CALL(read_string, RL_OK, data, &strdata, &strdatalen, &data);
		tmpdata = strdata + 10;
		while (*tmpdata != 255) {
			tmpdata = read_ziplist_entry(tmpdata, &strdata2, &strdata2len);
			if (!tmpdata) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			tmpdata = read_ziplist_entry(tmpdata, &strdata3, &strdata3len);
			if (!tmpdata) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			RL_CALL(rl_hset, RL_OK, db, key, keylen, strdata2, strdata2len, strdata3, strdata3len, NULL, 0);
			rl_free(strdata2);
			strdata2 = NULL;
			rl_free(strdata3);
			strdata3 = NULL;
		}
		rl_free(strdata);
		strdata = NULL;
	} else {
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	rl_free(tmp);
	rl_free(strdata);
	rl_free(strdata2);
	rl_free(strdata3);
	return retval;
}
