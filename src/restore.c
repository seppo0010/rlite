#include <arpa/inet.h>
#include "page_key.h"
#include "rlite.h"
#include "util.h"
#include "crc64.h"
#include "endianconv.h"
#include "lzf.h"

struct stringwithlength {
	unsigned char *string;
	long stringlen;
};

int ucread(struct rl_restore_streamer *streamer, unsigned char *str, long len) {
	struct stringwithlength *data = streamer->context;
	if (len > data->stringlen) {
		return RL_UNEXPECTED;
	}
	if (str) {
		memcpy(str, data->string, len);
	}
	data->string += len;
	data->stringlen -= len;
	return RL_OK;
}

static rl_restore_streamer* init_string_streamer(unsigned char *data, long datalen)
{
	rl_restore_streamer *streamer = rl_malloc(sizeof(*streamer));
	if (!streamer) {
		return NULL;
	}
	struct stringwithlength *s = rl_malloc(sizeof(*s));
	if (!s) {
		rl_free(streamer);
		return NULL;
	}
	s->string = data;
	s->stringlen = datalen;
	streamer->context = s;
	streamer->read = &ucread;
	return streamer;
}

static void free_string_streamer(rl_restore_streamer *streamer) {
	rl_free(streamer->context);
	rl_free(streamer);
}

static int read(rl_restore_streamer *streamer, unsigned char *target, long len) {
	return streamer->read(streamer, target, len);
}

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

static int read_signed_int(unsigned char *data, long *value) {
	*value = (long)data[0] | (long)(data[1] << 8) | (long)(data[2] << 16) | (long)(data[3] << 24);
	return RL_OK;
}

static int read_signed_long(unsigned char *data, long *value) {
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
	return RL_OK;
}

static int read_unsigned_short(rl_restore_streamer *streamer, unsigned long *value) {
	int retval;
	unsigned char ucint;
	long val;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val = ucint;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 8;
	*value = val;
cleanup:
	return retval;
}

static int read_unsigned_int(rl_restore_streamer *streamer, unsigned long *value) {
	int retval;
	unsigned char ucint;
	long val;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val = ucint;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 8;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 16;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 24;
	*value = val;
cleanup:
	return retval;
}

static int read_unsigned_long(rl_restore_streamer *streamer, unsigned long *value) {
	int retval;
	unsigned char ucint;
	unsigned long val;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val = ucint;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 8;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 16;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= ucint << 24;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= (unsigned long)ucint << 32;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= (unsigned long)ucint << 40;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= (unsigned long)ucint << 48;
	RL_CALL(read, RL_OK, streamer, &ucint, 1);
	val |= (unsigned long)ucint << 56;
	*value = val;
cleanup:
	return retval;
}

static int read_length_with_encoding(rl_restore_streamer *streamer, long *length, int *is_encoded)
{
	int retval;
	unsigned char f;
	unsigned char f4[4];
	RL_CALL(read, RL_OK, streamer, &f, 1);
	int enc_type = (f & 0xC0) >> 6;
	if (enc_type == REDIS_RDB_ENCVAL) {
		if (is_encoded) {
			*is_encoded = 1;
		}
		*length = f & 0x3F;
		return RL_OK;
	} else if (enc_type == REDIS_RDB_6BITLEN) {
		if (is_encoded) {
			*is_encoded = 0;
		}
		*length = f & 0x3F;
		return RL_OK;
	} else if (enc_type == REDIS_RDB_14BITLEN) {
		if (is_encoded) {
			*is_encoded = 0;
		}
		*length = ((f & 0x3F) << 8 );
		RL_CALL(read, RL_OK, streamer, &f, 1);
		*length |= f;
		return RL_OK;
	} else {
		if (is_encoded) {
			*is_encoded = 0;
		}
		RL_CALL(read, RL_OK, streamer, f4, 4);
		*length = ntohl(*(uint32_t*)f4);
		return RL_OK;
	}
cleanup:
	return retval;
}

static int read_ziplist_entry(rl_restore_streamer *streamer, unsigned long prev_length, unsigned char **_entry, long *_length)
{
	int retval = RL_OK;
	long length = 0;
	unsigned char *entry;
	if (prev_length == 254) {
		RL_CALL(read_unsigned_int, RL_OK, streamer, &prev_length)
	}
	unsigned char ucaux;
	unsigned long entry_header;
	RL_CALL(read, RL_OK, streamer, &ucaux, 1);
	entry_header = ucaux;
	if ((entry_header >> 6) == 0) {
		length = entry_header & 0x3F;
	}
	else if ((entry_header >> 6) == 1) {
		length = ((entry_header & 0x3F) << 8);
		RL_CALL(read, RL_OK, streamer, &ucaux, 1);
		length |= ucaux;
	}
	else if ((entry_header >> 6) == 2) {
		// TODO: length = read_big_endian_unsigned_int(f)
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	} else if ((entry_header >> 4) == 12) {
		length = entry_header;
		RL_CALL(read, RL_OK, streamer, &ucaux, 1);
		length |= ucaux << 8;
	} else if ((entry_header >> 4) == 13) {
		unsigned char b[4];
		b[0] = entry_header;
		RL_CALL(read, RL_OK, streamer, &b[1], 3);
		RL_CALL(read_signed_int, RL_OK, b, &length);
	} else if ((entry_header >> 4) == 14) {
		unsigned char b[8];
		b[0] = entry_header;
		RL_CALL(read, RL_OK, streamer, &b[1], 7);
		RL_CALL(read_signed_long, RL_OK, b, &length);
	} else if (entry_header == 240) {
		unsigned char tmp[5];
		tmp[0] = tmp[4] = 0;
		tmp[1] = entry_header;
		RL_CALL(read, RL_OK, streamer, &tmp[2], 2);
		RL_CALL(read_signed_int, RL_OK, tmp, &length);
	} else if (entry_header == 254) {
		RL_CALL(read, RL_OK, streamer, &ucaux, 1);
		length = ucaux;
	} else if (entry_header >= 241 && entry_header <= 253) {
		RL_MALLOC(entry, sizeof(unsigned char) * 2);
		length = snprintf((char *)entry, 2, "%lu", entry_header - 241);
		goto ret;
	}
	RL_MALLOC(entry, sizeof(unsigned char) * length);
	RL_CALL(read, RL_OK, streamer, entry, length);
ret:
	*_entry = entry;
	*_length = length;
cleanup:
	return retval;
}

static int read_string(rl_restore_streamer *streamer, unsigned char **str, long *strlen)
{
	int retval = RL_OK;
	long length, strdatalen, cdatalen;
	unsigned char *strdata;
	int is_encoded;
	unsigned char ucint;
	int16_t i16int;
	int32_t i32int;
	RL_CALL(read_length_with_encoding, RL_OK, streamer, &length, &is_encoded);
	if (is_encoded && length == REDIS_RDB_ENC_INT8) {
		strdatalen = 5;
		RL_MALLOC(strdata, strdatalen * sizeof(unsigned char));
		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		strdatalen = snprintf((char *)strdata, strdatalen, "%d", (signed char)ucint);
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	} else if (is_encoded && length == REDIS_RDB_ENC_INT16) {
		strdatalen = 7;
		RL_MALLOC(strdata, strdatalen * sizeof(unsigned char));

		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i16int = ucint;
		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i16int |= ucint << 8;

		strdatalen = snprintf((char *)strdata, strdatalen, "%d", i16int);
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	} else if (is_encoded && length == REDIS_RDB_ENC_INT32) {
		strdatalen = 12;
		RL_MALLOC(strdata, strdatalen * sizeof(unsigned char));

		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i32int = ucint;
		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i32int |= ucint << 8;
		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i32int |= ucint << 16;
		RL_CALL(read, RL_OK, streamer, &ucint, 1);
		i32int |= ucint << 24;

		strdatalen = snprintf((char *)strdata, strdatalen, "%d", i32int);
		if (strdatalen < 0) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
	} else if (is_encoded && length == REDIS_RDB_ENC_LZF) {
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &cdatalen, NULL);
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &strdatalen, NULL);
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		unsigned char *cdata = malloc(sizeof(unsigned char) * cdatalen);
		RL_CALL(read, RL_OK, streamer, cdata, cdatalen);
		rl_lzf_decompress(cdata, cdatalen, strdata, strdatalen);
		free(cdata);
	} else if (!is_encoded) {
		strdatalen = length;
		strdata = rl_malloc(strdatalen * sizeof(unsigned char));
		RL_CALL(read, RL_OK, streamer, strdata, strdatalen);
	} else {
		retval = RL_NOT_IMPLEMENTED;
		goto cleanup;
	}
	*str = strdata;
	*strlen = strdatalen;
cleanup:
	return retval;
}

int rl_restore_stream(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, rl_restore_streamer *streamer) {
	int retval;
	unsigned char type, ucaux;
	long i, length, length2;
	unsigned char *strdata = NULL, *strdata2 = NULL, *strdata3 = NULL;
	long strdatalen = 0, strdata2len, strdata3len;
	unsigned long j, encoding, numentries, ulvalue;
	void **tmp = NULL;
	char f[40];
	double d;

	RL_CALL(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL(read, RL_OK, streamer, &type, 1);

	if (type == REDIS_RDB_TYPE_STRING) {
		RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
		if (key) {
			RL_CALL(rl_set, RL_OK, db, key, keylen, strdata, strdatalen, 1, expires);
		}
	}
	else if (type == REDIS_RDB_TYPE_LIST) {
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
			if (key) {
				RL_CALL(rl_push, RL_OK, db, key, keylen, 1, 0, 1, &strdata, &strdatalen, NULL);
			}
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_SET) {
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
			if (key) {
				RL_CALL(rl_sadd, RL_OK, db, key, keylen, 1, &strdata, &strdatalen, NULL);
			}
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_ZSET) {
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
			RL_CALL(read, RL_OK, streamer, &ucaux, 1);
			length2 = ucaux;
			if (length2 > 40 || length2 < 1) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			RL_CALL(read, RL_OK, streamer, (unsigned char *)f, length2);
			f[length2] = 0;
			d = strtold(f, NULL);

			if (key) {
				RL_CALL(rl_zadd, RL_OK, db, key, keylen, d, strdata, strdatalen);
			}
			rl_free(strdata);
			strdata = NULL;
		}
	}
	else if (type == REDIS_RDB_TYPE_HASH) {
		RL_CALL(read_length_with_encoding, RL_OK, streamer, &length, NULL);
		for (i = 0; i < length; i++) {
			RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
			RL_CALL(read_string, RL_OK, streamer, &strdata2, &length2);

			if (key) {
				RL_CALL(rl_hset, RL_OK, db, key, keylen, strdata, strdatalen, strdata2, length2, NULL, 0);
			}
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
		RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
		rl_restore_streamer *substreamer = init_string_streamer(strdata, strdatalen);
		RL_CALL(read, RL_OK, substreamer, NULL, 10);
		while (1) {
			RL_CALL(read, RL_OK, substreamer, &ucaux, 1);
			if (ucaux == 255) {
				break;
			}
			RL_CALL(read_ziplist_entry, RL_OK, substreamer, ucaux, &strdata2, &strdata2len);
			if (key) {
				RL_CALL(rl_push, RL_OK, db, key, keylen, 1, 0, 1, &strdata2, &strdata2len, NULL);
			}
			rl_free(strdata2);
			strdata2 = NULL;
		}
		free_string_streamer(substreamer);
		rl_free(strdata);
		strdata = NULL;
	}
	else if (type == REDIS_RDB_TYPE_SET_INTSET) {
		RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
		rl_restore_streamer *substreamer = init_string_streamer(strdata, strdatalen);
		RL_CALL(read_unsigned_int, RL_OK, substreamer, &encoding);
		RL_CALL(read_unsigned_int, RL_OK, substreamer, &numentries);
		if (encoding != 2 && encoding != 4 && encoding != 8) {
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		RL_MALLOC(tmp, sizeof(void *));
		for (j = 0; j < numentries; j++) {
			if (encoding == 8) {
				RL_CALL(read_unsigned_long, RL_OK, substreamer, &ulvalue);
			} else if (encoding == 4) {
				RL_CALL(read_unsigned_int, RL_OK, substreamer, &ulvalue);
			} else if (encoding == 2) {
				RL_CALL(read_unsigned_short, RL_OK, substreamer, &ulvalue);
			}
			length2 = snprintf(f, 40, "%lu", ulvalue);
			tmp[0] = f;
			if (key) {
				RL_CALL(rl_sadd, RL_OK, db, key, keylen, 1, (unsigned char **)tmp, &length2, NULL);
			}
		}
		free_string_streamer(substreamer);
	}
	else if (type == REDIS_RDB_TYPE_ZSET_ZIPLIST) {
		RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
		rl_restore_streamer *substreamer = init_string_streamer(strdata, strdatalen);
		RL_CALL(read, RL_OK, substreamer, NULL, 10);
		while (1) {
			RL_CALL(read, RL_OK, substreamer, &ucaux, 1);
			if (ucaux == 255) {
				break;
			}
			RL_CALL(read_ziplist_entry, RL_OK, substreamer, ucaux, &strdata2, &strdata2len);
			RL_CALL(read, RL_OK, substreamer, NULL, 1);
			RL_CALL(read_ziplist_entry, RL_OK, substreamer, ucaux, &strdata3, &strdata3len);

			if (strdata3len > 40 || strdata3len < 1) {
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
			memcpy(f, strdata3, strdata3len);
			f[strdata3len] = 0;
			d = strtold(f, NULL);
			if (key) {
				RL_CALL(rl_zadd, RL_OK, db, key, keylen, d, strdata2, strdata2len);
			}
			rl_free(strdata2);
			strdata2 = NULL;
			rl_free(strdata3);
			strdata3 = NULL;
		}
		free_string_streamer(substreamer);
		rl_free(strdata);
		strdata = NULL;
	}
	else if (type == REDIS_RDB_TYPE_HASH_ZIPLIST) {
		RL_CALL(read_string, RL_OK, streamer, &strdata, &strdatalen);
		rl_restore_streamer *substreamer = init_string_streamer(strdata, strdatalen);
		RL_CALL(read, RL_OK, substreamer, NULL, 10);
		while (1) {
			RL_CALL(read, RL_OK, substreamer, &ucaux, 1);
			if (ucaux == 255) {
				break;
			}
			RL_CALL(read_ziplist_entry, RL_OK, substreamer, ucaux, &strdata2, &strdata2len);
			RL_CALL(read, RL_OK, substreamer, NULL, 1);
			RL_CALL(read_ziplist_entry, RL_OK, substreamer, ucaux, &strdata3, &strdata3len);
			if (key) {
				RL_CALL(rl_hset, RL_OK, db, key, keylen, strdata2, strdata2len, strdata3, strdata3len, NULL, 0);
			}
			rl_free(strdata2);
			strdata2 = NULL;
			rl_free(strdata3);
			strdata3 = NULL;
		}
		free_string_streamer(substreamer);
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

int rl_restore(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, unsigned char *data, long datalen)
{
	int retval;
	rl_restore_streamer* streamer = NULL;
	RL_CALL(verify, RL_OK, data, datalen);
	streamer = init_string_streamer(data, datalen);
	if (!streamer) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	RL_CALL(rl_restore_stream, RL_OK, db, key, keylen, expires, streamer);
cleanup:
	if (streamer) {
		free_string_streamer(streamer);
	}
	return retval;
}
