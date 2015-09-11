#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../src/rlite.h"
#include "../src/rlite/type_hash.h"
#include "util.h"

#define UNSIGN(str) ((unsigned char *)(str))

TEST basic_test_hset_hget(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = NULL;
	long data2len;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data2, &data2len);

	EXPECT_BYTES(data, datalen, data2, data2len);

	rl_free(data2);
	rl_close(db);
	PASS();
}

TEST basic_test_hset_hexists(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hexists, RL_FOUND, db, key, keylen, field, fieldlen);
	RL_CALL_VERBOSE(rl_hexists, RL_NOT_FOUND, db, key, keylen, field2, field2len);

	rl_close(db);
	PASS();
}

TEST basic_test_hset_hdel(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field2, field2len, data, datalen, NULL, 0);
	RL_BALANCED();

	long deleted;
	unsigned char *fields[3] = {field, field2, data};
	long fieldslen[3] = {fieldlen, field2len, datalen};

	RL_CALL_VERBOSE(rl_hdel, RL_OK, db, key, keylen, 3, fields, fieldslen, &deleted);
	RL_BALANCED();

	EXPECT_LONG(deleted, 2);

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_hset_hgetall(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);
	rl_hash_iterator *iterator;
	unsigned char *f, *m;
	long fl, ml, i = 0;

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field2, field2len, data2, data2len, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hgetall, RL_OK, db, &iterator, key, keylen);
	while ((retval = rl_hash_iterator_next(iterator, NULL, &f, &fl, NULL, &m, &ml)) == RL_OK) {
		if (i == 0) {
			EXPECT_BYTES(f, fl, field, fieldlen);
			EXPECT_BYTES(m, ml, data, datalen);
		}
		else if (i == 1) {
			EXPECT_BYTES(f, fl, field2, field2len);
			EXPECT_BYTES(m, ml, data2, data2len);
		}
		rl_free(f);
		rl_free(m);
		i++;
	}

	EXPECT_INT(retval, RL_END);
	EXPECT_LONG(i, 2);

	rl_close(db);
	PASS();
}

TEST basic_test_hset_hlen(int _commit)
{
	int retval;
	long len;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field2, field2len, data2, data2len, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hlen, RL_OK, db, key, keylen, &len);
	EXPECT_LONG(len, 2);

	rl_close(db);
	PASS();
}

TEST basic_test_hsetnx(int _commit)
{
	int retval;
	long added;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);
	unsigned char *data3;
	long data3len;

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, &added, 0);
	EXPECT_LONG(added, 1);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hset, RL_FOUND, db, key, keylen, field, fieldlen, data2, data2len, &added, 0);
	EXPECT_LONG(added, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data3, &data3len);
	EXPECT_BYTES(data, datalen, data3, data3len);
	rl_free(data3);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data2, data2len, &added, 1);
	EXPECT_LONG(added, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data3, &data3len);
	EXPECT_BYTES(data2, data2len, data3, data3len);
	rl_free(data3);

	rl_close(db);
	PASS();
}

TEST basic_test_hset_hmget(int _commit)
{
	int retval;
	long added;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);
	unsigned char *fields[3] = {field, (unsigned char *)"nonexistent", field2};
	long fieldslen[3] = {fieldlen, strlen((char *)fields[1]), field2len};
	unsigned char **datas = NULL;
	long *dataslen = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, &added, 0);
	EXPECT_LONG(added, 1);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field2, field2len, data2, data2len, &added, 0);
	EXPECT_LONG(added, 1);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hmget, RL_OK, db, key, keylen, 3, fields, fieldslen, &datas, &dataslen);
	EXPECT_BYTES(data, datalen, datas[0], dataslen[0]);

	EXPECT_LONG(dataslen[1], -1);
	EXPECT_PTR(datas[1], NULL);

	EXPECT_BYTES(data2, data2len, datas[2], dataslen[2]);

	if (datas) {
		for (added = 0; added < 3; added++) {
			rl_free(datas[added]);
		}
		rl_free(datas);
	}
	rl_free(dataslen);
	rl_close(db);
	PASS();
}

TEST basic_test_hmset_hmget(int _commit)
{
	int i, retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *field2 = UNSIGN("my field2");
	long field2len = strlen((char *)field2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);
	unsigned char *fieldsset[3] = {field, field2};
	long fieldslenset[3] = {fieldlen, field2len};
	unsigned char *fieldsset2[3] = {field2, field};
	long fieldslenset2[3] = {field2len, fieldlen};
	unsigned char *datasset[3] = {data, data2};
	long dataslenset[3] = {datalen, data2len};
	unsigned char *fields[3] = {field, (unsigned char *)"nonexistent", field2};
	long fieldslen[3] = {fieldlen, strlen((char *)fields[1]), field2len};
	unsigned char **datas = NULL;
	long *dataslen = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hmset, RL_OK, db, key, keylen, 2, fieldsset, fieldslenset, datasset, dataslenset);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hmget, RL_OK, db, key, keylen, 3, fields, fieldslen, &datas, &dataslen);
	EXPECT_BYTES(data, datalen, datas[0], dataslen[0]);
	EXPECT_LONG(dataslen[1], -1);
	EXPECT_PTR(datas[1], NULL);

	EXPECT_BYTES(data2, data2len, datas[2], dataslen[2]);

	for (i = 0; i < 3; i++) {
		rl_free(datas[i]);
	}
	rl_free(datas);
	datas = NULL;
	rl_free(dataslen);
	dataslen = NULL;

	RL_CALL_VERBOSE(rl_hmset, RL_OK, db, key, keylen, 2, fieldsset2, fieldslenset2, datasset, dataslenset);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hmget, RL_OK, db, key, keylen, 3, fields, fieldslen, &datas, &dataslen);
	EXPECT_BYTES(data2, data2len, datas[0], dataslen[0]);
	EXPECT_LONG(dataslen[1], -1);
	EXPECT_PTR(datas[1], NULL);
	EXPECT_BYTES(data, datalen, datas[2], dataslen[2]);

	if (datas) {
		for (i = 0; i < 3; i++) {
			rl_free(datas[i]);
		}
		rl_free(datas);
	}
	rl_free(dataslen);
	rl_close(db);
	PASS();
}

TEST basic_test_hincrby_hget(int _commit)
{
	int retval;
	long value;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = NULL;
	long datalen;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hincrby, RL_OK, db, key, keylen, field, fieldlen, 10, &value);
	EXPECT_LONG(value, 10);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data, &datalen);

	EXPECT_STR("10", data, datalen);
	rl_free(data);
	data = NULL;

	RL_CALL_VERBOSE(rl_hincrby, RL_OK, db, key, keylen, field, fieldlen, 100, &value);
	EXPECT_LONG(value, 110);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data, &datalen);
	EXPECT_STR("110", data, datalen);
	rl_free(data);
	data = NULL;

	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_test_hincrby_invalid(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hincrby, RL_NAN, db, key, keylen, field, fieldlen, 100, NULL);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

TEST basic_test_hincrby_overflow(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("-9223372036854775484");
	long datalen = strlen((char *)data);
	unsigned char *data2;
	long data2len;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hincrby, RL_OK, db, key, keylen, field, fieldlen, -1, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data2, &data2len);
	EXPECT_STR("-9223372036854775485", data2, data2len);
	rl_free(data2);

	RL_CALL_VERBOSE(rl_hincrby, RL_OVERFLOW, db, key, keylen, field, fieldlen, -10000, NULL);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

TEST basic_test_hincrbyfloat_hget(int _commit)
{
	int retval;
	double value;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = NULL;
	long i, datalen;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hincrbyfloat, RL_OK, db, key, keylen, field, fieldlen, 10.5, &value);
	RL_BALANCED();
	EXPECT_DOUBLE(value, 10.5);

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data, &datalen);
	EXPECT_STR("10.5", data, 4);

	for (i = 4; i < datalen; i++) {
		EXPECT_INT(data[i], '0');
	}
	rl_free(data);
	data = NULL;

	RL_CALL_VERBOSE(rl_hincrbyfloat, RL_OK, db, key, keylen, field, fieldlen, 100.3, &value);
	EXPECT_DOUBLE(value, 110.8);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data, &datalen);
	EXPECT_STR("110.8", data, 5);

	for (i = 5; i < datalen; i++) {
		EXPECT_INT(data[i], '0');
	}
	rl_free(data);
	data = NULL;

	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_test_hincrbyfloat_invalid(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hincrbyfloat, RL_NAN, db, key, keylen, field, fieldlen, 100, NULL);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

TEST basic_test_hset_del(int _commit)
{
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hget, RL_NOT_FOUND, db, key, keylen, field, fieldlen, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST hiterator_destroy()
{
	int _commit = 0;
	int retval;

	rlite *db = NULL;
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	rl_hash_iterator *iterator;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 0);
	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen - 1, data, datalen - 1, NULL, 0);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_hgetall, RL_OK, db, &iterator, key, keylen);
	RL_CALL_VERBOSE(rl_hash_iterator_next, RL_OK, iterator, NULL, NULL, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_hash_iterator_destroy, RL_OK, iterator);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

SUITE(type_hash_test)
{
	int i;
	for (i = 0; i < 3; i++) {
		RUN_TEST1(basic_test_hset_hget, i);
		RUN_TEST1(basic_test_hset_hexists, i);
		RUN_TEST1(basic_test_hset_hdel, i);
		RUN_TEST1(basic_test_hset_hgetall, i);
		RUN_TEST1(basic_test_hset_hlen, i);
		RUN_TEST1(basic_test_hsetnx, i);
		RUN_TEST1(basic_test_hset_hmget, i);
		RUN_TEST1(basic_test_hmset_hmget, i);
		RUN_TEST1(basic_test_hincrby_hget, i);
		RUN_TEST1(basic_test_hincrby_invalid, i);
		RUN_TEST1(basic_test_hincrby_overflow, i);
		RUN_TEST1(basic_test_hincrbyfloat_hget, i);
		RUN_TEST1(basic_test_hincrbyfloat_invalid, i);
		RUN_TEST1(basic_test_hset_del, i);
	}
	RUN_TEST(hiterator_destroy);
}
