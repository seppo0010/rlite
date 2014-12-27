#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_set.h"
#include "test_util.h"

#define UNSIGN(str) ((unsigned char *)(str))

static int basic_test_sadd_sismember(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sismember %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	long count;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, &count);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (count != 2) {
		fprintf(stderr, "Expected count to be 2, got %ld instead on line %d\n", count, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, data, datalen);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, data2, data2len);
	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, (unsigned char *)"new data", 8);

	fprintf(stderr, "End basic_test_sadd_sismember\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_scard(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_scard %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	long card;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 1, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &card);

	if (card != 1) {
		fprintf(stderr, "Expected card to be 1, got %ld instead on line %d\n", card, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &card);

	if (card != 2) {
		fprintf(stderr, "Expected card to be 2, got %ld instead on line %d\n", card, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_sadd_scard\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_srem(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_srem %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	long count;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_srem, RL_OK, db, key, keylen, 1, datas, dataslen, &count);

	if (count != 1) {
		fprintf(stderr, "Expected count to be 1, got %ld instead on line %d\n", count, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_srem, RL_OK, db, key, keylen, 2, datas, dataslen, &count);

	if (count != 1) {
		fprintf(stderr, "Expected count to be 1, got %ld instead on line %d\n", count, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_sadd_srem\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_spop(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_spop %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char *datapop, *datapop2;
	long datapoplen, datapoplen2;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, key, keylen, &datapop, &datapoplen);
	RL_CALL_VERBOSE(rl_spop, RL_OK, db, key, keylen, &datapop2, &datapoplen2);
	RL_CALL_VERBOSE(rl_spop, RL_NOT_FOUND, db, key, keylen, NULL, NULL);

	if (!(datapoplen == datalen && memcmp(datapop, data, datalen) == 0 && datapoplen2 == data2len && memcmp(datapop2, data2, data2len) == 0) &&
			!(datapoplen == data2len && memcmp(datapop, data2, data2len) == 0 && datapoplen2 == datalen && memcmp(datapop2, data, datalen) == 0))
	{
		fprintf(stderr, "unexpected pop elements on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	rl_free(datapop);
	rl_free(datapop2);

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_sadd_spop\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}
static int basic_test_sadd_sdiff(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sdiff %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char **datasdiff;
	long *datasdifflen, datasc, i;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sdiff, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);

	if (datasc != 1) {
		fprintf(stderr, "Expected datasc %ld to be %d on line %d\n", datasc, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasdifflen[0] != data2len || memcmp(datasdiff[0], data2, data2len) != 0) {
		fprintf(stderr, "Expected diff to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", data2, datasdiff[0], datasdifflen[0], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);


	fprintf(stderr, "End basic_test_sadd_sdiff\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_sdiff_nonexistent(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sdiff_nonexistent %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char **datasdiff;
	long *datasdifflen, datasc, i;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sdiff, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);

	if (datasc != 2) {
		fprintf(stderr, "Expected datasc %ld to be %d on line %d\n", datasc, 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);


	fprintf(stderr, "End basic_test_sadd_sdiff_nonexistent\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_sdiffstore(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sdiffstore %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *target = UNSIGN("my target");
	long targetlen = strlen((char *)target);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char *datapop;
	long datapoplen, size;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sdiffstore, RL_OK, db, target, targetlen, 2, keys, keyslen, &size);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	if (size != 1) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, target, targetlen, &datapop, &datapoplen);

	if (datapoplen != data2len || memcmp(datapop, data2, data2len) != 0) {
		fprintf(stderr, "Expected diff to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", data2, datapop, datapoplen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(datapop);

	fprintf(stderr, "End basic_test_sadd_sdiffstore\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_sinter(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sinter %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char **datasdiff;
	long *datasdifflen, datasc, i;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sinter, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);

	if (datasc != 1) {
		fprintf(stderr, "Expected datasc %ld to be %d on line %d\n", datasc, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasdifflen[0] != datalen || memcmp(datasdiff[0], data, datalen) != 0) {
		fprintf(stderr, "Expected diff to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", data, datasdiff[0], datasdifflen[0], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);


	fprintf(stderr, "End basic_test_sadd_sinter\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_sinterstore(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sinterstore %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *target = UNSIGN("my target");
	long targetlen = strlen((char *)target);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char *datapop;
	long datapoplen, size;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sinterstore, RL_OK, db, target, targetlen, 2, keys, keyslen, &size);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	if (size != 1) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, target, targetlen, &datapop, &datapoplen);

	if (datapoplen != datalen || memcmp(datapop, data, datalen) != 0) {
		fprintf(stderr, "Expected inter to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", data, datapop, datapoplen, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	rl_free(datapop);

	fprintf(stderr, "End basic_test_sadd_sinterstore\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_sunion(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_sunion %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *datas[2] = {UNSIGN("my data"), UNSIGN("my data2")};
	long dataslen[2] = {strlen((char *)datas[0]), strlen((char *)datas[1]) };
	unsigned char *datas2[3] = {UNSIGN("other data2"), UNSIGN("yet another data"), UNSIGN("my data")};
	long datas2len[3] = {strlen((char *)datas2[0]), strlen((char *)datas2[1]), strlen((char *)datas2[2])};
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char **datasunion;
	long *datasunionlen, datasc, i;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 3, datas2, datas2len, NULL);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sunion, RL_OK, db, 2, keys, keyslen, &datasc, &datasunion, &datasunionlen);

	if (datasc != 4) {
		fprintf(stderr, "Expected datasc %ld to be %d on line %d\n", datasc, 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasunionlen[0] != dataslen[1] || memcmp(datasunion[0], datas[1], dataslen[1]) != 0) {
		fprintf(stderr, "Expected datasunion[0] to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", datas[1], datasunion[0], datasunionlen[0], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasunionlen[1] != dataslen[0] || memcmp(datasunion[1], datas[0], dataslen[0]) != 0) {
		fprintf(stderr, "Expected datasunion[1] to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", datas[0], datasunion[0], datasunionlen[0], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasunionlen[2] != datas2len[1] || memcmp(datasunion[2], datas2[1], datas2len[1]) != 0) {
		fprintf(stderr, "Expected datasunion[2] to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", datas2[1], datasunion[2], datasunionlen[2], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (datasunionlen[3] != datas2len[0] || memcmp(datasunion[3], datas2[0], datas2len[0]) != 0) {
		fprintf(stderr, "Expected datasunion[3] to be \"%s\" but got \"%s\" (%ld) instead on line %d\n", datas2[0], datasunion[3], datasunionlen[3], __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	for (i = 0; i < datasc; i++) {
		rl_free(datasunion[i]);
	}
	rl_free(datasunion);
	rl_free(datasunionlen);


	fprintf(stderr, "End basic_test_sadd_sunion\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_smove(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_smove %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_smove, RL_OK, db, key, keylen, key2, key2len, (unsigned char *)"new data", 8);
	RL_CALL_VERBOSE(rl_smove, RL_OK, db, key, keylen, key2, key2len, data, datalen);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, data, datalen);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key2, key2len, data, datalen);

	RL_CALL_VERBOSE(rl_smove, RL_OK, db, key, keylen, key2, key2len, data2, data2len);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, data2, data2len);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key2, key2len, data2, data2len);

	RL_CALL_VERBOSE(rl_smove, RL_NOT_FOUND, db, key, keylen, key2, key2len, data, datalen);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_sadd_smove\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static int basic_test_sadd_smembers(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_sadd_smembers %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("other data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	unsigned char *testdata;
	long testdatalen;
	int i;
	rl_set_iterator *iterator;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_smembers, RL_OK, db, &iterator, key, keylen);

	i = 0;
	while ((retval = rl_set_iterator_next(iterator, &testdata, &testdatalen)) == RL_OK) {
		if (i++ == 1) {
			if (testdatalen != datalen || memcmp(testdata, data, datalen) != 0) {
				fprintf(stderr, "Expected data to be \"%s\", got \"%s\" (%ld) on line %d\n", data, testdata, testdatalen, __LINE__);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		} else {
			if (testdatalen != data2len || memcmp(testdata, data2, data2len) != 0) {
				fprintf(stderr, "Expected data to be \"%s\", got \"%s\" (%ld) on line %d\n", data2, testdata, testdatalen, __LINE__);
				retval = RL_UNEXPECTED;
				goto cleanup;
			}
		}
		rl_free(testdata);
	}

	if (retval != RL_END) {
		fprintf(stderr, "Iterator didn't finish clearly, got %d on line %d\n", retval, __LINE__);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_sadd_smembers\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

static long indexOf(long size, unsigned char **elements, long *elementslen, unsigned char *element, long elementlen)
{
	long i;
	for (i = 0; i < size; i++) {
		if (elementslen[i] == elementlen && memcmp(elements[i], element, elementlen) == 0) {
			return i;
		}
	}
	return -1;
}

static int fuzzy_test_srandmembers_unique(long size, int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start fuzzy_test_srandmembers_unique %ld %d\n", size, _commit);

	unsigned char **elements = malloc(sizeof(unsigned char *) * size);
	long *elementslen = malloc(sizeof(long) * size);
	int *results = malloc(sizeof(int) * size);

	long i, j;
	for (i = 0; i < size; i++) {
		elementslen[i] = ((float)rand() / RAND_MAX) * 10 + 5;
		elements[i] = malloc(sizeof(unsigned char) * elementslen[i]);
		for (j = 0; j < elementslen[i]; j++) {
			elements[i][j] = rand() % CHAR_MAX;
		}
		j++;
	}

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	long added;
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, size, elements, elementslen, &added);
	if (added != size) {
		fprintf(stderr, "Expected to add %ld elements, but added %ld instead\n", size, added);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	long memberc = 1;
	unsigned char **members = NULL;
	long *memberslen = NULL;
	retval = rl_srandmembers(db, key, keylen, 0, &memberc, &members, &memberslen);
	for (i = 0; i < memberc; i++) {
		if (indexOf(size, elements, elementslen, members[i], memberslen[i]) == -1) {
			fprintf(stderr, "randmembers result not found in elements, got 0x");
			for (j = 0; j < memberslen[i]; j++) {
				fprintf(stderr, "%2x", members[i][j]);
			}
			fprintf(stderr, " on line %d\n", __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	memberc = size * 2;
	members = NULL;
	memberslen = NULL;
	retval = rl_srandmembers(db, key, keylen, 0, &memberc, &members, &memberslen);
	if (size != memberc) {
		fprintf(stderr, "Expected memberc %ld to be %ld on line %d\n", memberc, size, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	for (i = 0; i < size; i++) {
		results[i] = 0;
	}

	for (i = 0; i < memberc; i++) {
		j = indexOf(size, elements, elementslen, members[i], memberslen[i]);
		if (j == -1) {
			fprintf(stderr, "randmembers result not found in elements, got 0x");
			for (j = 0; j < memberslen[i]; j++) {
				fprintf(stderr, "%2x", members[i][j]);
			}
			fprintf(stderr, " on line %d\n", __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		if (results[j] > 0) {
			fprintf(stderr, "got repeated result on line %d\n", __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		results[j]++;
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	memberc = size * 2;
	members = NULL;
	memberslen = NULL;
	retval = rl_srandmembers(db, key, keylen, 1, &memberc, &members, &memberslen);
	if (memberc != size * 2) {
		fprintf(stderr, "Expected memberc %ld to be %ld on line %d\n", memberc, size * 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	for (i = 0; i < size; i++) {
		results[i] = 0;
	}

	for (i = 0; i < memberc; i++) {
		j = indexOf(size, elements, elementslen, members[i], memberslen[i]);
		if (j == -1) {
			fprintf(stderr, "randmembers result not found in elements, got 0x");
			for (j = 0; j < memberslen[i]; j++) {
				fprintf(stderr, "%2x", members[i][j]);
			}
			fprintf(stderr, " on line %d\n", __LINE__);
			retval = RL_UNEXPECTED;
			goto cleanup;
		}
		results[j]++;
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	fprintf(stderr, "End fuzzy_test_srandmembers_unique\n");
	retval = 0;
cleanup:
	for (i = 0; i < size; i++) {
		free(elements[i]);
	}
	free(elements);
	free(results);
	free(elementslen);
	if (db) {
		rl_close(db);
	}
	return retval;
}

RL_TEST_MAIN_START(type_set_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_sadd_sismember, i);
		RL_TEST(basic_test_sadd_scard, i);
		RL_TEST(basic_test_sadd_srem, i);
		RL_TEST(basic_test_sadd_smove, i);
		RL_TEST(basic_test_sadd_smembers, i);
		RL_TEST(basic_test_sadd_spop, i);
		RL_TEST(basic_test_sadd_sdiff, i);
		RL_TEST(basic_test_sadd_sdiffstore, i);
		RL_TEST(basic_test_sadd_sdiff_nonexistent, i);
		RL_TEST(basic_test_sadd_sinter, i);
		RL_TEST(basic_test_sadd_sinterstore, i);
		RL_TEST(basic_test_sadd_sunion, i);
		RL_TEST(fuzzy_test_srandmembers_unique, 10, i);
		RL_TEST(fuzzy_test_srandmembers_unique, 1000, i);
	}
}
RL_TEST_MAIN_END
