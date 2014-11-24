#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../page_key.h"
#include "../rlite.h"
#include "../type_zset.h"

static int expect_key(rlite *db, unsigned char *key, long keylen, char type, long page)
{
	unsigned char type2;
	long page2;
	int retval;
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type2, NULL, &page2, NULL);

	if (type != type2) {
		fprintf(stderr, "Expected type %c, got %c instead\n", type, type2);
		return 1;
	}

	if (page != page2) {
		fprintf(stderr, "Expected page %ld, got %ld instead\n", page, page2);
		return 1;
	}
	retval = 0;
cleanup:
	return retval;
}

int basic_test_set_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_get %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 0);

	if (_commit) {
		rl_commit(db);
	}

	retval = expect_key(db, key, keylen, type, page);
	if (retval != 0) {
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_set_get\n");
	rl_close(db);
	retval = 0;
cleanup:
	return retval;
}

int basic_test_get_unexisting()
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_get_unexisting\n");

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	retval = rl_key_get(db, key, keylen, NULL, NULL, NULL, NULL);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Expected not to find key %d\n", retval);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_get_unexisting\n");
	rl_close(db);
	retval = 0;
cleanup:
	return retval;
}

int basic_test_set_delete()
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_delete\n");

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);


	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 0);
	RL_CALL_VERBOSE(rl_key_delete, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_set_delete\n");
	rl_close(db);
	retval = 0;
cleanup:
	return retval;
}

int basic_test_get_or_create(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_get_or_create %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, page2 = 200; // set dummy values for != assert
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page);

	if (_commit) {
		rl_commit(db);
	}

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &page2);
	if (page != page2) {
		fprintf(stderr, "Expected page2 %ld to match page %ld\n", page2, page);
		goto cleanup;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_key_get_or_create(db, key, keylen, type + 1, &page2);
	if (retval != RL_WRONG_TYPE) {
		fprintf(stderr, "Expected get_or_create to return wrong type, got %d instead\n", retval);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_get_or_create\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

int basic_test_multidb(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_multidb %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, page2 = 200, pagetest; // set dummy values for != assert

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page2);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest);
	if (pagetest != page2) {
		fprintf(stderr, "Expected pagetest %ld to match page2 %ld\n", pagetest, page2);
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest);
	if (pagetest == page) {
		fprintf(stderr, "Expected pagetest %ld to mismatch page %ld\n", pagetest, page);
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 0);

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest);
	if (pagetest != page) {
		fprintf(stderr, "Expected pagetest %ld to match page %ld\n", pagetest, page);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_multidb\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

int basic_test_move(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_move %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, pagetest; // set dummy values for != assert

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_move, RL_OK, db, key, keylen, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest);
	if (pagetest != page) {
		fprintf(stderr, "Expected pagetest %ld to match page %ld\n", pagetest, page);
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_move\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

int basic_test_expires(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_expires %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100;

	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, mstime() - 1);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 10000 + mstime());

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_expires\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

int basic_test_change_expiration(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_change_expiration %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100;
	unsigned long long expiration = mstime() + 1000, expirationtest;

	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, expiration);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, &expirationtest);
	if (expirationtest != expiration) {
		fprintf(stderr, "Expected expirationtest %llu to match expiration %llu\n", expirationtest, expiration);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	RL_CALL_VERBOSE(rl_key_expires, RL_OK, db, key, keylen, expiration + 1000);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_key_expires, RL_OK, db, key, keylen, expiration - 10000);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL);

	fprintf(stderr, "End basic_test_change_expiration\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

int test_delete_with_value(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_delete_with_value %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 100, (unsigned char *)"asd", 3);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_delete_with_value\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_rename_ok(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_rename_ok %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 100, (unsigned char *)"asd", 3);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_rename, RL_OK, db, key, keylen, key2, key2len, 0);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_rename_ok\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_rename_overwrite(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_rename_overwrite %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	double score = 100, score2 = 200, scoretest;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_rename, RL_OK, db, key, keylen, key2, key2len, 1);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key2, key2len, data, datalen, &scoretest);
	if (scoretest != score) {
		fprintf(stderr, "Expected scoretest %lf to match score %lf on line %d\n", scoretest, score, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_rename_overwrite\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_rename_no_overwrite(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_rename_no_overwrite %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	double score = 100, score2 = 200, scoretest;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_rename, RL_FOUND, db, key, keylen, key2, key2len, 0);
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key2, key2len, data, datalen, &scoretest);
	if (scoretest != score2) {
		fprintf(stderr, "Expected scoretest %lf to match score2 %lf on line %d\n", scoretest, score2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_rename_no_overwrite\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_dbsize(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_dbsize %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	double score = 100, score2 = 200;
	long size;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);

	if (size != 1) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);

	if (size != 2) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);

	if (size != 1) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key2, key2len);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);

	if (size != 0) {
		fprintf(stderr, "Expected size %ld to be %d on line %d\n", size, 0, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_dbsize\n");
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

static int test_keys(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start test_keys %d\n", _commit);

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	double score = 100, score2 = 200;
	long len, *keyslen = NULL;
	unsigned char **keys = NULL;
	long i;

#define FREE_KEYS()\
	for (i = 0; i < len; i++) {\
		rl_free(keys[i]);\
	}\
	rl_free(keyslen);\
	rl_free(keys);\
	keys = NULL;\
	keyslen = NULL;\
	len = 0;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"", 0, &len, &keys, &keyslen);

	if (len != 0) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 0, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	FREE_KEYS();

	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);

	if (len != 1) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (memcmp(keys[0], key, keylen) != 0) {
		fprintf(stderr, "Expected keys[0] to match key on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (keyslen[0] != keylen) {
		fprintf(stderr, "Expected keyslen[0] to match keylen on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);

	if (len != 2) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 2, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if ((keyslen[0] == keylen && memcmp(keys[0], key, keylen) != 0) || (keyslen[1] == keylen && memcmp(keys[1], key, keylen) != 0)) {
		fprintf(stderr, "Expected keys to contain key on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if ((keyslen[0] == key2len && memcmp(keys[0], key2, key2len) != 0) || (keyslen[1] == key2len && memcmp(keys[1], key2, key2len) != 0)) {
		fprintf(stderr, "Expected keys to contain key2 on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*2", 2, &len, &keys, &keyslen);

	if (len != 1) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (memcmp(keys[0], key2, key2len) != 0) {
		fprintf(stderr, "Expected keys[0] to match key2 on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (keyslen[0] != key2len) {
		fprintf(stderr, "Expected keyslen[0] to match key2len on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);

	if (len != 1) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 1, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (memcmp(keys[0], key2, key2len) != 0) {
		fprintf(stderr, "Expected keys[0] to match key2 on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	if (keyslen[0] != key2len) {
		fprintf(stderr, "Expected keyslen[0] to match key2len on line %d\n", __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key2, key2len);
	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	}

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);

	if (len != 0) {
		fprintf(stderr, "Expected len %ld to be %d on line %d\n", len, 0, __LINE__);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	fprintf(stderr, "End test_keys\n");
	retval = 0;
cleanup:
	FREE_KEYS();
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(key_test)
{
	long i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_set_get, i);
		RL_TEST(basic_test_get_or_create, i);
		RL_TEST(basic_test_multidb, i);
		RL_TEST(basic_test_move, i);
		RL_TEST(basic_test_expires, i);
		RL_TEST(basic_test_change_expiration, i);
		RL_TEST(test_delete_with_value, i);
		RL_TEST(test_rename_ok, i);
		RL_TEST(test_rename_overwrite, i);
		RL_TEST(test_rename_no_overwrite, i);
		RL_TEST(test_dbsize, i);
		RL_TEST(test_keys, i);
	}
	RL_TEST(basic_test_get_unexisting, 0);
	RL_TEST(basic_test_set_delete, 0);
	RL_TEST(basic_test_get_or_create, 0);
}
RL_TEST_MAIN_END
