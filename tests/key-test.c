#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "util.h"
#include "../src/rlite/page_key.h"
#include "../src/rlite/rlite.h"
#include "../src/rlite/type_zset.h"

static int expect_key(rlite *db, unsigned char *key, long keylen, char type, long page)
{
	unsigned char type2;
	long page2;
	int retval;
	RL_CALL(rl_key_get, RL_FOUND, db, key, keylen, &type2, NULL, &page2, NULL, NULL);
	EXPECT_INT(type, type2);
	EXPECT_LONG(page, page2);
	retval = 0;
cleanup:
	return retval;
}

TEST basic_test_set_get(int _commit)
{
	int retval;
	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 0, 0);
	RL_COMMIT();
	RL_CALL_VERBOSE(expect_key, RL_OK, db, key, keylen, type, page);
	rl_close(db);
	PASS();
}

TEST basic_test_get_unexisting()
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	rl_close(db);
	PASS();
}

TEST basic_test_set_delete()
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 0, 0);
	RL_CALL_VERBOSE(rl_key_delete, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_get_or_create(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, page2 = 200; // set dummy values for != assert
	long version;
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page, &version);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &page2, &version);
	RL_COMMIT();
	EXPECT_LONG(page, page2);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_WRONG_TYPE, db, key, keylen, type + 1, &page2, &version);

	rl_close(db);
	PASS();
}

TEST basic_test_multidb(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, page2 = 200, pagetest; // set dummy values for != assert

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page, NULL);
	RL_COMMIT();
	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page2, NULL);
	RL_COMMIT();
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest, NULL);
	EXPECT_LONG(pagetest, page2);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest, NULL);
	ASSERT(pagetest != page);

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 0);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest, NULL);
	EXPECT_LONG(pagetest, page);

	rl_close(db);
	PASS();
}

TEST basic_test_move(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, pagetest; // set dummy values for != assert

	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page, NULL);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_move, RL_OK, db, key, keylen, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest, NULL);
	EXPECT_LONG(pagetest, page);
	rl_close(db);
	PASS();
}

TEST existing_test_move(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100, page2 = 101, pagetest; // set dummy values for != assert

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page, NULL);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 0);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_NOT_FOUND, db, key, keylen, type, &page2, NULL);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_move, RL_FOUND, db, key, keylen, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, &pagetest, NULL, NULL);
	EXPECT_LONG(pagetest, page2)

	RL_CALL_VERBOSE(rl_select, RL_OK, db, 1);
	RL_CALL_VERBOSE(rl_key_get_or_create, RL_FOUND, db, key, keylen, type, &pagetest, NULL);
	EXPECT_LONG(pagetest, page);
	rl_close(db);
	PASS();
}

TEST basic_test_expires(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100;

	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, rl_mstime() - 1, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, 10000 + rl_mstime(), 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_change_expiration(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 100;
	unsigned long long expiration = rl_mstime() + 1000, expirationtest;

	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, page, expiration, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, &expirationtest, NULL);
	EXPECT_LLU(expirationtest, expiration);
	RL_CALL_VERBOSE(rl_key_expires, RL_OK, db, key, keylen, expiration + 1000);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_key_expires, RL_OK, db, key, keylen, expiration - 10000);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST test_delete_with_value(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 100, (unsigned char *)"asd", 3);
	RL_COMMIT();
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	rl_close(db);
	PASS();
}

TEST test_rename_ok(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 100, (unsigned char *)"asd", 3);
	RL_COMMIT();
	RL_CALL_VERBOSE(rl_rename, RL_OK, db, key, keylen, key2, key2len, 0);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	rl_close(db);
	PASS();
}

TEST test_rename_overwrite(int _commit)
{
	int retval;

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
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_rename, RL_OK, db, key, keylen, key2, key2len, 1);
	RL_COMMIT();
	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key2, key2len, data, datalen, &scoretest);
	EXPECT_DOUBLE(scoretest, score);

	rl_close(db);
	PASS();
}

TEST test_rename_no_overwrite(int _commit)
{
	int retval;

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
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_rename, RL_FOUND, db, key, keylen, key2, key2len, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key2, key2len, data, datalen, &scoretest);
	EXPECT_DOUBLE(scoretest, score2);

	rl_close(db);
	PASS();
}

TEST test_dbsize(int _commit)
{
	int retval;

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
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);

	EXPECT_LONG(size, 1);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);
	EXPECT_LONG(size, 2);

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);
	EXPECT_LONG(size, 1);

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key2, key2len);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &size);
	EXPECT_LONG(size, 0);

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	rl_close(db);
	PASS();
}

TEST test_keys(int _commit)
{
	int retval;

	rlite *db;
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	double score = 100, score2 = 200;
	long len = 0, *keyslen = NULL;
	unsigned char **keys = NULL;
	long i;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

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
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"", 0, &len, &keys, &keyslen);
	EXPECT_LONG(len, 0);

	FREE_KEYS();

	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);
	EXPECT_LONG(len, 1);

	EXPECT_BYTES(keys[0], keyslen[0], key, keylen);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key2, key2len, score2, data, datalen);
	RL_COMMIT();

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);
	EXPECT_LONG(len, 2);

	if ((keyslen[0] == keylen && memcmp(keys[0], key, keylen) != 0) || (keyslen[1] == keylen && memcmp(keys[1], key, keylen) != 0)) {
		FAIL();
	}

	if ((keyslen[0] == key2len && memcmp(keys[0], key2, key2len) != 0) || (keyslen[1] == key2len && memcmp(keys[1], key2, key2len) != 0)) {
		FAIL();
	}

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*2", 2, &len, &keys, &keyslen);

	EXPECT_LONG(len, 1);
	EXPECT_BYTES(keys[0], keyslen[0], key2, key2len);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_COMMIT();

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);

	EXPECT_LONG(len, 1);
	EXPECT_BYTES(keys[0], keyslen[0], key2, key2len);

	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key2, key2len);
	RL_COMMIT();

	FREE_KEYS();
	RL_CALL_VERBOSE(rl_keys, RL_OK, db, (unsigned char*)"*", 1, &len, &keys, &keyslen);
	EXPECT_LONG(len, 0);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	FREE_KEYS();
	rl_close(db);
	PASS();
}

TEST test_randomkey(int _commit)
{
	int retval;

	rlite *db;
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char *testkey;
	long testkeylen, i;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_randomkey, RL_OK, db, &testkey, &testkeylen);
	EXPECT_BYTES(testkey, testkeylen, key, keylen);
	rl_free(testkey);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key2, key2len, data, datalen, 0, 0);
	RL_COMMIT();

	for (i = 0; i < 20; i++) {
		RL_CALL_VERBOSE(rl_randomkey, RL_OK, db, &testkey, &testkeylen);

		if ((keylen == testkeylen && memcmp(key, testkey, testkeylen) != 0) || (key2len == testkeylen && memcmp(key2, testkey, testkeylen) != 0)) {
			FAIL();
		}
		rl_free(testkey);
	}

	rl_close(db);
	PASS();
}

TEST test_flushdb(int _commit)
{
	int retval;

	rlite *db;
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *key2 = (unsigned char *)"my key 2";
	long key2len = strlen((char *)key2);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	long len;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key2, key2len, data, datalen, 0, 0);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_flushdb, RL_OK, db);
	RL_COMMIT();

	RL_CALL_VERBOSE(rl_dbsize, RL_OK, db, &len);
	EXPECT_LONG(len, 0);

	rl_close(db);
	PASS();
}

TEST string_version_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char type = 'A';
	long page = 23, version = -1, version2 = -2, version3 = -3;
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version2);
	EXPECT_LONG(version + 1, version2);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version3);
	ASSERT(version3 != version && version2 != version);
	rl_close(db);
	PASS();
}

TEST list_version_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char type = 'A';
	long page = 23, version = -1, version2 = -2, version3 = -3;
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &data, &datalen, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version);
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &data, &datalen, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version2);
	EXPECT_LONG(version + 1, version2);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_push, RL_OK, db, key, keylen, 1, 1, 1, &data, &datalen, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version3);
	ASSERT(version3 != version && version2 != version);
	rl_close(db);
	PASS();
}

TEST set_version_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char *data2 = (unsigned char *)"asd2";
	long data2len = strlen((char *)data2);
	unsigned char type = 'A';
	long page = 23, version = -1, version2 = -2, version3 = -3;
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 1, &data, &datalen, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 1, &data2, &data2len, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version2);
	EXPECT_LONG(version + 1, version2);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 1, &data, &datalen, NULL);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version3);
	ASSERT(version3 != version && version2 != version);

	rl_close(db);
	PASS();
}

TEST zset_version_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char *data2 = (unsigned char *)"asd2";
	long data2len = strlen((char *)data2);
	unsigned char type = 'A';
	long page = 23, version = -1, version2 = -2, version3 = -3;
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, data, datalen);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, data2, data2len);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version2);
	EXPECT_LONG(version + 1, version2);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, data, datalen);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version3);
	ASSERT(version3 != version && version2 != version);

	rl_close(db);
	PASS();
}

TEST hash_version_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *field = (unsigned char *)"field";
	long fieldlen = strlen((char *)field);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);
	unsigned char *data2 = (unsigned char *)"asd2";
	long data2len = strlen((char *)data2);
	unsigned char type = 'A';
	long page = 23, version = -1, version2 = -2, version3 = -3;
	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version);
	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data2, data2len, NULL, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version2);
	EXPECT_LONG(version + 1, version2);
	RL_CALL_VERBOSE(rl_key_delete_with_value, RL_OK, db, key, keylen);
	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, NULL, 1);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type, NULL, &page, NULL, &version3);
	ASSERT(version3 != version && version2 != version);

	rl_close(db);
	PASS();
}


TEST watch_test(int _commit)
{
	int retval;

	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char *data = (unsigned char *)"asd";
	long datalen = strlen((char *)data);

	struct watched_key *wkey;

	RL_CALL_VERBOSE(rl_watch, RL_OK, db, &wkey, key, keylen);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, data, datalen, 0, 0);
	RL_CALL_VERBOSE(rl_check_watched_keys, RL_OUTDATED, db, 1, &wkey);
	rl_free(wkey);

	RL_CALL_VERBOSE(rl_watch, RL_OK, db, &wkey, key, keylen);
	RL_CALL_VERBOSE(rl_check_watched_keys, RL_OK, db, 1, &wkey);
	rl_free(wkey);

	rl_close(db);
	PASS();
}

SUITE(key_test)
{
	long i;
	for (i = 0; i < 3; i++) {
		RUN_TESTp(basic_test_set_get, i);
		RUN_TESTp(basic_test_get_or_create, i);
		RUN_TESTp(basic_test_multidb, i);
		RUN_TESTp(basic_test_move, i);
		RUN_TESTp(existing_test_move, i);
		RUN_TESTp(basic_test_expires, i);
		RUN_TESTp(basic_test_change_expiration, i);
		RUN_TESTp(test_delete_with_value, i);
		RUN_TESTp(test_rename_ok, i);
		RUN_TESTp(test_rename_overwrite, i);
		RUN_TESTp(test_rename_no_overwrite, i);
		RUN_TESTp(test_dbsize, i);
		RUN_TESTp(test_keys, i);
		RUN_TESTp(test_randomkey, i);
		RUN_TESTp(test_flushdb, i);
		RUN_TESTp(string_version_test, i);
		RUN_TESTp(list_version_test, i);
		RUN_TESTp(set_version_test, i);
		RUN_TESTp(zset_version_test, i);
		RUN_TESTp(hash_version_test, i);
		RUN_TESTp(watch_test, i);
	}
	RUN_TEST(basic_test_get_unexisting);
	RUN_TEST(basic_test_set_delete);
}
