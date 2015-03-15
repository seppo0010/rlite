#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "rlite.h"
#include "util.h"
#include "wal.h"

static const char *db_path = "rlite-test.rld";
static const char *wal_path = ".rlite-test.rld.wal";

TEST test_full_wal(int _commit) {
	int retval;
	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_CALL_VERBOSE(rl_write_wal, RL_OK, wal_path, db, NULL, NULL);

	rl_close(db);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 0);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	ASSERT_EQm("Expected wal path not to exist", access(wal_path, F_OK), -1);

	rl_close(db);
	PASS();
}

TEST test_full_wal_readonly(int _commit) {
	int retval;
	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value"), *testvalue;
	long valuelen = strlen((char *)value), testvaluelen;

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_CALL_VERBOSE(rl_write_wal, RL_OK, wal_path, db, NULL, NULL);

	rl_close(db);
	RL_CALL_VERBOSE(rl_open, RL_OK, db_path, &db, RLITE_OPEN_READONLY);

	RL_CALL_VERBOSE(rl_get, RL_OK, db, key, keylen, &testvalue, &testvaluelen);
	EXPECT_BYTES(value, valuelen, testvalue, testvaluelen);
	rl_free(testvalue);

	ASSERT_EQm("Expected wal path to exist", access(wal_path, F_OK), 0);

	rl_close(db);
	PASS();
}

TEST test_partial_wal(int _commit) {
	int retval;
	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);

	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_CALL_VERBOSE(rl_write_wal, RL_OK, wal_path, db, NULL, NULL);
	truncate(wal_path, 3000);

	rl_close(db);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 0);

	RL_CALL_VERBOSE(rl_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL);
	ASSERT_EQm("Expected wal path not to exist", access(wal_path, F_OK), -1);

	rl_close(db);
	PASS();
}

TEST test_partial_wal_readonly(int _commit) {
	int retval;
	rlite *db;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *value = UNSIGN("my value");
	long valuelen = strlen((char *)value);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *value2 = UNSIGN("my value2");
	long value2len = strlen((char *)value2);

	// We need to make sure the database exiss before opening in read only
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key2, key2len, value2, value2len, 0, 0);
	RL_CALL_VERBOSE(rl_commit, RL_OK, db);
	rl_close(db);

	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 0);
	RL_CALL_VERBOSE(rl_set, RL_OK, db, key, keylen, value, valuelen, 0, 0);
	RL_CALL_VERBOSE(rl_write_wal, RL_OK, wal_path, db, NULL, NULL);
	truncate(wal_path, 3000);

	rl_close(db);
	RL_CALL_VERBOSE(rl_open, RL_OK, db_path, &db, RLITE_OPEN_READONLY);

	RL_CALL_VERBOSE(rl_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL);

	ASSERT_EQm("Expected wal path to exist", access(wal_path, F_OK), 0);

	rl_close(db);
	PASS();
}

SUITE(wal_test)
{
	RUN_TEST1(test_full_wal, 1);
	RUN_TEST1(test_full_wal_readonly, 1);
	RUN_TEST1(test_partial_wal, 1);
	RUN_TEST1(test_partial_wal_readonly, 1);
}
