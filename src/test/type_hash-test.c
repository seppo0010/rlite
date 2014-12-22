#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../rlite.h"
#include "../type_hash.h"
#include "test_util.h"

#define UNSIGN(str) ((unsigned char *)(str))

static int basic_test_hset_hget(int _commit)
{
	int retval = 0;
	long added;
	fprintf(stderr, "Start basic_test_hset_hget %d\n", _commit);

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *field = UNSIGN("my field");
	long fieldlen = strlen((char *)field);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2;
	long data2len;

	RL_CALL_VERBOSE(rl_hset, RL_OK, db, key, keylen, field, fieldlen, data, datalen, &added);

	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

	if (_commit) {
		RL_CALL_VERBOSE(rl_commit, RL_OK, db);
		RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);
	}

	RL_CALL_VERBOSE(rl_hget, RL_FOUND, db, key, keylen, field, fieldlen, &data2, &data2len);

	if (datalen != data2len) {
		fprintf(stderr, "expected %ld == %ld on line %d\n", datalen, data2len, __LINE__);
		retval = 1;
		goto cleanup;
	}

	if (memcmp(data, data2, datalen)) {
		fprintf(stderr, "expected %s == %s on line %d\n", data, data2, __LINE__);
		retval = 1;
		goto cleanup;
	}

	fprintf(stderr, "End basic_test_hset_hget\n");
	retval = 0;
cleanup:
	if (db) {
		rl_close(db);
	}
	return retval;
}

RL_TEST_MAIN_START(type_hash_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_hset_hget, i);
	}
}
RL_TEST_MAIN_END
