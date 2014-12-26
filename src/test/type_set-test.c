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
	unsigned char *data2 = UNSIGN("my data2");
	long data2len = strlen((char *)data2);
	unsigned char *datas[2] = {data, data2};
	long dataslen[2] = {datalen, data2len};
	long count;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, &count);
	RL_CALL_VERBOSE(rl_is_balanced, RL_OK, db);

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

RL_TEST_MAIN_START(type_set_test)
{
	int i;
	for (i = 0; i < 2; i++) {
		RL_TEST(basic_test_sadd_sismember, i);
	}
}
RL_TEST_MAIN_END
