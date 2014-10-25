#include <string.h>
#include "../rlite.h"
#include "../type_zset.h"
#include "test_util.h"

int basic_test_zadd_zrange(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_zadd_zrange %d\n", _commit);

	rlite *db = setup_db(_commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	double score = 1.41, score2;
	unsigned char *data = (unsigned char *)"my data";
	long datalen = strlen((char *)data);

	retval = rl_zadd(db, key, keylen, score, data, datalen);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zadd %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_zscore(db, key, keylen, data, datalen, &score2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to zscore %d\n", retval);
		return 1;
	}

	if (score != score2) {
		fprintf(stderr, "Expected score %lf to match score2 %lf\n", score, score2);
		return 1;
	}

	fprintf(stderr, "End basic_test_zadd_zrange\n");
	rl_close(db);
	return 0;
}

int basic_test_zadd_zscore2(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_zadd_zscore2 %d\n", _commit);

	rlite *db = setup_db(_commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	double score = 8913.109, score2;
	unsigned char *data = (unsigned char *)"my data";
	long datalen = strlen((char *)data);
	unsigned char *data2 = (unsigned char *)"my data2";
	long datalen2 = strlen((char *)data2);

	retval = rl_zadd(db, key, keylen, score, data, datalen);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zadd %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_zadd(db, key, keylen, score, data2, datalen2);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zadd a second time %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_zscore(db, key, keylen, data, datalen, &score2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to zscore %d\n", retval);
		return 1;
	}

	if (score != score2) {
		fprintf(stderr, "Expected score %lf to match score2 %lf\n", score, score2);
		return 1;
	}

	retval = rl_zscore(db, key, keylen, data2, datalen2, &score2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to zscore %d\n", retval);
		return 1;
	}

	if (score != score2) {
		fprintf(stderr, "Expected score %lf to match score2 %lf\n", score, score2);
		return 1;
	}

	fprintf(stderr, "End basic_test_zadd_zscore2\n");
	rl_close(db);
	return 0;
}

int main()
{
	int retval = basic_test_zadd_zrange(0);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_zadd_zrange(1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_zadd_zscore2(0);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_zadd_zscore2(1);
	if (retval != 0) {
		goto cleanup;
	}
cleanup:
	return retval;
}
