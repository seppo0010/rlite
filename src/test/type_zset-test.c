#include <stdlib.h>
#include <string.h>
#include "../rlite.h"
#include "../type_zset.h"
#include "test_util.h"

int basic_test_zadd_zscore(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_zadd_zscore %d\n", _commit);

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

	fprintf(stderr, "End basic_test_zadd_zscore\n");
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
	long card;

	retval = rl_zadd(db, key, keylen, score, data, datalen);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zadd %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_zcard(db, key, keylen, &card);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zcard %d\n", retval);
		return 1;
	}
	if (card != 1) {
		fprintf(stderr, "Expected zcard to be 1, got %ld instead\n", card);
		return 1;
	}

	retval = rl_zadd(db, key, keylen, score, data2, datalen2);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zadd a second time %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = rl_zcard(db, key, keylen, &card);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zcard a second time %d\n", retval);
		return 1;
	}
	if (card != 2) {
		fprintf(stderr, "Expected zcard to be 2, got %ld instead\n", card);
		return 1;
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

int basic_test_zadd_zrank(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_zadd_zrank %d\n", _commit);

	rlite *db = setup_db(_commit, 1);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	double score = 8913.109;
	long rank;
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

	retval = rl_zrank(db, key, keylen, data, datalen, &rank);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to zrank %d\n", retval);
		return 1;
	}

	if (0 != rank) {
		fprintf(stderr, "Expected rank %d to be %ld\n", 0, rank);
		return 1;
	}

	retval = rl_zrank(db, key, keylen, data2, datalen2, &rank);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to zrank %d\n", retval);
		return 1;
	}

	if (1 != rank) {
		fprintf(stderr, "Expected rank %d to be %ld\n", 1, rank);
		return 1;
	}

	fprintf(stderr, "End basic_test_zadd_zrank\n");
	rl_close(db);
	return 0;
}

int basic_test_zadd_zrange()
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_zadd_zrange\n");

	rlite *db = setup_db(0, 1);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);

	long i, setdatalen = 1;
	unsigned char setdata[1];
	for (i = 0; i < 200; i++) {
		setdata[0] = i;

		retval = rl_zadd(db, key, keylen, i * 10.5, setdata, setdatalen);
		if (retval != RL_OK) {
			fprintf(stderr, "Unable to zadd %d\n", retval);
			return 1;
		}
	}

	unsigned char **data;
	long *datalen, size;
	double *scores;
	retval = rl_zrange(db, key, keylen, 0, -1, &data, &datalen, &scores, &size);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zrange %d\n", retval);
		return 1;
	}
	if (size != 200) {
		fprintf(stderr, "Expected size to be 200, got %ld instead\n", size);
		return 1;
	}

	for (i = 0; i < 200; i++) {
		free(data[i]);
	}
	free(data);
	free(scores);
	free(datalen);

	retval = rl_zrange(db, key, keylen, 0, 0, &data, &datalen, &scores, &size);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to zrange %d\n", retval);
		return 1;
	}
	if (size != 1) {
		fprintf(stderr, "Expected size to be 1, got %ld instead\n", size);
		return 1;
	}
	if (data[0][0] != 0) {
		fprintf(stderr, "Expected data to be 0, got %d instead\n", data[0][0]);
		return 1;
	}
	if (datalen[0] != 1) {
		fprintf(stderr, "Expected data to be 1, got %ld instead\n", datalen[0]);
		return 1;
	}
	free(data[0]);
	free(data);
	free(scores);
	free(datalen);

	rl_close(db);
	fprintf(stderr, "End basic_test_zadd_zrange\n");
	return 0;
}

int main()
{
	int retval, i;
	for (i = 0; i < 2; i++) {
		retval = basic_test_zadd_zscore(i);
		if (retval != 0) {
			goto cleanup;
		}
		retval = basic_test_zadd_zscore2(i);
		if (retval != 0) {
			goto cleanup;
		}
		retval = basic_test_zadd_zrank(i);
		if (retval != 0) {
			goto cleanup;
		}
	}
	retval = basic_test_zadd_zrange();
	if (retval != 0) {
		goto cleanup;
	}
cleanup:
	return retval;
}
