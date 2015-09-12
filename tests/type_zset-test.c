#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../src/rlite.h"
#include "../src/rlite/type_zset.h"
#include "../src/rlite/page_key.h"
#include "util.h"
#include "greatest.h"

TEST basic_test_zadd_zscore(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 1.41, score2;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, data, datalen, &score2);

	ASSERT_EQ(score, score2);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zscore2(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 8913.109, score2;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long datalen2 = strlen((char *)data2);
	long card;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zcard, RL_OK, db, key, keylen, &card);
	EXPECT_LONG(card, 1);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data2, datalen2);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zcard, RL_OK, db, key, keylen, &card);
	EXPECT_LONG(card, 2);

	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, data, datalen, &score2);
	EXPECT_DOUBLE(score, score2);

	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, data2, datalen2, &score2);
	EXPECT_DOUBLE(score, score2);

	RL_BALANCED();

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zrank(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 8913.109;
	long rank;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long datalen2 = strlen((char *)data2);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data2, datalen2);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zrank, RL_FOUND, db, key, keylen, data, datalen, &rank);
	EXPECT_LONG(rank, 0);

	RL_CALL_VERBOSE(rl_zrank, RL_FOUND, db, key, keylen, data2, datalen2, &rank);
	EXPECT_LONG(rank, 1);

	RL_CALL_VERBOSE(rl_zrevrank, RL_FOUND, db, key, keylen, data, datalen, &rank);
	EXPECT_LONG(rank, 1);

	RL_CALL_VERBOSE(rl_zrevrank, RL_FOUND, db, key, keylen, data2, datalen2, &rank);
	EXPECT_LONG(rank, 0);

	rl_close(db);
	PASS();
}

TEST basic_test_invalidlex()
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 1.41;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_CALL_VERBOSE(rl_zrangebylex, RL_INVALID_PARAMETERS, db, key, keylen, UNSIGN("foo"), 3, UNSIGN("bar"), 3, 0, -1, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zrange(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, 0, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	long i, setdatalen = 1;
	unsigned char setdata[1];
	for (i = 0; i < 200; i++) {
		setdata[0] = i;

		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, i * 10.5, setdata, setdatalen);
		RL_BALANCED();
	}

	unsigned char *data;
	long datalen;
	double score;
	rl_zset_iterator* iterator;

	RL_CALL_VERBOSE(rl_zrange, RL_OK, db, key, keylen, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 200);
	rl_zset_iterator_destroy(iterator);

	RL_CALL_VERBOSE(rl_zrevrange, RL_OK, db, key, keylen, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 200);
	rl_zset_iterator_destroy(iterator);

	RL_CALL_VERBOSE(rl_zrange, RL_OK, db, key, keylen, 0, 0, &iterator);
	EXPECT_LONG(iterator->size, 1);

	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, NULL, &data, &datalen);
	EXPECT_BYTES("\0", 1, data, datalen);

	rl_free(data);
	rl_zset_iterator_destroy(iterator);

	RL_CALL_VERBOSE(rl_zrevrange, RL_OK, db, key, keylen, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 200);

	RL_CALL_VERBOSE(rl_zset_iterator_next, retval, iterator, NULL, &score, &data, &datalen);
	EXPECT_DOUBLE(score, 10.5 * 199);
	EXPECT_INT(data[0], 199);
	EXPECT_LONG(datalen, 1);

	rl_free(data);
	rl_zset_iterator_destroy(iterator);

	rl_close(db);
	PASS();
}

TEST test_zrangebylex(rlite *db, unsigned char *key, long keylen, long initial, long size, long total_size, unsigned char min[3], long minlen, unsigned char max[3], long maxlen, long offset, long limit)
{
	rl_zset_iterator *iterator;
	long lexcount;
	int retval;
	unsigned char *data2;
	long data2_len;
	long i;
	if (offset == 0 && limit == 0) {
		// These test expect different values if offset or limit exist
		RL_CALL2_VERBOSE(rl_zlexcount, RL_OK, RL_NOT_FOUND, db, key, keylen, min, minlen, max, maxlen, &lexcount);
		if (retval != RL_NOT_FOUND || size != 0) {
			EXPECT_LONG(size, lexcount);
		}
	}

	RL_CALL2_VERBOSE(rl_zrangebylex, RL_OK, RL_NOT_FOUND, db, key, keylen, min, minlen, max, maxlen, offset, limit, &iterator);
	if (retval != RL_NOT_FOUND || size != 0) {
		EXPECT_LONG(size, iterator->size);
		i = initial;
		while ((retval = rl_zset_iterator_next(iterator, NULL, NULL, &data2, &data2_len)) == RL_OK) {
			EXPECT_INT(data2_len, ((i & 1) == 0 ? 1 : 2));
			EXPECT_INT(data2[0], 'a' + (i / 2));
			if (data2_len == 2 && data2[1] != 'A' + i) {
				FAIL();
			}
			i++;
			rl_free(data2);
		}

		EXPECT_LONG(i, size + initial);
		EXPECT_INT(retval, RL_END);
	}

	RL_CALL2_VERBOSE(rl_zrevrangebylex, RL_OK, RL_NOT_FOUND, db, key, keylen, max, maxlen, min, minlen, offset, limit, &iterator);
	if (retval != RL_NOT_FOUND || size != 0) {
		EXPECT_LONG(size, iterator->size);

		i = total_size - 1 - offset;
		while ((retval = rl_zset_iterator_next(iterator, NULL, NULL, &data2, &data2_len)) == RL_OK) {
			EXPECT_INT(data2_len, ((i & 1) == 0 ? 1 : 2));
			EXPECT_INT(data2[0], 'a' + (i / 2));
			if (data2_len == 2 && data2[1] != 'A' + i) {
				FAIL();
			}
			i--;
			rl_free(data2);
		}

		EXPECT_INT(retval, RL_END);

		EXPECT_LONG(i, total_size - size - 1 - offset);
	}
	PASS();
}

TEST basic_test_zadd_zrangebylex(int _commit)
{
#define ZRANGEBYLEX_SIZE 20
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	unsigned char data[2];
	long i;
	for (i = 0; i < ZRANGEBYLEX_SIZE; i++) {
		data[0] = 'a' + (i / 2);
		data[1] = 'A' + i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 1.0, data, ((i & 1) == 0) ? 1 : 2);
		RL_BALANCED();
	}

	unsigned char min[3];
	unsigned char max[3];

#define run_test_zrangebylex(min0, min1, minlen, max0, max1, max2, maxlen, initial, size, total_size, offset, limit)\
	min[0] = min0;\
	min[1] = min1;\
	max[0] = max0;\
	max[1] = max1;\
	max[2] = max2;\
	RL_CALL_VERBOSE(test_zrangebylex, 0, db, key, keylen, initial, size, total_size, min, minlen, max, maxlen, offset, limit);

	run_test_zrangebylex('-', 0, 1, '(', 'a', 0, 2, 0, 0, 0, 0, -1);
	run_test_zrangebylex('-', 0, 1, '[', 'a' - 1, 'a' - 1, 3, 0, 0, 0, 0, -1);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 0, ZRANGEBYLEX_SIZE, ZRANGEBYLEX_SIZE, 0, -1);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 1, ZRANGEBYLEX_SIZE - 1, ZRANGEBYLEX_SIZE, 1, -1);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 0, 1, ZRANGEBYLEX_SIZE, 0, 1);
	run_test_zrangebylex('(', 'c', 2, '+', 0, 0, 1, 5, ZRANGEBYLEX_SIZE - 5, ZRANGEBYLEX_SIZE, 0, -1);
	run_test_zrangebylex('[', 'c', 2, '+', 0, 0, 1, 4, ZRANGEBYLEX_SIZE - 4, ZRANGEBYLEX_SIZE, 0, -1);
	run_test_zrangebylex('[', 'c', 2, '[', 'f', 0, 2, 4, 7, 11, 0, -1);
	run_test_zrangebylex('-', 0, 1, '[', 'f', 0, 2, 0, 11, 11, 0, -1);
	run_test_zrangebylex('-', 0, 1, '[', 'c', 1, 3, 0, 5, 5, 0, -1);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zrangebylex_with_empty(int _commit)
{
	rl_zset_iterator *iterator;
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *min = UNSIGN("-");
	unsigned char *max = UNSIGN("(a");
	long lexcount;
	unsigned char *data2;
	long data2_len;

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, UNSIGN(""), 0);
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 0.0, UNSIGN("a"), 1);
	RL_BALANCED();

	// These test expect different values if offset or limit exist
	RL_CALL_VERBOSE(rl_zlexcount, RL_OK, db, key, keylen, min, 1, max, 2, &lexcount);
	EXPECT_LONG(lexcount, 1);

	RL_CALL_VERBOSE(rl_zrangebylex, RL_OK, db, key, keylen, min, 1, max, 2, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 1);
	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, NULL, &data2, &data2_len);
	EXPECT_LONG(data2_len, 0);
	rl_free(data2);

	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_END, iterator, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_zrevrangebylex, RL_OK, db, key, keylen, max, 2, min, 1, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 1);

	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, NULL, &data2, &data2_len);
	EXPECT_LONG(data2_len, 0);
	rl_free(data2);

	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_END, iterator, NULL, NULL, NULL, NULL);
	EXPECT_INT(retval, RL_END);

	rl_close(db);
	PASS();
}

TEST test_zrangebyscore(rlite *db, unsigned char *key, long keylen, rl_zrangespec *range, long size, double base_score, double step)
{
	rl_zset_iterator *iterator;
	long offset = 0, limit = -1;
	int retval;
	RL_CALL2_VERBOSE(rl_zrangebyscore, RL_OK, RL_NOT_FOUND, db, key, keylen, range, offset, limit, &iterator);
	if (size != 0 || retval != RL_NOT_FOUND) {
		double score;
		long i = 0;
		while ((retval = rl_zset_iterator_next(iterator, NULL, &score, NULL, NULL)) == RL_OK) {
			EXPECT_DOUBLE(score, base_score + i * step);
			i++;
		}
		EXPECT_INT(retval, RL_END);
		EXPECT_LONG(i, size);
	}

	RL_CALL2_VERBOSE(rl_zrevrangebyscore, RL_OK, RL_NOT_FOUND, db, key, keylen, range, offset, limit, &iterator);
	if (size != 0 || retval != RL_NOT_FOUND) {
		double score;
		long i = size - 1;
		while ((retval = rl_zset_iterator_next(iterator, NULL, &score, NULL, NULL)) == RL_OK) {
			EXPECT_DOUBLE(score, base_score + i * step);
			i--;
		}
		EXPECT_INT(retval, RL_END);
		EXPECT_LONG(i, -1);
	}

	PASS();
}

TEST basic_test_zadd_zrangebyscore(int _commit)
{
#define ZRANGEBYSCORE_SIZE 20
	int retval;
	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	unsigned char data[1];
	long i;
	for (i = 0; i < ZRANGEBYSCORE_SIZE; i++) {
		data[0] = 'a' + i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, i, data, 1);
		RL_BALANCED();
	}

	rl_zrangespec range;
#define run_test_zrangebyscore(_min, _minex, _max, _maxex, size, base_score) \
	range.min = _min;\
	range.minex = _minex;\
	range.max = _max;\
	range.maxex = _maxex;\
	RL_CALL_VERBOSE(test_zrangebyscore, 0, db, key, keylen, &range, size, base_score, 1);

	run_test_zrangebyscore(-INFINITY, 1, INFINITY, 1, ZRANGEBYSCORE_SIZE, 0);
	run_test_zrangebyscore(-INFINITY, 0, INFINITY, 1, ZRANGEBYSCORE_SIZE, 0);
	run_test_zrangebyscore(-INFINITY, 1, INFINITY, 0, ZRANGEBYSCORE_SIZE, 0);
	run_test_zrangebyscore(-INFINITY, 0, INFINITY, 0, ZRANGEBYSCORE_SIZE, 0);
	run_test_zrangebyscore(-INFINITY, 0, 1, 1, 1, 0);
	run_test_zrangebyscore(-5, 0, INFINITY, 0, ZRANGEBYSCORE_SIZE, 0);
	run_test_zrangebyscore(5, 0, INFINITY, 0, ZRANGEBYSCORE_SIZE - 5, 5);
	run_test_zrangebyscore(5, 0, 6, 1, 1, 5);
	run_test_zrangebyscore(21, 0, 40, 0, 0, 0);
	run_test_zrangebyscore(-INFINITY, 0, -5, 0, 0, 0);
	run_test_zrangebyscore(0, 0, 0, 0, 1, 0);
	run_test_zrangebyscore(0, 1, 0, 1, 0, 0);
	run_test_zrangebyscore(1, 0, 1, 0, 1, 1);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zrem(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 8913.109;
	long rank;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	unsigned char *data2 = UNSIGN("my data2");
	long datalen2 = strlen((char *)data2);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data2, datalen2);
	RL_BALANCED();

	unsigned char *members[1] = {data};
	long members_len[1] = {datalen};
	long changed;
	RL_CALL_VERBOSE(rl_zrem, RL_OK, db, key, keylen, 1, members, members_len, &changed);
	RL_BALANCED();

	EXPECT_LONG(changed, 1);

	RL_CALL_VERBOSE(rl_zrank, RL_NOT_FOUND, db, key, keylen, data, datalen, &rank);
	RL_CALL_VERBOSE(rl_zscore, RL_NOT_FOUND, db, key, keylen, data, datalen, NULL);
	RL_CALL_VERBOSE(rl_zrank, RL_FOUND, db, key, keylen, data2, datalen2, &rank);
	EXPECT_LONG(rank, 0);

	unsigned char *members2[2] = {data, data2};
	long members_len2[2] = {datalen, datalen2};
	RL_CALL_VERBOSE(rl_zrem, RL_OK, db, key, keylen, 2, members2, members_len2, &changed);
	EXPECT_LONG(changed, 1);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

#define ZCOUNT_SIZE 100
TEST basic_test_zadd_zcount(int _commit)
{
	int retval;

	rlite *db = NULL;
	long datalen = 20;
	unsigned char *data = malloc(sizeof(unsigned char) * datalen);
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	long i, count;

	for (i = 0; i < datalen; i++) {
		data[i] = i;
	}

	for (i = 0; i < ZCOUNT_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, i * 1.0, data, datalen);
		RL_BALANCED();
	}
	rl_free(data);
	data = NULL;

	rl_zrangespec range;
	range.min = -1000;
	range.minex = 0;
	range.max = 1000;
	range.maxex = 0;
	RL_CALL_VERBOSE(rl_zcount, RL_OK, db, key, keylen, &range, &count);
	EXPECT_LONG(count, ZCOUNT_SIZE);

	range.min = 0;
	range.minex = 0;
	range.max = ZCOUNT_SIZE - 1;
	range.maxex = 0;
	RL_CALL_VERBOSE(rl_zcount, RL_OK, db, key, keylen, &range, &count);
	EXPECT_LONG(count, ZCOUNT_SIZE);

	range.min = 0;
	range.minex = 1;
	range.max = ZCOUNT_SIZE - 1;
	range.maxex = 1;
	RL_CALL_VERBOSE(rl_zcount, RL_OK, db, key, keylen, &range, &count);
	EXPECT_LONG(count, ZCOUNT_SIZE - 2);

	range.min = 1;
	range.minex = 1;
	range.max = 2;
	range.maxex = 0;
	RL_CALL_VERBOSE(rl_zcount, RL_OK, db, key, keylen, &range, &count);
	EXPECT_LONG(count, 1);

	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zincrby(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 4.2;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);
	double newscore;

	RL_CALL_VERBOSE(rl_zincrby, RL_OK, db, key, keylen, score, data, datalen, &newscore);
	EXPECT_DOUBLE(score, newscore);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zincrby, RL_OK, db, key, keylen, score, data, datalen, &newscore);
	EXPECT_DOUBLE(score * 2, newscore);
	RL_BALANCED();

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_dupe(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	double score = 4.2, newscore;
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, score, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zadd, RL_FOUND, db, key, keylen, score * 2, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zscore, RL_FOUND, db, key, keylen, data, datalen, &newscore);
	EXPECT_DOUBLE(score * 2, newscore);

	rl_close(db);
	PASS();
}

TEST basic_test_zincrnan(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *data = UNSIGN("my data");
	long datalen = strlen((char *)data);

	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, INFINITY, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_zincrby, RL_NAN, db, key, keylen, -INFINITY, data, datalen, NULL);
	RL_BALANCED();

	rl_close(db);
	PASS();
}
#define ZINTERSTORE_KEYS 4
#define ZINTERSTORE_MEMBERS 10
TEST basic_test_zadd_zinterstore(int _commit, long params[5])
{
	unsigned char *data = NULL;
	int retval;

	long keys_len[ZINTERSTORE_KEYS];
	unsigned char *keys[ZINTERSTORE_KEYS];
	long i, j;
	for (i = 0; i < ZINTERSTORE_KEYS; i++) {
		keys[i] = malloc(sizeof(unsigned char));
		keys[i][0] = 'a' + i;
		keys_len[i] = 1;
	}

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char members[ZINTERSTORE_MEMBERS][1];
	for (i = 0; i < ZINTERSTORE_MEMBERS; i++) {
		members[i][0] = 'A' + i;
	}

	for (i = 1; i < ZINTERSTORE_KEYS; i++) {
		for (j = 0; j < ZINTERSTORE_MEMBERS; j++) {
			RL_CALL_VERBOSE(rl_zadd, RL_OK, db, keys[i], 1, i * j, members[j], 1);
			RL_BALANCED();
		}
		unsigned char own_member[1];
		own_member[0] = (unsigned char)(CHAR_MAX - i);
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, keys[i], 1, i, own_member, 1);
		RL_BALANCED();
	}

	double weights[3];
	weights[0] = params[1];
	weights[1] = params[2];
	weights[2] = params[3];
	RL_CALL_VERBOSE(rl_zinterstore, RL_OK, db, ZINTERSTORE_KEYS, keys, keys_len, params[1] == 0 && params[2] == 0 && params[3] == 0 ? NULL : weights, params[0]);
	RL_BALANCED();

	rl_zset_iterator *iterator;
	RL_CALL_VERBOSE(rl_zrange, RL_OK, db, keys[0], 1, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, ZINTERSTORE_MEMBERS);

	i = 0;
	long datalen;
	double score;
	while ((retval = rl_zset_iterator_next(iterator, NULL, &score, &data, &datalen)) == RL_OK) {
		EXPECT_DOUBLE(score, i * params[4]);
		EXPECT_LONG(data[0], members[i][0]);
		EXPECT_LONG(datalen, 1);
		rl_free(data);
		data = NULL;
		i++;
	}

	EXPECT_INT(retval, RL_END);


	for (i = 0; i < ZINTERSTORE_KEYS; i++) {
		rl_free(keys[i]);
	}
	rl_free(data);
	rl_close(db);
	PASS();
}

TEST basic_test_sadd_zinterstore(int _commit, long params[5])
{
	unsigned char *data = NULL;
	int retval;

	long keys_len[ZINTERSTORE_KEYS];
	unsigned char *keys[ZINTERSTORE_KEYS];
	long i;
	for (i = 0; i < ZINTERSTORE_KEYS; i++) {
		keys[i] = malloc(sizeof(unsigned char));
		keys[i][0] = 'a' + i;
		keys_len[i] = 1;
	}

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *members[ZINTERSTORE_MEMBERS];
	long memberslen[ZINTERSTORE_MEMBERS];
	for (i = 0; i < ZINTERSTORE_MEMBERS; i++) {
		members[i] = malloc(sizeof(unsigned char));
		members[i][0] = 'A' + i;
		memberslen[i] = 1;
	}

	for (i = 1; i < ZINTERSTORE_KEYS; i++) {
		RL_CALL_VERBOSE(rl_sadd, RL_OK, db, keys[i], 1, ZINTERSTORE_MEMBERS, (unsigned char **)members, memberslen, NULL);

		RL_BALANCED();
		unsigned char *own_member = malloc(sizeof(unsigned char));
		own_member[0] = (unsigned char)(CHAR_MAX - i);
		RL_CALL_VERBOSE(rl_sadd, RL_OK, db, keys[i], 1, 1, (unsigned char **)&own_member, keys_len, NULL);
		RL_BALANCED();
        free(own_member);
	}

	double weights[3];
	weights[0] = params[1];
	weights[1] = params[2];
	weights[2] = params[3];
	RL_CALL_VERBOSE(rl_zinterstore, RL_OK, db, ZINTERSTORE_KEYS, keys, keys_len, params[1] == 0 && params[2] == 0 && params[3] == 0 ? NULL : weights, params[0]);
	RL_BALANCED();

	rl_zset_iterator *iterator;
	RL_CALL_VERBOSE(rl_zrange, RL_OK, db, keys[0], 1, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, ZINTERSTORE_MEMBERS);

	i = 0;
	long datalen;
	double score;
	while ((retval = rl_zset_iterator_next(iterator, NULL, &score, &data, &datalen)) == RL_OK) {
		EXPECT_DOUBLE(score, params[4]);
		EXPECT_LONG(data[0], members[i][0]);
		EXPECT_LONG(datalen, 1);
		rl_free(data);
		data = NULL;
		free(members[i]);
		i++;
	}

	EXPECT_INT(retval, RL_END);


	for (i = 0; i < ZINTERSTORE_KEYS; i++) {
		rl_free(keys[i]);
	}
	rl_free(data);
	rl_close(db);
	PASS();
}

#define ZUNIONSTORE_KEYS 4
#define ZUNIONSTORE_MEMBERS 10
TEST basic_test_zadd_zunionstore(int _commit, long params[5])
{
	int retval;

	long keys_len[ZUNIONSTORE_KEYS];
	unsigned char *keys[ZUNIONSTORE_KEYS];
	long i, j;
	for (i = 0; i < ZUNIONSTORE_KEYS; i++) {
		keys[i] = malloc(sizeof(unsigned char));
		keys[i][0] = 'a' + i;
		keys_len[i] = 1;
	}

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char members[ZUNIONSTORE_MEMBERS][1];
	for (i = 0; i < ZUNIONSTORE_MEMBERS; i++) {
		members[i][0] = 'A' + i;
	}

	for (i = 1; i < ZUNIONSTORE_KEYS; i++) {
		for (j = 0; j < ZUNIONSTORE_MEMBERS; j++) {
			RL_CALL_VERBOSE(rl_zadd, RL_OK, db, keys[i], 1, i * j, members[j], 1);
			RL_BALANCED();
		}
		unsigned char own_member[1];
		own_member[0] = (unsigned char)(CHAR_MAX - i);
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, keys[i], 1, i, own_member, 1);
		RL_BALANCED();
	}

	RL_BALANCED();

	double weights[3];
	weights[0] = params[1];
	weights[1] = params[2];
	weights[2] = params[3];
	RL_CALL_VERBOSE(rl_zunionstore, RL_OK, db, ZUNIONSTORE_KEYS, keys, keys_len, params[1] == 0 && params[2] == 0 && params[3] == 0 ? NULL : weights, params[0]);

	RL_BALANCED();

	rl_zset_iterator *iterator;
	RL_CALL_VERBOSE(rl_zrange, RL_OK, db, keys[0], 1, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, ZUNIONSTORE_MEMBERS + ZUNIONSTORE_KEYS - 1);
	i = 0;
	unsigned char *data;
	long datalen;
	double score, exp_score;
	long pos;
	while ((retval = rl_zset_iterator_next(iterator, NULL, &score, &data, &datalen)) == RL_OK) {
		EXPECT_LONG(datalen, 1);
		if (data[0] == (unsigned char)(CHAR_MAX - 1) || data[0] == (unsigned char)(CHAR_MAX - 2) || data[0] == (unsigned char)(CHAR_MAX - 3)) {
			pos = (unsigned char)CHAR_MAX - data[0];
			exp_score = pos * (params[1] == 0 && params[2] == 0 && params[3] == 0 ? 1 : params[pos]);
			EXPECT_DOUBLE(exp_score, score);
			i--;
		} else {
			EXPECT_DOUBLE(score, i * params[4]);
			EXPECT_LONG(data[0], members[i][0]);
		}
		rl_free(data);
		i++;
	}

	EXPECT_INT(retval, RL_END);

	for (i = 0; i < ZUNIONSTORE_KEYS; i++) {
		rl_free(keys[i]);
	}
	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zremrangebyrank(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

#define ZREMRANGEBYRANK_SIZE 20
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key), i, changed;
	unsigned char data[1];
	for (i = 0; i < ZREMRANGEBYRANK_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, i * 1.0, data, 1);
		RL_BALANCED();
	}

#define run_test_zremrangebyrank(start, end, changed_expected, data_rank, rank_expected)\
	RL_CALL2_VERBOSE(rl_zremrangebyrank, RL_OK, RL_DELETED, db, key, keylen, start, end, &changed);\
	EXPECT_LONG(changed, changed_expected);\
	RL_BALANCED();\
	data[0] = data_rank;\
	RL_CALL2_VERBOSE(rl_zrank, RL_FOUND, RL_NOT_FOUND, db, key, keylen, data, 1, &i);\
	if (rank_expected == -1) {\
		EXPECT_INT(retval, RL_NOT_FOUND);\
	} else {\
		EXPECT_INT(retval, RL_FOUND);\
		EXPECT_LONG(i, rank_expected);\
	}

	run_test_zremrangebyrank(0, 10, 11, 19, 8);
	run_test_zremrangebyrank(-5, -1, 5, 14, 3);
	run_test_zremrangebyrank(0, 0, 1, 14, 2);
	run_test_zremrangebyrank(0, -1, 3, 0, -1);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zremrangebyscore(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

#define ZREMRANGEBYSCORE_SIZE 20
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key), i, changed;
	unsigned char data[1];
	for (i = 0; i < ZREMRANGEBYSCORE_SIZE; i++) {
		data[0] = i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, i * 10.0, data, 1);
		RL_BALANCED();
	}

	rl_zrangespec range;

#define run_test_zremrangebyscore(_min, _minex, _max, _maxex, changed_expected, data_score, rank_expected)\
	range.min = _min;\
	range.minex = _minex;\
	range.max = _max;\
	range.maxex = _maxex;\
	retval = rl_zremrangebyscore(db, key, keylen, &range, &changed);\
	if ((changed_expected > 0 && retval != RL_OK) || (changed_expected == 0 && retval != RL_NOT_FOUND)) {\
		FAIL();\
	}\
	EXPECT_LONG(changed, changed_expected);\
	RL_BALANCED();\
	data[0] = data_score;\
	RL_CALL2_VERBOSE(rl_zrank, RL_FOUND, RL_NOT_FOUND, db, key, keylen, data, 1, &i);\
	if (rank_expected == -1) {\
		EXPECT_INT(retval, RL_NOT_FOUND);\
	} else {\
		EXPECT_INT(retval, RL_FOUND);\
		EXPECT_LONG(i, rank_expected);\
	}

	run_test_zremrangebyscore(0, 0, 100, 1, 10, 19, 9);
	run_test_zremrangebyscore(0, 0, 100, 1, 0, 19, 9);
	run_test_zremrangebyscore(-INFINITY, 0, 100, 0, 1, 19, 8);
	run_test_zremrangebyscore(-INFINITY, 0, 100, 0, 0, 19, 8);
	run_test_zremrangebyscore(180, 0, INFINITY, 0, 2, 17, 6);
	run_test_zremrangebyscore(-INFINITY, 0, INFINITY, 0, 7, 0, -1);

	rl_close(db);
	PASS();
}

TEST basic_test_zadd_zremrangebylex(int _commit)
{
#define ZRANGEBYLEX_SIZE 20
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	unsigned char data[2];
	long i;
	for (i = 0; i < ZRANGEBYLEX_SIZE; i++) {
		data[0] = 'a' + (i / 2);
		data[1] = 'A' + i;
		RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 1.0, data, ((i & 1) == 0) ? 1 : 2);
		RL_BALANCED();
	}

	unsigned char min[3];
	unsigned char max[3];

	long changed;
#define run_remrangebylex(min, max, expected_changed)\
	RL_CALL_VERBOSE(rl_zremrangebylex, RL_OK, db, key, keylen, UNSIGN(min), strlen(min), UNSIGN(max), strlen(max), &changed);\
	EXPECT_LONG(changed, expected_changed);\
	RL_BALANCED();

	run_remrangebylex("-", "(a", 0);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 0, ZRANGEBYLEX_SIZE, ZRANGEBYLEX_SIZE, 0, -1)
	run_remrangebylex("-", "[a", 1);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 1, ZRANGEBYLEX_SIZE - 1, ZRANGEBYLEX_SIZE, 0, -1)
	run_remrangebylex("(a", "[b", 2);
	run_test_zrangebylex('-', 0, 1, '+', 0, 0, 1, 3, ZRANGEBYLEX_SIZE - 3, ZRANGEBYLEX_SIZE, 0, -1)

	rl_close(db);
	PASS();
}

TEST regression_zrangebyscore(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);

	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);

	unsigned char data[2];
	// -inf a 1 b 2 c 3 d 4 e 5 f +inf g
	data[0] = 'a';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, -INFINITY, data, 1);
	data[0] = 'b';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 1, data, 1);
	data[0] = 'c';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 2, data, 1);
	data[0] = 'd';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 3, data, 1);
	data[0] = 'e';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 4, data, 1);
	data[0] = 'f';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, 5, data, 1);
	data[0] = 'g';
	RL_CALL_VERBOSE(rl_zadd, RL_OK, db, key, keylen, INFINITY, data, 1);
	RL_BALANCED();

	rl_zrangespec range;
	range.min = 3;
	range.minex = 0;
	range.max = 6;
	range.maxex = 0;
	rl_zset_iterator *iterator;
	RL_CALL_VERBOSE(rl_zrangebyscore, RL_OK, db, key, keylen, &range, 0, -1, &iterator);
	EXPECT_LONG(iterator->size, 3);

	double score;
	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, &score, NULL, NULL);
	EXPECT_DOUBLE(score, 3.0);
	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, &score, NULL, NULL);
	EXPECT_DOUBLE(score, 4.0);
	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_OK, iterator, NULL, &score, NULL, NULL);
	EXPECT_DOUBLE(score, 5.0);
	RL_CALL_VERBOSE(rl_zset_iterator_next, RL_END, iterator, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

#define SADD_ZINTERSTORE_TESTS 4
#define ZINTERSTORE_TESTS 7
SUITE(type_zset_test)
{
	long sadd_zinterunionstore_tests[SADD_ZINTERSTORE_TESTS][5] = {
		{RL_ZSET_AGGREGATE_SUM, 0, 0, 0, 3},
		{RL_ZSET_AGGREGATE_SUM, 1, 1, 1, 3},
		{RL_ZSET_AGGREGATE_MIN, 1, 1, 1, 1},
		{RL_ZSET_AGGREGATE_MAX, 1, 1, 1, 1},
	};

	long zinterunionstore_tests[ZINTERSTORE_TESTS][5] = {
		{RL_ZSET_AGGREGATE_SUM, 0, 0, 0, 6},
		{RL_ZSET_AGGREGATE_SUM, 1, 1, 1, 6},
		{RL_ZSET_AGGREGATE_MIN, 1, 1, 1, 1},
		{RL_ZSET_AGGREGATE_MAX, 1, 1, 1, 3},
		{RL_ZSET_AGGREGATE_SUM, 5, 1, 1, 10},
		{RL_ZSET_AGGREGATE_MIN, 5, 1, 1, 2},
		{RL_ZSET_AGGREGATE_MAX, 5, 1, 1, 5},
	};
	int i, j;
	for (i = 0; i < 3; i++) {
		RUN_TESTp(basic_test_zadd_zscore, i);
		RUN_TESTp(basic_test_zadd_zscore2, i);
		RUN_TESTp(basic_test_zadd_zrank, i);
		RUN_TESTp(basic_test_zadd_zrem, i);
		RUN_TESTp(basic_test_zadd_zcount, i);
		RUN_TESTp(basic_test_zadd_zincrby, i);
		RUN_TESTp(basic_test_zadd_zrangebylex, i);
		RUN_TESTp(basic_test_zadd_zrangebylex_with_empty, i);
		RUN_TESTp(basic_test_zadd_zrangebyscore, i);
		RUN_TESTp(basic_test_zadd_zremrangebyrank, i);
		RUN_TESTp(basic_test_zadd_zremrangebyscore, i);
		RUN_TESTp(basic_test_zadd_zremrangebylex, i);
		RUN_TESTp(basic_test_zadd_dupe, i);
		RUN_TESTp(basic_test_zincrnan, i);
		RUN_TESTp(regression_zrangebyscore, i);
		for (j = 0; j < ZINTERSTORE_TESTS; j++) {
			RUN_TESTp(basic_test_zadd_zinterstore, i, zinterunionstore_tests[j]);
			RUN_TESTp(basic_test_zadd_zunionstore, i, zinterunionstore_tests[j]);
		}
		for (j = 0; j < SADD_ZINTERSTORE_TESTS; j++) {
			RUN_TESTp(basic_test_sadd_zinterstore, i, sadd_zinterunionstore_tests[j]);
		}
		RUN_TESTp(basic_test_zadd_zrange, 0);
	}
	RUN_TEST(basic_test_invalidlex);
}
