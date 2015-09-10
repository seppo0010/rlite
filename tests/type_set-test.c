#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include "../src/rlite.h"
#include "../src/rlite/type_set.h"
#include "util.h"

#define IS_EQUAL(s1, l1, s2, l2)\
	(l1 == l2 && memcmp(s1, s2, l1) == 0)

TEST basic_test_sadd_sismember(int _commit)
{
	int retval;

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
	RL_BALANCED();
	EXPECT_LONG(count, 2);

	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, data, datalen);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key, keylen, data2, data2len);
	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, (unsigned char *)"new data", 8);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_scard(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &card);
	EXPECT_LONG(card, 1);

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_scard, RL_OK, db, key, keylen, &card);
	EXPECT_LONG(card, 2);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_srem(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_srem, RL_OK, db, key, keylen, 1, datas, dataslen, &count);
	RL_BALANCED();
	EXPECT_LONG(count, 1);

	RL_CALL_VERBOSE(rl_srem, RL_OK, db, key, keylen, 2, datas, dataslen, &count);
	RL_BALANCED();
	EXPECT_LONG(count, 1);

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_spop(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, key, keylen, &datapop, &datapoplen);
	RL_CALL_VERBOSE(rl_spop, RL_OK, db, key, keylen, &datapop2, &datapoplen2);
	RL_CALL_VERBOSE(rl_spop, RL_NOT_FOUND, db, key, keylen, NULL, NULL);

	if (!(datapoplen == datalen && memcmp(datapop, data, datalen) == 0 && datapoplen2 == data2len && memcmp(datapop2, data2, data2len) == 0) &&
			!(datapoplen == data2len && memcmp(datapop, data2, data2len) == 0 && datapoplen2 == datalen && memcmp(datapop2, data, datalen) == 0))
	{
		fprintf(stderr, "unexpected pop elements on line %d\n", __LINE__);
		FAIL();
	}

	rl_free(datapop);
	rl_free(datapop2);

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}
TEST basic_test_sadd_sdiff(int _commit)
{
	int retval;

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
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sdiff, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);
	EXPECT_LONG(datasc, 1);
	EXPECT_BYTES(data2, data2len, datasdiff[0], datasdifflen[0]);

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);


	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sdiff_nonexistent(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sdiff, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);
	EXPECT_LONG(datasc, 2);

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sdiffstore(int _commit)
{
	int retval;

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
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sdiffstore, RL_OK, db, target, targetlen, 2, keys, keyslen, &size);
	RL_BALANCED();
	EXPECT_LONG(size, 1);

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, target, targetlen, &datapop, &datapoplen);
	EXPECT_BYTES(datapop, datapoplen, data2, data2len);
	rl_free(datapop);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sinter(int _commit)
{
	int retval;

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
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 1, datas, dataslen, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sinter, RL_OK, db, 2, keys, keyslen, &datasc, &datasdiff, &datasdifflen);
	EXPECT_LONG(datasc, 1);
	EXPECT_BYTES(data, datalen, datasdiff[0], datasdifflen[0]);

	for (i = 0; i < datasc; i++) {
		rl_free(datasdiff[i]);
	}
	rl_free(datasdiff);
	rl_free(datasdifflen);


	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sinterstore(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sinterstore, RL_OK, db, target, targetlen, 2, keys, keyslen, &size);
	RL_BALANCED();
	EXPECT_LONG(size, 1);

	RL_CALL_VERBOSE(rl_spop, RL_OK, db, target, targetlen, &datapop, &datapoplen);
	EXPECT_BYTES(data, datalen, datapop, datapoplen);
	rl_free(datapop);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sunion(int _commit)
{
	int retval;

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
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 3, datas2, datas2len, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sunion, RL_OK, db, 2, keys, keyslen, &datasc, &datasunion, &datasunionlen);
	EXPECT_LONG(datasc, 4);

	EXPECT_BYTES(datas[1], dataslen[1], datasunion[0], datasunionlen[0]);
	EXPECT_BYTES(datas[0], dataslen[0], datasunion[1], datasunionlen[1]);
	EXPECT_BYTES(datas2[1], datas2len[1], datasunion[2], datasunionlen[2]);
	EXPECT_BYTES(datas2[0], datas2len[0], datasunion[3], datasunionlen[3]);

	for (i = 0; i < datasc; i++) {
		rl_free(datasunion[i]);
	}
	rl_free(datasunion);
	rl_free(datasunionlen);


	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sunionstore(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	unsigned char *target = UNSIGN("my target");
	long targetlen = strlen((char *)target);
	unsigned char *datas[2] = {UNSIGN("my data"), UNSIGN("my data2")};
	long dataslen[2] = {strlen((char *)datas[0]), strlen((char *)datas[1]) };
	unsigned char *datas2[3] = {UNSIGN("other data2"), UNSIGN("yet another data"), UNSIGN("my data")};
	long datas2len[3] = {strlen((char *)datas2[0]), strlen((char *)datas2[1]), strlen((char *)datas2[2])};
	unsigned char *keys[2] = {key, key2};
	long keyslen[2] = {keylen, key2len};
	unsigned char **datasunion;
	long *datasunionlen, datasc, i;
	long added;

	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key, keylen, 2, datas, dataslen, NULL);
	RL_BALANCED();
	RL_CALL_VERBOSE(rl_sadd, RL_OK, db, key2, key2len, 3, datas2, datas2len, NULL);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sunionstore, RL_OK, db, target, targetlen, 2, keys, keyslen, &added);
	EXPECT_LONG(added, 4);

	datasc = added;
	RL_CALL_VERBOSE(rl_srandmembers, RL_OK, db, target, targetlen, 0, &datasc, &datasunion, &datasunionlen);

#define ASSERT_IN_4(s, l)\
	if (!IS_EQUAL(s, l, datasunion[0], datasunionlen[0]) &&\
			!IS_EQUAL(s, l, datasunion[1], datasunionlen[1]) &&\
			!IS_EQUAL(s, l, datasunion[2], datasunionlen[2]) &&\
			!IS_EQUAL(s, l, datasunion[3], datasunionlen[3])\
			) {\
		fprintf(stderr, "Expected union to contains \"%s\" (%ld) on line %d\n", datas[0], dataslen[0], __LINE__);\
		FAIL();\
	}

	ASSERT_IN_4(datas[0], dataslen[0]);
	ASSERT_IN_4(datas[1], dataslen[1]);
	ASSERT_IN_4(datas2[0], datas2len[0]);
	ASSERT_IN_4(datas2[1], datas2len[1]);

	for (i = 0; i < datasc; i++) {
		rl_free(datasunion[i]);
	}
	rl_free(datasunion);
	rl_free(datasunionlen);


	rl_close(db);
	PASS();
}

TEST basic_test_sadd_sunionstore_empty(int _commit)
{
	int retval;

	rlite *db = NULL;
	RL_CALL_VERBOSE(setup_db, RL_OK, &db, _commit, 1);
	unsigned char *key = UNSIGN("my key");
	long keylen = strlen((char *)key);
	unsigned char *key2 = UNSIGN("my key2");
	long key2len = strlen((char *)key2);
	long added;

	RL_CALL_VERBOSE(rl_sunionstore, RL_OK, db, key, keylen, 1, &key2, &key2len, &added);
	RL_BALANCED();
	EXPECT_LONG(added, 0);

	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_smove(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_smove, RL_NOT_FOUND, db, key, keylen, key2, key2len, (unsigned char *)"new data", 8);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_smove, RL_OK, db, key, keylen, key2, key2len, data, datalen);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, data, datalen);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key2, key2len, data, datalen);

	RL_CALL_VERBOSE(rl_smove, RL_OK, db, key, keylen, key2, key2len, data2, data2len);
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_sismember, RL_NOT_FOUND, db, key, keylen, data2, data2len);
	RL_CALL_VERBOSE(rl_sismember, RL_FOUND, db, key2, key2len, data2, data2len);

	RL_CALL_VERBOSE(rl_smove, RL_NOT_FOUND, db, key, keylen, key2, key2len, data, datalen);
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);

	rl_close(db);
	PASS();
}

TEST basic_test_sadd_smembers(int _commit)
{
	int retval;

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
	RL_BALANCED();

	RL_CALL_VERBOSE(rl_smembers, RL_OK, db, &iterator, key, keylen);

	i = 0;
	while ((retval = rl_set_iterator_next(iterator, NULL, &testdata, &testdatalen)) == RL_OK) {
		if (i++ == 1) {
			EXPECT_BYTES(data, datalen, testdata, testdatalen);
		} else {
			EXPECT_BYTES(data2, data2len, testdata, testdatalen);
		}
		rl_free(testdata);
	}

	EXPECT_INT(retval, RL_END);

	rl_close(db);
	PASS();
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

TEST fuzzy_test_srandmembers_unique(long size, int _commit)
{
	int retval;

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
	EXPECT_LONG(added, size);
	RL_BALANCED();

	long memberc = 1;
	unsigned char **members = NULL;
	long *memberslen = NULL;
	RL_CALL_VERBOSE(rl_srandmembers, RL_OK, db, key, keylen, 0, &memberc, &members, &memberslen);
	for (i = 0; i < memberc; i++) {
		if (indexOf(size, elements, elementslen, members[i], memberslen[i]) == -1) {
			fprintf(stderr, "randmembers result not found in elements, got 0x");
			for (j = 0; j < memberslen[i]; j++) {
				fprintf(stderr, "%2x", members[i][j]);
			}
			fprintf(stderr, " on line %d\n", __LINE__);
			FAIL();
		}
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	memberc = size * 2;
	members = NULL;
	memberslen = NULL;
	RL_CALL_VERBOSE(rl_srandmembers, RL_OK, db, key, keylen, 0, &memberc, &members, &memberslen);
	EXPECT_LONG(size, memberc);

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
			FAIL();
		}
		if (results[j] > 0) {
			fprintf(stderr, "got repeated result on line %d\n", __LINE__);
			FAIL();
		}
		results[j]++;
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	memberc = size * 2;
	members = NULL;
	memberslen = NULL;
	RL_CALL_VERBOSE(rl_srandmembers, RL_OK, db, key, keylen, 1, &memberc, &members, &memberslen);
	EXPECT_LONG(memberc, size * 2);

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
			FAIL();
		}
		results[j]++;
		rl_free(members[i]);
	}
	rl_free(memberslen);
	rl_free(members);

	for (i = 0; i < size; i++) {
		free(elements[i]);
	}
	free(elements);
	free(results);
	free(elementslen);
	rl_close(db);
	PASS();
}

SUITE(type_set_test)
{
	int i;
	for (i = 0; i < 3; i++) {
		RUN_TEST1(basic_test_sadd_sismember, i);
		RUN_TEST1(basic_test_sadd_scard, i);
		RUN_TEST1(basic_test_sadd_srem, i);
		RUN_TEST1(basic_test_sadd_smove, i);
		RUN_TEST1(basic_test_sadd_smembers, i);
		RUN_TEST1(basic_test_sadd_spop, i);
		RUN_TEST1(basic_test_sadd_sdiff, i);
		RUN_TEST1(basic_test_sadd_sdiffstore, i);
		RUN_TEST1(basic_test_sadd_sdiff_nonexistent, i);
		RUN_TEST1(basic_test_sadd_sinter, i);
		RUN_TEST1(basic_test_sadd_sinterstore, i);
		RUN_TEST1(basic_test_sadd_sunion, i);
		RUN_TEST1(basic_test_sadd_sunionstore, i);
		RUN_TEST1(basic_test_sadd_sunionstore_empty, i);
		RUN_TESTp(fuzzy_test_srandmembers_unique, 10, i);
		RUN_TESTp(fuzzy_test_srandmembers_unique, 1000, i);
	}
}
