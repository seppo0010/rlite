#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "test_util.h"
#include "../obj_key.h"
#include "../rlite.h"

#define COLLISION_KEY_SIZE 128
static void get_test_key(unsigned char *data, char *filename)
{
	FILE *fp = fopen(filename, "r");
	if (!fp) {
		fprintf(stderr, "Unable to open file %s\n", filename);
		goto cleanup;
	}
	size_t read = fread(data, sizeof(unsigned char), COLLISION_KEY_SIZE, fp);
	if (COLLISION_KEY_SIZE != read) {
		fprintf(stderr, "Unable to read key from file %s\n", filename);
	}
cleanup:
	if (fp) {
		fclose(fp);
	}
}

static int expect_key(rlite *db, unsigned char *key, long keylen, char type, long page)
{
	unsigned char type2;
	long page2;
	int retval = rl_obj_key_get(db, key, keylen, &type2, &page2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Unable to get key %d\n", retval);
		return 1;
	}

	if (type != type2) {
		fprintf(stderr, "Expected type %c, got %c instead\n", type, type2);
		return 1;
	}

	if (page != page2) {
		fprintf(stderr, "Expected page %ld, got %ld instead\n", page, page2);
		return 1;
	}
	return 0;
}

int basic_test_set_get(int _commit)
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_get %d\n", _commit);

	rlite *db = setup_db(_commit);
	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	retval = rl_obj_key_set(db, key, keylen, type, page);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to set key %d\n", retval);
		return 1;
	}

	if (_commit) {
		rl_commit(db);
	}

	retval = expect_key(db, key, keylen, type, page);

	fprintf(stderr, "End basic_test_set_get\n");
	rl_close(db);
	return 0;
}

int basic_test_get_unexisting()
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_get_unexisting\n");

	rlite *db = setup_db(0);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	retval = rl_obj_key_get(db, key, keylen, NULL, NULL);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Expected not to found key %d\n", retval);
		return 1;
	}

	fprintf(stderr, "End basic_test_get_unexisting\n");
	rl_close(db);
	return 0;
}

int basic_test_set_delete()
{
	int retval = 0;
	fprintf(stderr, "Start basic_test_set_delete\n");

	rlite *db = setup_db(0);

	unsigned char *key = (unsigned char *)"my key";
	long keylen = strlen((char *)key);
	unsigned char type = 'A';
	long page = 23;
	retval = rl_obj_key_set(db, key, keylen, type, page);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to set key %d\n", retval);
		return 1;
	}

	retval = rl_obj_key_delete(db, key, keylen);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to delete key %d\n", retval);
		return 1;
	}

	retval = rl_obj_key_get(db, key, keylen, NULL, NULL);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
	} else {
		fprintf(stderr, "Expected not to find key, got %d instead\n", retval);
	}

	fprintf(stderr, "End basic_test_set_delete\n");
	rl_close(db);
	return 0;
}

int basic_test_set_collision()
{
	int retval = 0;
	unsigned char key1[128];
	unsigned char key2[128];
	fprintf(stderr, "Start basic_test_set_collision\n");

	rlite *db = setup_db(0);

	get_test_key(key1, "test/collision1");
	unsigned char type1 = 'A';
	long page1 = 23;
	retval = rl_obj_key_set(db, key1, COLLISION_KEY_SIZE, type1, page1);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to set key %d\n", retval);
		return 1;
	}

	get_test_key(key2, "test/collision2");
	unsigned char type2 = 'B';
	long page2 = 25;

	retval = rl_obj_key_set(db, key2, COLLISION_KEY_SIZE, type2, page2);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to set key %d\n", retval);
		return 1;
	}

	retval = expect_key(db, key1, COLLISION_KEY_SIZE, type1, page1);
	if (retval == 0) {
		retval = expect_key(db, key2, COLLISION_KEY_SIZE, type2, page2);
	}

	fprintf(stderr, "End basic_test_set_collision\n");
	rl_close(db);
	return 0;
}

int main()
{
	int retval = basic_test_set_get(0);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_set_get(1);
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_get_unexisting();
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_set_collision();
	if (retval != 0) {
		goto cleanup;
	}
	retval = basic_test_set_delete();
	if (retval != 0) {
		goto cleanup;
	}
cleanup:
	return retval;
}
