#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "test_util.h"
#include "../rlite.h"

int test_rlite_page_cache()
{
	rlite *db = malloc(sizeof(rlite));
	int retval;
	void *obj;

	int size = 15;
	db->write_pages_len = 0;
	db->read_pages = malloc(sizeof(rl_page *) * size);
	db->read_pages_len = size;
	db->read_pages_alloc = size;
	long i;
	for (i = 0; i < size; i++) {
		db->read_pages[i] = malloc(sizeof(rl_page));
		db->read_pages[i]->page_number = i;
		db->read_pages[i]->type = &rl_data_type_header;
		// not a real life scenario, we just need any pointer
		db->read_pages[i]->obj = db->read_pages[i];
	}
	for (i = 0; i < size; i++) {
		retval = rl_read(db, &rl_data_type_header, i, NULL, &obj, 1);
		if (retval != RL_FOUND) {
			fprintf(stderr, "Expected retval to be RL_FOUND, got %d insted\n", retval);
			goto cleanup;
		}
		if (obj != db->read_pages[i]) {
			fprintf(stderr, "Expected obj to be %p, got %p insted\n", (void *) db->read_pages[i], obj);
			retval = 1;
			goto cleanup;
		}
	}
	for (i = 0; i < size; i++) {
		rl_free(db->read_pages[i]);
	}
	rl_free(db->read_pages);
	rl_free(db);
	retval = 0;
cleanup:
	return retval;
}

int test_has_key()
{
	rlite *db = NULL;
	int retval;
	const char *filepath = "rlite-test.rld";
	if (access(filepath, F_OK) == 0) {
		unlink(filepath);
	}
	retval = rl_open(filepath, &db, RLITE_OPEN_CREATE | RLITE_OPEN_READWRITE);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to open file\n");
		goto cleanup;
	}
	unsigned char type = 'C', type2;
	const unsigned char *key = (unsigned char *)"random key";
	long keylen = strlen((char *) key);
	long value = 529, value2;
	retval = rl_key_get(db, key, keylen, NULL, NULL, NULL);
	if (retval != RL_NOT_FOUND) {
		fprintf(stderr, "Failed to not find unexisting key (%d)\n", retval);
		goto cleanup;
	}
	retval = rl_key_set(db, key, keylen, type, value);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to set key (%d)\n", retval);
		goto cleanup;
	}
	retval = rl_key_get(db, key, keylen, &type2, NULL, &value2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Failed to find existing key (%d)\n", retval);
		goto cleanup;
	}
	if (value2 != value) {
		fprintf(stderr, "Expected value2 (%ld) to be equal to value (%ld)\n", value2, value);
		retval = 1;
		goto cleanup;
	}
	if (value2 != value) {
		fprintf(stderr, "Expected type2 (%d) to be equal to type (%d)\n", type2, type);
		retval = 1;
		goto cleanup;
	}
	retval = 0;
cleanup:
	rl_close(db);
	return retval;
}

RL_TEST_MAIN_START(rlite_test)
{
	RL_TEST(test_rlite_page_cache, 0);
	RL_TEST(test_has_key, 0);
}
RL_TEST_MAIN_END
