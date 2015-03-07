#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "test_util.h"
#include "../src/rlite.h"

int test_rlite_page_cache(int UNUSED(_))
{
	rlite *db = malloc(sizeof(rlite));
	db->driver_type = RL_MEMORY_DRIVER;
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
		RL_CALL_VERBOSE(rl_read, RL_FOUND, db, &rl_data_type_header, i, NULL, &obj, 1);
		EXPECT_PTR(obj, db->read_pages[i])
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

int test_has_key(int UNUSED(_))
{
	rlite *db = NULL;
	int retval;
	const char *filepath = "rlite-test.rld";
	if (access(filepath, F_OK) == 0) {
		unlink(filepath);
	}
	RL_CALL_VERBOSE(rl_open, RL_OK, filepath, &db, RLITE_OPEN_CREATE | RLITE_OPEN_READWRITE);
	unsigned char type = 'C', type2;
	const unsigned char *key = (unsigned char *)"random key";
	long keylen = strlen((char *) key);
	long value = 529, value2;
	RL_CALL_VERBOSE(rl_key_get, RL_NOT_FOUND, db, key, keylen, NULL, NULL, NULL, NULL, NULL);
	RL_CALL_VERBOSE(rl_key_set, RL_OK, db, key, keylen, type, value, 0, 0);
	RL_CALL_VERBOSE(rl_key_get, RL_FOUND, db, key, keylen, &type2, NULL, &value2, NULL, NULL);
	EXPECT_LONG(value, value2);
	EXPECT_INT(type, type2);
	retval = RL_OK;
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
