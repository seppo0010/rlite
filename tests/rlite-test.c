#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "util.h"
#include "../src/rlite.h"
#include "rlite/util.h"

TEST test_rlite_page_cache()
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
	PASS();
}

TEST test_has_key()
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
	rl_close(db);
	PASS();
}

#ifdef RL_DEBUG
TEST rl_open_oom()
{
	int j, i, retval;
	rlite *db = NULL;
	for (j = 0; j < 4; j++) {
		for (i = 1; ; i++) {
			test_mode = 1;
			test_mode_caller = j % 2 == 0 ? "rl_open" : "rl_create_db";
			test_mode_counter = i;
			retval = rl_open(j < 2 ? ":memory:" : "rlite-test.rld", &db, RLITE_OPEN_CREATE | RLITE_OPEN_READWRITE);
			if (retval == RL_OK) {
				if (i == 1) {
					fprintf(stderr, "No OOM triggered\n");
					FAIL();
				}
				rl_close(db);
				break;
			}
			EXPECT_INT(retval, RL_OUT_OF_MEMORY);
		}
	}
	test_mode = 0;
	PASS();
}
#endif

SUITE(rlite_test)
{
	RUN_TEST(test_rlite_page_cache);
	RUN_TEST(test_has_key);
#ifdef RL_DEBUG
	RUN_TEST(rl_open_oom);
#endif
}
