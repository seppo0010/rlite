#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include "rlite.h"

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
		retval = rl_read(db, &rl_data_type_header, i, NULL, &obj);
		if (retval != RL_FOUND) {
			fprintf(stderr, "Expected retval to be RL_FOUND, got %d insted\n", retval);
			return 1;
		}
		if (obj != db->read_pages[i]) {
			fprintf(stderr, "Expected obj to be %p, got %p insted\n", (void *) db->read_pages[i], obj);
			return 1;
		}
	}
	for (i = 0; i < size; i++) {
		free(db->read_pages[i]);
	}
	free(db->read_pages);
	free(db);
	return 0;
}

int test_has_key()
{
	rlite *db;
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
	const unsigned char *key = (unsigned char *)"random key";
	long keylen = strlen((char *) key);
	long value = 529, value2;
	retval = rl_get_key(db, key, keylen, NULL);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
	}
	else {
		fprintf(stderr, "Failed to not find unexisting key (%d)\n", retval);
		goto cleanup;
	}
	retval = rl_set_key(db, key, keylen, value);
	if (retval != RL_OK) {
		fprintf(stderr, "Failed to set key (%d)\n", retval);
		goto cleanup;
	}
	retval = rl_get_key(db, key, keylen, &value2);
	if (retval != RL_FOUND) {
		fprintf(stderr, "Failed to find existing key (%d)\n", retval);
		goto cleanup;
	}
	if (value2 != value) {
		fprintf(stderr, "Expected value2 (%ld) to be equal to value (%ld)\n", value2, value);
		retval = 1;
		goto cleanup;
	}
	retval = rl_close(db);
cleanup:
	return retval;
}

int main()
{
	int retval = test_rlite_page_cache();
	if (retval != 0) {
		return retval;
	}
	retval = test_has_key();
	if (retval != 0) {
		return retval;
	}
}
