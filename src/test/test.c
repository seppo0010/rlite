#include <stdio.h>
#include <stdlib.h>
#include "btree-test.h"
#include "list-test.h"
#include "rlite-test.h"
#include "string-test.h"
#include "type_string-test.h"
#include "type_list-test.h"
#include "type_set-test.h"
#include "type_zset-test.h"
#include "type_hash-test.h"
#include "key-test.h"
#include "multi_string-test.h"
#include "skiplist-test.h"
#include "long-test.h"
#include "parser-test.h"
#include "../util.h"

#ifdef DEBUG
int main(int argc, char *argv[]) {
	int skip_tests = 0, passed_tests = 0;
	if (argc >= 2) {
		test_mode = atoi(argv[1]);
	}
	if (argc >= 3) {
		skip_tests = atoi(argv[2]);
	}
#define RUN_TEST(name) \
	retval = name(&skip_tests, &passed_tests);\
	if (retval != 0 || expect_fail()) {\
		return retval;\
	}
#else
int main() {
#define RUN_TEST(name) retval = name(); if (retval != 0) return retval;
#endif
	int retval;
	RUN_TEST(long_test);
	RUN_TEST(btree_test);
	RUN_TEST(list_test);
	RUN_TEST(rlite_test);
	RUN_TEST(string_test);
	RUN_TEST(key_test);
	RUN_TEST(multi_string_test);
	RUN_TEST(skiplist_test);
	RUN_TEST(type_string_test);
	RUN_TEST(type_list_test);
	RUN_TEST(type_set_test);
	RUN_TEST(type_zset_test);
	RUN_TEST(type_hash_test);
	RUN_TEST(parser_test);
	return retval;
}
