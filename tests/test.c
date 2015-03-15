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
#include "multi-test.h"
#include "multi_string-test.h"
#include "skiplist-test.h"
#include "long-test.h"
#include "restore-test.h"
#include "dump-test.h"
#include "hyperloglog-test.h"
#include "sort-test.h"
#include "wal-test.h"
#include "db-test.h"
#include "hmulti-test.h"
#include "echo-test.h"
#include "hash-test.h"
#include "parser-test.h"
#include "hlist-test.h"
#include "set-test.h"
#include "hstring-test.h"
#include "zset-test.h"
#include "hsort-test.h"
#include "scripting-test.h"
#include "concurrency-test.h"
#include "../src/util.h"

#ifdef RL_DEBUG
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
	RUN_TEST(restore_test);
	RUN_TEST(dump_test);
	RUN_TEST(hyperloglog_test);
	RUN_TEST(sort_test);
	RUN_TEST(wal_test);
	if (run_echo() != 0) { return 1; }
	if (run_parser() != 0) { return 1; }
	if (run_db() != 0) { return 1; }
	if (run_multi() != 0) { return 1; }
	if (run_list() != 0) { return 1; }
	if (run_set() != 0) { return 1; }
	if (run_string() != 0) { return 1; }
	if (run_zset() != 0) { return 1; }
	if (run_hash() != 0) { return 1; }
	if (run_sort() != 0) { return 1; }
	if (run_scripting_test() != 0) { return 1; }
	if (run_concurrency() != 0) { return 1; }
	return retval;
}
