#include <stdio.h>
#include <stdlib.h>
#include "greatest.h"

extern SUITE(btree_test);
extern SUITE(concurrency_test);
extern SUITE(db_test);
extern SUITE(rlite_test);
extern SUITE(list_test);
extern SUITE(string_test);
extern SUITE(multi_string_test);
extern SUITE(multi_test);
extern SUITE(key_test);
extern SUITE(type_string_test);
extern SUITE(type_list_test);
extern SUITE(type_set_test);
extern SUITE(type_zset_test);
extern SUITE(type_hash_test);
extern SUITE(skiplist_test);
extern SUITE(long_test);
extern SUITE(restore_test);
extern SUITE(hyperloglog_test);
extern SUITE(dump_test);
extern SUITE(sort_test);
extern SUITE(wal_test);
extern SUITE(zset_test);
extern SUITE(hmulti_test);
extern SUITE(hsort_test);
extern SUITE(scripting_test);
extern SUITE(echo_test);
extern SUITE(hash_test);
extern SUITE(hlist_test);
extern SUITE(parser_test);
extern SUITE(set_test);
extern SUITE(hstring_test);
extern SUITE(flock_test);
extern SUITE(fifo_test);
extern SUITE(pubsub_test);

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
	GREATEST_MAIN_BEGIN();
	RUN_SUITE(btree_test);
	RUN_SUITE(concurrency_test);
	RUN_SUITE(db_test);
	RUN_SUITE(rlite_test);
	RUN_SUITE(list_test);
	RUN_SUITE(string_test);
	RUN_SUITE(multi_string_test);
	RUN_SUITE(multi_test);
	RUN_SUITE(key_test);
	RUN_SUITE(type_string_test);
	RUN_SUITE(type_list_test);
	RUN_SUITE(type_set_test);
	RUN_SUITE(type_zset_test);
	RUN_SUITE(type_hash_test);
	RUN_SUITE(skiplist_test);
	RUN_SUITE(long_test);
	RUN_SUITE(restore_test);
	RUN_SUITE(hyperloglog_test);
	RUN_SUITE(dump_test);
	RUN_SUITE(sort_test);
	RUN_SUITE(wal_test);
	RUN_SUITE(zset_test);
	RUN_SUITE(hmulti_test);
	RUN_SUITE(hsort_test);
	RUN_SUITE(scripting_test);
	RUN_SUITE(echo_test);
	RUN_SUITE(hash_test);
	RUN_SUITE(hlist_test);
	RUN_SUITE(parser_test);
	RUN_SUITE(set_test);
	RUN_SUITE(hstring_test);
	RUN_SUITE(flock_test);
	RUN_SUITE(fifo_test);
	RUN_SUITE(pubsub_test);
	GREATEST_MAIN_END();
}
