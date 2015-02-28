OPTIMIZATION?=-O2

ifeq ($(uname_S),SunOS)
  # make isinf() available
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6 -fPIC
  DEBUG?=-g -ggdb
else
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -fPIC
  DEBUG?=-g -ggdb
endif

DEBUG.gcc += -rdynamic
CFLAGS.gcc += -std=c99 -I./

ifeq ($(shell $(CC) -v 2>&1 | grep -c "clang version"), 1)
DEBUG += ${DEBUG.clang}
CFLAGS += ${CFLAGS.clang}
else
DEBUG += ${DEBUG.gcc}
CFLAGS += ${CFLAGS.gcc}
endif

LIBS=-lm

all: buildtest buildhirlitetest

gcov: CFLAGS += -fprofile-arcs -ftest-coverage
gcov: clean test

builddebug: CFLAGS += -DDEBUG=1
builddebug: clean buildtest

debug: builddebug vtest

lcov: gcov
	mkdir -p lcov
	lcov --directory . --capture --output-file lcov/app.info
	genhtml  lcov/app.info -o lcov/html

full-lcov: CFLAGS += -fprofile-arcs -ftest-coverage -DDEBUG=1
full-lcov: clean buildtest buildhirlitetest

clang-analyzer:
	rm -rf analyzer
	scan-build -o analyzer make

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

libhirlite.a: hirlite.o librlite.a
	ar -cq libhirlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_string.o type_list.o type_set.o type_zset.o type_hash.o page_long.o page_multi_string.o util.o utilfromredis.o hyperloglog.o sha1.o crc64.o restore.o dump.o lzf_c.o lzf_d.o sort.o pqsort.o hirlite.o

librlite.a: rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o page_multi_string.o page_long.o type_string.o type_list.o type_set.o type_zset.o type_hash.o util.o restore.o dump.o sort.o pqsort.o ../deps/utilfromredis.o ../deps/hyperloglog.o ../deps/sha1.o ../deps/crc64.o ../deps/lzf_c.o ../deps/lzf_d.o
	ar -cq librlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_string.o type_list.o type_set.o type_zset.o type_hash.o page_long.o page_multi_string.o util.o utilfromredis.o hyperloglog.o sha1.o crc64.o lzf_c.o lzf_d.o restore.o dump.o sort.o pqsort.o

buildtest: librlite.a test/rlite-test.o test/btree-test.o test/list-test.o test/string-test.o test/multi_string-test.o test/multi-test.o test/key-test.o test/type_string-test.o test/type_list-test.o test/type_set-test.o test/type_zset-test.o test/type_hash-test.o test/skiplist-test.o test/long-test.o test/restore-test.o test/hyperloglog-test.o test/dump-test.o test/sort-test.o test/test_util.o test/test.o
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test test.o rlite-test.o btree-test.o list-test.o string-test.o multi_string-test.o multi-test.o key-test.o type_string-test.o type_list-test.o type_set-test.o type_zset-test.o type_hash-test.o skiplist-test.o long-test.o restore-test.o hyperloglog-test.o dump-test.o sort-test.o test_util.o librlite.a $(LIBS)

buildhirlitetest: libhirlite.a test/echo.o test/hash.o test/list.o test/parser.o test/set.o test/string.o test/test_hirlite.o test/zset.o test/db.o test/multi.o test/sort.o
	$(CC) $(DEBUG) $(CFLAGS) -o hirlite-test echo.o hash.o list.o parser.o set.o string.o test_hirlite.o zset.o db.o multi.o sort.o libhirlite.a $(LIBS)

rlitetest: buildtest
	./rlite-test

hirlitetest: buildhirlitetest
	./hirlite-test

test: rlitetest hirlitetest

vrlitetest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test

vhirlitetest: buildhirlitetest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./hirlite-test

vtest: vrlitetest vhirlitetest

clean:
	rm -rf *-test *.o *.a *.dSYM *.gcda *.gcno lcov
