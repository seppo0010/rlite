OPTIMIZATION?=-O2

ifeq ($(uname_S),SunOS)
  # make isinf() available
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  DEBUG?=-g -ggdb
else
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  DEBUG?=-g -ggdb
endif

DEBUG.gcc += -rdynamic
CFLAGS.gcc += -std=c99 -I./ -I../hirlite/

ifeq ($(shell $(CC) -v 2>&1 | grep -c "clang version"), 1)
DEBUG += ${DEBUG.clang}
CFLAGS += ${CFLAGS.clang}
else
DEBUG += ${DEBUG.gcc}
CFLAGS += ${CFLAGS.gcc}
endif

LIBS=-lm

all: buildtest

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
full-lcov: clean buildtest

clang-analyzer:
	rm -rf analyzer
	scan-build -o analyzer make

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

libhirlite.a: ../hirlite/hirlite.o librlite.a
	ar -cq libhirlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_string.o type_list.o type_set.o type_zset.o type_hash.o page_long.o page_multi_string.o util.o utilfromredis.o sha1.o hirlite.o

librlite.a: rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o page_multi_string.o page_long.o type_string.o type_list.o type_set.o type_zset.o type_hash.o util.o ../deps/utilfromredis.o ../deps/sha1.o
	ar -cq librlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_string.o type_list.o type_set.o type_zset.o type_hash.o page_long.o page_multi_string.o util.o utilfromredis.o sha1.o

buildtest: librlite.a test/rlite-test.o test/btree-test.o test/list-test.o test/string-test.o test/multi_string-test.o test/key-test.o test/type_string-test.o test/type_list-test.o test/type_set-test.o test/type_zset-test.o test/type_hash-test.o test/skiplist-test.o test/long-test.o test/test_util.o test/test.o
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test test.o rlite-test.o btree-test.o list-test.o string-test.o multi_string-test.o key-test.o type_string-test.o type_list-test.o type_set-test.o type_zset-test.o type_hash-test.o skiplist-test.o long-test.o test_util.o librlite.a $(LIBS)

buildhirlitetest: libhirlite.a ../hirlite/test/echo.o ../hirlite/test/hash.o ../hirlite/test/list.o ../hirlite/test/parser.o ../hirlite/test/set.o ../hirlite/test/string.o ../hirlite/test/test.o ../hirlite/test/zset.o
	$(CC) $(DEBUG) $(CFLAGS) -o hirlite-test echo.o hash.o list.o parser.o set.o string.o test.o zset.o libhirlite.a $(LIBS)

test: buildtest buildhirlitetest
	./rlite-test
	./hirlite-test

vtest: buildtest buildhirlitetest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./hirlite-test

clean:
	rm -rf *-test *.o *.a *.dSYM *.gcda *.gcno lcov
