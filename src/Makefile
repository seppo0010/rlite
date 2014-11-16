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
CFLAGS.gcc += -std=c99

ifeq ($(shell $(CC) -v 2>&1 | grep -c "clang version"), 1)
DEBUG += ${DEBUG.clang}
CFLAGS += ${CFLAGS.clang}
else
DEBUG += ${DEBUG.gcc}
CFLAGS += ${CFLAGS.gcc}
endif

LIBS=-lm -lssl -lcrypto

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

librlite.a: rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o page_multi_string.o type_zset.o util.o
	ar -cq librlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_zset.o page_multi_string.o util.o

buildtest: librlite.a test/rlite-test.o test/btree-test.o test/list-test.o test/string-test.o test/multi_string-test.o test/key-test.o test/type_zset-test.o test/skiplist-test.o test/test_util.o test/test.o
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test test.o rlite-test.o btree-test.o list-test.o string-test.o multi_string-test.o key-test.o type_zset-test.o skiplist-test.o test_util.o librlite.a $(LIBS)

test: buildtest
	./rlite-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test

clean:
	rm -rf *-test *.o *.a *.dSYM *.gcda *.gcno lcov
