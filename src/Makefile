OPTIMIZATION?=-O2

ifeq ($(uname_S),SunOS)
  # make isinf() available
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  DEBUG?=-g -ggdb 
else
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  DEBUG?=-g -rdynamic -ggdb 
endif
LIBS=-lm -lssl -lcrypto

all: buildtest

gcov: CFLAGS += -fprofile-arcs -ftest-coverage
gcov: clean test

builddebug: CFLAGS += -DDEBUG=1
builddebug: clean buildtest

debug: builddebug vtest

lcov: gcov
	lcov --directory . --capture --output-file lcov/app.info
	genhtml  lcov/app.info -o lcov/html

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

librlite.a: rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o page_multi_string.o type_zset.o util.o
	ar -cq librlite.a rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o type_zset.o page_multi_string.o util.o

rlite-test: test/rlite-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test rlite-test.o test_util.o librlite.a $(LIBS)

btree-test: test/btree-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o btree-test btree-test.o test_util.o librlite.a $(LIBS)

list-test: test/list-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o list-test list-test.o test_util.o librlite.a $(LIBS)

string-test: test/string-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o string-test string-test.o test_util.o librlite.a $(LIBS)

multi_string-test: test/multi_string-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o multi_string-test multi_string-test.o test_util.o librlite.a $(LIBS)

key-test: test/key-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o key-test key-test.o test_util.o librlite.a $(LIBS)

type_zset-test: test/type_zset-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o type_zset-test type_zset-test.o test_util.o librlite.a $(LIBS)

skiplist-test: test/skiplist-test.o test/test_util.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o skiplist-test skiplist-test.o test_util.o librlite.a $(LIBS)

buildtest: rlite-test btree-test list-test string-test multi_string-test key-test type_zset-test skiplist-test

test: buildtest
	./btree-test
	./list-test
	./rlite-test
	./string-test
	./multi_string-test
	./key-test
	./type_zset-test
	./skiplist-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./btree-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./list-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./string-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./multi_string-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./key-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./type_zset-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./skiplist-test

clean:
	rm -rf *-test *.o *.a *.dSYM *.gcda *.gcno
