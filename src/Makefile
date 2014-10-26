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

debug: CFLAGS += -DDEBUG=1
debug: clean vtest

lcov: gcov
	lcov --directory . --capture --output-file lcov/app.info
	genhtml  lcov/app.info -o lcov/html

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

librlite.a: rlite.o page_string.o page_list.o page_btree.o obj_key.o obj_string.o util.o
	ar -cq librlite.a rlite.o page_string.o page_list.o page_btree.o obj_key.o obj_string.o util.o

rlite-test: test/rlite-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test rlite-test.o librlite.a $(LIBS)

btree-test: test/btree-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o btree-test btree-test.o librlite.a $(LIBS)

list-test: test/list-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o list-test list-test.o librlite.a $(LIBS)

string-test: test/string-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o string-test string-test.o librlite.a $(LIBS)

obj_string-test: test/obj_string-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o obj_string-test obj_string-test.o librlite.a $(LIBS)

obj_key-test: test/obj_key-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o obj_key-test obj_key-test.o librlite.a $(LIBS)

buildtest: rlite-test btree-test list-test string-test obj_string-test obj_key-test

test: buildtest
	./btree-test
	./list-test
	./rlite-test
	./string-test
	./obj_string-test
	./obj_key-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./btree-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./list-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./string-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./obj_string-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./obj_key-test

clean:
	rm -rf *-test *.o *.a *.dSYM
