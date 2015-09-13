LIBS=-lm -lpthread
CFLAGS +=  -I../src/ -I../deps/lua/src/
STLIBNAME=../src/libhirlite.a ../deps/lua/src/liblua.a
OBJS=hstring-test.o set-test.o parser-test.o hlist-test.o hash-test.o echo-test.o scripting-test.o hsort-test.o hmulti-test.o zset-test.o wal-test.o sort-test.o dump-test.o hyperloglog-test.o restore-test.o long-test.o skiplist-test.o type_hash-test.o type_zset-test.o type_set-test.o type_list-test.o type_string-test.o key-test.o multi-test.o multi_string-test.o string-test.o list-test.o rlite-test.o btree-test.o concurrency-test.o db-test.o signal-test.o flock-test.o pubsub-test.o hpubsub-test.o util.o test.o

CFLAGS.gcc += -std=c99

ifeq ($(shell $(CC) -v 2>&1 | grep -c "clang version"), 1)
DEBUG += ${DEBUG.clang}
CFLAGS += ${CFLAGS.clang}
else
DEBUG += ${DEBUG.gcc}
CFLAGS += ${CFLAGS.gcc}
endif

gcov: CFLAGS += -fprofile-arcs -ftest-coverage
gcov: clean test

builddebug: CFLAGS += -DDEBUG=1
builddebug: clean buildtest

debug: builddebug vtest

full-lcov: CFLAGS += -fprofile-arcs -ftest-coverage -DDEBUG=1
full-lcov: clean buildtest

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

buildtest: $(OBJS)
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test $(OBJS) $(STLIBNAME) $(LIBS)

test: buildtest
	./rlite-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes --suppressions=../.valgrind.supp --error-exitcode=1 ./rlite-test

clean:
	rm -f *.o rlite-test hirlite-test
