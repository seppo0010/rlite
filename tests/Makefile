CFLAGS +=  -I../src/ -I../deps/lua/src/
STLIBNAME=../src/libhirlite.a ../deps/lua/src/liblua.a
OBJS=rlite-test.o btree-test.o list-test.o string-test.o multi_string-test.o multi-test.o key-test.o type_string-test.o type_list-test.o type_set-test.o type_zset-test.o type_hash-test.o skiplist-test.o long-test.o restore-test.o hyperloglog-test.o dump-test.o sort-test.o test_util.o test.o wal-test.o
HIRLITE_OBJS=test_hirlite.o zset.o db.o multi.o hsort.o scripting-test.o concurrency-test.o echo.o hash.o list.o parser.o set.o string.o

gcov: CFLAGS += -fprofile-arcs -ftest-coverage
gcov: clean test

builddebug: CFLAGS += -DDEBUG=1
builddebug: clean buildtest

debug: builddebug vtest

full-lcov: CFLAGS += -fprofile-arcs -ftest-coverage -DDEBUG=1
full-lcov: clean buildtest

buildtest: $(OBJS) $(HIRLITE_OBJS)
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test $(OBJS) $(STLIBNAME) $(LIBS)
	$(CC) $(DEBUG) $(CFLAGS) -o hirlite-test $(HIRLITE_OBJS) $(STLIBNAME) $(LUA_STLIB) $(LIBS)

test: buildtest
	./rlite-test
	./hirlite-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./hirlite-test

clean:
	rm -f *.o rlite-test hirlite-test
