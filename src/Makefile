# Hirlite Makefile
# Copyright (C) 2010-2011 Salvatore Sanfilippo <antirez at gmail dot com>
# Copyright (C) 2010-2011 Pieter Noordhuis <pcnoordhuis at gmail dot com>
# Copyright (C) 2015 Sebastian Waisbrot <seppo0010 at gmail dot com>

# Partially based on hiredis's Makefile
# https://github.com/redis/hiredis/blob/master/Makefile

uname_S:= $(shell sh -c 'uname -s 2>/dev/null || echo not')

OBJ=rlite.o page_skiplist.o page_string.o page_list.o page_btree.o page_key.o page_multi_string.o page_long.o type_string.o type_list.o type_set.o type_zset.o type_hash.o util.o restore.o dump.o sort.o pqsort.o utilfromredis.o hyperloglog.o sha1.o crc64.o lzf_c.o lzf_d.o scripting.o rand.o hirlite.o
LUA_OBJ=../deps/lua/src/lapi.o ../deps/lua/src/lcode.o ../deps/lua/src/ldebug.o ../deps/lua/src/ldo.o ../deps/lua/src/ldump.o ../deps/lua/src/lfunc.o ../deps/lua/src/lgc.o ../deps/lua/src/llex.o ../deps/lua/src/lmem.o ../deps/lua/src/lobject.o ../deps/lua/src/lopcodes.o ../deps/lua/src/lparser.o ../deps/lua/src/lstate.o  ../deps/lua/src/lstring.o ../deps/lua/src/ltable.o ../deps/lua/src/ltm.o ../deps/lua/src/lundump.o ../deps/lua/src/lvm.o ../deps/lua/src/lzio.o ../deps/lua/src/strbuf.o ../deps/lua/src/fpconv.o ../deps/lua/src/lauxlib.o ../deps/lua/src/lbaselib.o ../deps/lua/src/ldblib.o ../deps/lua/src/liolib.o ../deps/lua/src/lmathlib.o ../deps/lua/src/loslib.o ../deps/lua/src/ltablib.o ../deps/lua/src/lstrlib.o ../deps/lua/src/loadlib.o ../deps/lua/src/linit.o ../deps/lua/src/lua_cjson.o ../deps/lua/src/lua_struct.o ../deps/lua/src/lua_cmsgpack.o ../deps/lua/src/lua_bit.o
LIBNAME=libhirlite
PKGCONFNAME=hirlite.pc

HIRLITE_MAJOR=$(shell grep HIRLITE_MAJOR hirlite.h | awk '{print $$3}')
HIRLITE_MINOR=$(shell grep HIRLITE_MINOR hirlite.h | awk '{print $$3}')
HIRLITE_PATCH=$(shell grep HIRLITE_PATCH hirlite.h | awk '{print $$3}')

OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6 -fPIC
  DEBUG?=-g -ggdb
else
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -fPIC
  DEBUG?=-g -ggdb
endif

CFLAGS +=  -I./ -I../deps/lua/src/

DEBUG.gcc += -rdynamic
CFLAGS.gcc += -std=c99

ifeq ($(shell $(CC) -v 2>&1 | grep -c "clang version"), 1)
DEBUG += ${DEBUG.clang}
CFLAGS += ${CFLAGS.clang}
else
DEBUG += ${DEBUG.gcc}
CFLAGS += ${CFLAGS.gcc}
endif

LIBS=-lm

INSTALL?= cp -a
# Installation related variables and target
PREFIX?=/usr/local
INCLUDE_PATH?=include/hirlite
LIBRARY_PATH?=lib
PKGCONF_PATH?=pkgconfig
INSTALL_INCLUDE_PATH=$(DESTDIR)$(PREFIX)/$(INCLUDE_PATH)
INSTALL_LIBRARY_PATH=$(DESTDIR)$(PREFIX)/$(LIBRARY_PATH)
INSTALL_PKGCONF_PATH=$(INSTALL_LIBRARY_PATH)/$(PKGCONF_PATH)

ifeq ($(shell uname -s),Darwin)
  DYLIBSUFFIX=dylib
  DYLIB_MINOR_NAME=$(LIBNAME).$(HIRLITE_MAJOR).$(HIRLITE_MINOR).$(DYLIBSUFFIX)
  DYLIB_MAJOR_NAME=$(LIBNAME).$(HIRLITE_MAJOR).$(DYLIBSUFFIX)
  DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
  DYLIB_MAKE_CMD=$(CC) -shared -Wl,-install_name,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
else
  DYLIBSUFFIX=so
  DYLIB_MINOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(HIRLITE_MAJOR).$(HIRLITE_MINOR)
  DYLIB_MAJOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(HIRLITE_MAJOR)
  DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
  DYLIB_MAKE_CMD=$(CC) -shared -Wl,-soname,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
endif


STLIBSUFFIX=a
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)

all: $(DYLIBNAME) $(STLIBNAME)

gcov: CFLAGS += -fprofile-arcs -ftest-coverage
gcov: clean test

builddebug: CFLAGS += -DDEBUG=1
builddebug: clean buildtest

debug: builddebug vtest

ifeq ($(uname_S),SunOS)
	# Make isinf() available
	LUA_CFLAGS= -D__C99FEATURES__=1
endif
LUA_CFLAGS+= -O2 -Wall -DENABLE_CJSON_GLOBAL -fPIC $(CFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)
LUA_STLIB=../deps/lua/src/liblua.a
AR=ar
ARFLAGS=rcu

lua:
	cd ../deps/lua && $(MAKE) ansi CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)" AR="$(AR) $(ARFLAGS)"

lcov: gcov
	mkdir -p lcov
	lcov --directory . --capture --output-file lcov/app.info
	genhtml  lcov/app.info -o lcov/html

full-lcov: CFLAGS += -fprofile-arcs -ftest-coverage -DDEBUG=1
full-lcov: clean buildtest

clang-analyzer:
	rm -rf analyzer
	scan-build -o analyzer $(MAKE)

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

$(DYLIBNAME): $(OBJ) lua
	$(DYLIB_MAKE_CMD) $(LUA_OBJ) $(OBJ)

$(STLIBNAME): $(OBJ)
	ar -cq libhirlite.a $(OBJ)

buildtest: $(STLIBNAME) ../tests/rlite-test.o ../tests/btree-test.o ../tests/list-test.o ../tests/string-test.o ../tests/multi_string-test.o ../tests/multi-test.o ../tests/key-test.o ../tests/type_string-test.o ../tests/type_list-test.o ../tests/type_set-test.o ../tests/type_zset-test.o ../tests/type_hash-test.o ../tests/skiplist-test.o ../tests/long-test.o ../tests/restore-test.o ../tests/hyperloglog-test.o ../tests/dump-test.o ../tests/sort-test.o ../tests/test_util.o ../tests/test.o ../tests/echo.o ../tests/hash.o ../tests/list.o ../tests/parser.o ../tests/set.o ../tests/string.o ../tests/test_hirlite.o ../tests/zset.o ../tests/db.o ../tests/multi.o ../tests/hsort.o ../tests/scripting-test.o
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test test.o rlite-test.o btree-test.o list-test.o string-test.o multi_string-test.o multi-test.o key-test.o type_string-test.o type_list-test.o type_set-test.o type_zset-test.o type_hash-test.o skiplist-test.o long-test.o restore-test.o hyperloglog-test.o dump-test.o sort-test.o test_util.o $(STLIBNAME) $(LIBS)
	$(CC) $(DEBUG) $(CFLAGS) -o hirlite-test echo.o hash.o list.o parser.o set.o string.o test_hirlite.o zset.o db.o multi.o hsort.o scripting-test.o $(STLIBNAME) $(LUA_STLIB) $(LIBS)

test: buildtest
	./rlite-test
	./hirlite-test

vtest: buildtest
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./rlite-test
	valgrind --track-origins=yes --leak-check=full --show-reachable=yes ./hirlite-test

$(PKGCONFNAME): hirlite.h
	@echo "Generating $@ for pkgconfig..."
	@echo prefix=$(PREFIX) > $@
	@echo exec_prefix=$${prefix} >> $@
	@echo libdir=$(PREFIX)/$(INCLUDE_PATH) >> $@
	@echo includedir=$(PREFIX)/$(LIBRARY_PATH) >> $@
	@echo >> $@
	@echo Name: hirlite >> $@
	@echo Description: rlite database library. >> $@
	@echo Version: $(HIRLITE_MAJOR).$(HIRLITE_MINOR).$(HIRLITE_PATCH) >> $@
	@echo Libs: -L$${libdir} -lhirlite >> $@
	@echo Cflags: -I$${includedir} -D_FILE_OFFSET_BITS=64 >> $@

install: $(DYLIBNAME) $(STLIBNAME) $(PKGCONFNAME)
	mkdir -p $(INSTALL_INCLUDE_PATH) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) *.h $(INSTALL_INCLUDE_PATH)
	$(INSTALL) $(DYLIBNAME) $(INSTALL_LIBRARY_PATH)/$(DYLIB_MINOR_NAME)
	cd $(INSTALL_LIBRARY_PATH) && ln -sf $(DYLIB_MINOR_NAME) $(DYLIB_MAJOR_NAME)
	cd $(INSTALL_LIBRARY_PATH) && ln -sf $(DYLIB_MAJOR_NAME) $(DYLIBNAME)
	$(INSTALL) $(STLIBNAME) $(INSTALL_LIBRARY_PATH)
	mkdir -p $(INSTALL_PKGCONF_PATH)
	$(INSTALL) $(PKGCONFNAME) $(INSTALL_PKGCONF_PATH)

clean:
	rm -rf *-test *.o *.a *.dSYM *.gcda *.gcno lcov *.dylib ./deps/lua/src/*.a
