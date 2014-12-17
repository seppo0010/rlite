OPTIMIZATION?=-O2

ifeq ($(uname_S),SunOS)
  # make isinf() available
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  DEBUG?=-g -ggdb
else
  CFLAGS?= -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  DEBUG?=-g -ggdb
endif

CFLAGS += -I../deps/rlite/src/ -I./

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

all: libhirlite.a

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

libhirlite.a: hirlite.o
	cd ../deps/rlite && make librlite.a
	ar -cq libhirlite.a hirlite.o

buildtest: libhirlite.a test/echo.o test/zset.o test/test.o
	 $(CC) $(DEBUG) $(CFLAGS) -o hirlite-test echo.o zset.o test.o libhirlite.a ../deps/rlite/src/librlite.a $(LIBS)

test: buildtest
	./hirlite-test

vtest: buildtest
	valgrind --leak-check=full --track-origins=yes --show-reachable=yes ./hirlite-test

clean:
	rm -f *.o *.a

distclean: clean
	cd ../deps/rlite && make clean
