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

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

librlite.a: rlite.o list.o btree.o util.o
	ar -cq librlite.a rlite.o list.o btree.o util.o

buildtest: rlite-test.o list-test.o btree-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o btree-test btree-test.o librlite.a $(LIBS)
	$(CC) $(DEBUG) $(CFLAGS) -o list-test list-test.o librlite.a $(LIBS)
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test rlite-test.o librlite.a $(LIBS)

test: buildtest
	./btree-test && ./list-test && ./rlite-test

clean:
	rm -rf *-test *.o *.a *.dSYM
