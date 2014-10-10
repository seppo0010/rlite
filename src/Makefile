OPTIMIZATION?=-O2

ifeq ($(uname_S),SunOS)
  # make isinf() available
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  DEBUG?=-g -ggdb 
else
  CFLAGS?=-std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  DEBUG?=-g -rdynamic -ggdb 
endif

all: buildtest

.c.o:
	$(CC) $(ARCH) $(DEBUG) $(CFLAGS) -c $<

librlite.a: btree.o
	ar -cq librlite.a btree.o

buildtest: btree-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o btree-test btree-test.o librlite.a -lm

test: buildtest
	./btree-test

clean:
	rm -rf btree-test *.o *.a rlite-*.dSYM
