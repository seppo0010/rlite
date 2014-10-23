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

librlite.a: rlite.o page_string.o page_list.o page_btree.o util.o
	ar -cq librlite.a rlite.o page_string.o page_list.o page_btree.o util.o

rlite-test: rlite-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o rlite-test rlite-test.o librlite.a $(LIBS)

btree-test: btree-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o btree-test btree-test.o librlite.a $(LIBS)

list-test: list-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o list-test list-test.o librlite.a $(LIBS)

string-test: string-test.o librlite.a
	$(CC) $(DEBUG) $(CFLAGS) -o string-test string-test.o librlite.a $(LIBS)

buildtest: rlite-test btree-test list-test string-test

test: buildtest
	./btree-test && ./list-test && ./rlite-test && ./string-test

clean:
	rm -rf *-test *.o *.a *.dSYM
