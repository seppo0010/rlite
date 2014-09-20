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

libredislite.a: btree.o
	ar -cq libredislite.a btree.o

buildtest: redislite-test.o libredislite.a
	$(CC) $(DEBUG) $(CFLAGS) -lm -o redislite-test redislite-test.o libredislite.a

test: buildtest
	./redislite-test

clean:
	rm -rf redislite-test *.o *.a redislite-*.dSYM
