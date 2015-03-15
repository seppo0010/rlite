#include <unistd.h>
#include "../src/rlite.h"

int setup_db(rlite **_db, int file, int del)
{
	const char *filepath = "rlite-test.rld";
	const char *wal_filepath = ".rlite-test.rld.wal";
	if (del) {
		if (access(filepath, F_OK) == 0) {
			unlink(filepath);
		}
		if (access(wal_filepath, F_OK) == 0) {
			unlink(wal_filepath);
		}
	}
	rlite *db;
	int retval = rl_open(file == 1 ? filepath : ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE);
	if (retval != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
	} else {
		*_db = db;
	}
	return retval;
}
