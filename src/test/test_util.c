#include <unistd.h>
#include "../rlite.h"

int setup_db(rlite **_db, int file, int del)
{
	const char *filepath = "rlite-test.rld";
	if (del && access(filepath, F_OK) == 0) {
		unlink(filepath);
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
