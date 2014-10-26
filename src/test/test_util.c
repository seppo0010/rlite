#include <unistd.h>
#include "../rlite.h"

rlite *setup_db(int file)
{
	const char *filepath = "rlite-test.rld";
	if (access(filepath, F_OK) == 0) {
		unlink(filepath);
	}
	rlite *db;
	if (rl_open(file ? filepath : ":memory:", &db, RLITE_OPEN_READWRITE | RLITE_OPEN_CREATE) != RL_OK) {
		fprintf(stderr, "Unable to open rlite\n");
		return NULL;
	}
	return db;
}
