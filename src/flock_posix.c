#include <sys/fcntl.h>
#include <sys/file.h>

#include <errno.h>

#include "rlite.h"

int rl_flock(FILE *fp, int type) {
	int fd = fileno(fp);
	int locktype;
	if (type == RLITE_FLOCK_SH) {
		locktype = LOCK_SH;
	} else if (type == RLITE_FLOCK_EX) {
		locktype = LOCK_EX;
	} else if (type == RLITE_FLOCK_UN) {
		locktype = LOCK_UN;
	} else {
		return RL_UNEXPECTED;
	}
	flock(fd, locktype);
	return RL_OK;
}
