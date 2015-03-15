#include <sys/fcntl.h>
#include <sys/file.h>

#include <errno.h>

#include "rlite.h"

int rl_flock(FILE *fp, size_t UNUSED(start), size_t UNUSED(len), int type) {
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
/*
 * Ideally, we only lock the block of the files we need. Some polishing seems
 * to be needed for this to work.
 * Update: `fcntl` provides the same lock for two file descriptor in the same
 * process, which defeats multi-thread safety. The current implementation
 * uses file locks, which might be slower, but it is safer.
int rl_flock(FILE *fp, size_t start, size_t len, int type) {
	struct flock flck;

	int fd = fileno(fp);
	flck.l_start = start;
	flck.l_len = len;
	flck.l_whence = SEEK_SET;
	if (type == RLITE_FLOCK_SH) {
		flck.l_type = F_RDLCK;
	} else if (type == RLITE_FLOCK_EX) {
		flck.l_type = F_WRLCK;
	} else if (type == RLITE_FLOCK_UN) {
		flck.l_type = F_UNLCK;
	} else {
		return RL_UNEXPECTED;
	}

	int ret = fcntl(fd, F_SETLKW, &flck);
	return ret == -1 ? RL_UNEXPECTED : RL_OK;
}
*/
