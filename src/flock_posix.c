#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>

#include "rlite/rlite.h"

int rl_flock(FILE *fp, int type)
{
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
	// all documented error codes for flock do not apply
	// EWOULDBLOCK because we are not using LOCK_NB
	// ENOTSUP, EBADF and EINVAL because we received the fileno from a FILE*
	if (flock(fd, locktype) == 0) {
		return RL_OK;
	} else {
		return RL_UNEXPECTED;
	}
}

int rl_is_flocked(const char *path, int type)
{
	int retval, oflags = O_NONBLOCK;
	int locktype;

	if (type == RLITE_FLOCK_SH) {
		oflags |= O_RDONLY;
		// we want to know if someone has a shared lock
		// if we can get an exclusive, we know they do not
		// if we cannot, we will try to get a shared to discard someone
		// having an exclusive one
		locktype = LOCK_EX;
	} else if (type == RLITE_FLOCK_EX) {
		oflags |= O_WRONLY;
		// we'll get a shared lock iff nobody has an exclusive lock
		locktype = LOCK_SH;
	} else {
		return RL_UNEXPECTED;
	}
	int fd = open(path, oflags);
	if (fd == -1) {
		// if the file does not exist, the lock is not found
		if (errno == ENOENT || errno == EACCES) {
			return RL_NOT_FOUND;
		}
		return RL_UNEXPECTED;
	}

	retval = flock(fd, locktype | LOCK_NB);
	if (retval == 0) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	if (errno == EWOULDBLOCK) {
		if (type == RLITE_FLOCK_SH) {
			retval = flock(fd, LOCK_SH | LOCK_NB);
			if (retval == 0) {
				// can get a shared lock but not an exclusive
				// thus someone has a shared lock
				retval = RL_FOUND;
				goto cleanup;
			} else {
				retval = RL_NOT_FOUND;
				goto cleanup;
			}
		}
		retval = RL_FOUND;
		goto cleanup;
	}
	retval = RL_UNEXPECTED;
cleanup:
	close(fd);
	return retval;
}
