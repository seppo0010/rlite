#include "compat.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>

int rl_file_exists(const char *path)
{
	return access(path, F_OK) == 0;
}

int rl_file_is_readable(const char *path)
{
	return access(path, R_OK | F_OK) == 0;
}

int rl_file_is_writable(const char *path)
{
	return access(path, W_OK) == 0;
}

unsigned long long rl_mstime()
{
	struct timeval tp;
	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

