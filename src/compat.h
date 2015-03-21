#ifndef _RL_COMPAT_H
#define _RL_COMPAT_H

#ifdef _WIN32
#include "compat_win.h"
#else
#include "compat_posix.h"
#endif

int rl_file_exists(const char *path);
int rl_file_is_readable(const char *path);
int rl_file_is_writable(const char *path);
unsigned long long rl_mstime();

#endif
