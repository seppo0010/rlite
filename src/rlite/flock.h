#ifndef _RL_FLOCK_H
#define _RL_FLOCK_H
#include <stdio.h>

int rl_flock(FILE *fp, int type);
int rl_is_flocked(const char *path, int type);

#endif
