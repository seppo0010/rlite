#ifndef _RL_FLOCK_H
#define _RL_FLOCK_H
#include <stdio.h>

int rl_flock(FILE *fp, size_t start, size_t len, int type);
#endif
