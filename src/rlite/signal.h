#ifndef _RL_FIFO_H
#define _RL_FIFO_H

#include <sys/time.h>

int rl_create_signal(const char *signal_name);
int rl_delete_signal(const char *signal_name);
int rl_read_signal(const char *signal_name, struct timeval *timeout, char **_data, size_t *_datalen);
int rl_write_signal(const char *signal_name, const char *data, size_t datalen);

#endif
