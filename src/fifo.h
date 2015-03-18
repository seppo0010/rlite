#ifndef _RL_FIFO_H
#define _RL_FIFO_H

int rl_create_fifo(const char *fifo_name);
int rl_delete_fifo(const char *fifo_name);
int rl_read_fifo(const char *fifo_name, char **_data, size_t *_datalen);
int rl_write_fifo(const char *fifo_name, const char *data, size_t datalen);

#endif
