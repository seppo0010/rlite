#ifndef _RL_WAL_H
#define _RL_WAL_H

int rl_write_apply_wal(rlite *db);
int rl_write_wal(const char *wal_path, rlite *db, unsigned char **_data, size_t *_datalen);
int rl_apply_wal(rlite *db);

#endif
