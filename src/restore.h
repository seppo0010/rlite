#include "rlite.h"

typedef struct rl_restore_streamer {
	void *context;
	int (*read)(struct rl_restore_streamer *streamer, unsigned char *str, long len);
} rl_restore_streamer;

int rl_restore(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, unsigned char *data, long datalen);
int rl_restore_stream(struct rlite *db, const unsigned char *key, long keylen, unsigned long long expires, rl_restore_streamer *streamer);
