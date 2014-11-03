#define RL_TYPE_ZSET 'Z'

struct rlite;

int rl_zadd(struct rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen);
int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, double *score);
int rl_zrank(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, long *rank);
int rl_zcard(rlite *db, unsigned char *key, long keylen, long *card);
