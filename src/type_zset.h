#define RL_TYPE_ZSET 'Z'

struct rlite;

typedef struct {
	double min;
	int minex;
	double max;
	int maxex;
} rl_zrangespec;

int rl_zadd(struct rlite *db, unsigned char *key, long keylen, double score, unsigned char *data, long datalen);
int rl_zscore(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, double *score);
int rl_zrank(rlite *db, unsigned char *key, long keylen, unsigned char *data, long datalen, long *rank);
int rl_zcard(rlite *db, unsigned char *key, long keylen, long *card);
int rl_zrange(rlite *db, unsigned char *key, long keylen, long min, long max, unsigned char ***data, long **datalen, double **scores, long *size);
