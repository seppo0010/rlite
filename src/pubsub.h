#ifndef _RL_PUBSUB_H
#define _RL_PUBSUB_H

#include "rlite.h"

int rl_subscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen, char **data, size_t *datalen);
int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen);

#endif
