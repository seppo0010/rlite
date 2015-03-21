#ifndef _RL_PUBSUB_H
#define _RL_PUBSUB_H

#include "rlite.h"

int rl_subscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen);
int rl_poll(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen);
int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen, long *recipients);

#endif
