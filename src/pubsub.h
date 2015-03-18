#ifndef _RL_PUBSUB_H
#define _RL_PUBSUB_H

#include "rlite.h"

int rl_subscribe(struct rlite *db, unsigned char *channel, size_t channellen, char **data, size_t *datalen);
int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen);

#endif
