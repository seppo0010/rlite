#ifndef _RL_PUBSUB_H
#define _RL_PUBSUB_H

#include <sys/time.h>
#include "rlite.h"

int rl_subscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen);
int rl_psubscribe(rlite *db, int patternc, unsigned char **patternv, long *patternvlen);
int rl_unsubscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen);
int rl_unsubscribe_all(rlite *db);
int rl_poll(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen);
/**
 * When timeout is NULL, it will wait for ever.
 */
int rl_poll_wait(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen, struct timeval *timeout);
int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen, long *recipients);

#endif
