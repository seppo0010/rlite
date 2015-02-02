#ifndef _RL_TYPE_SET_H
#define _RL_TYPE_SET_H

#include "page_btree.h"

#define RL_TYPE_SET 'S'

struct rlite;

typedef rl_btree_iterator rl_set_iterator;

int rl_set_get_objects(struct rlite *db, const unsigned char *key, long keylen, long *_set_page_number, rl_btree **btree, int update_version, int create);
int rl_set_iterator_next(rl_set_iterator *iterator, unsigned char **member, long *memberlen);
int rl_set_iterator_destroy(rl_set_iterator *iterator);

int rl_sadd(struct rlite *db, const unsigned char *key, long keylen, int memberc, unsigned char **members, long *memberslen, long *added);
int rl_sismember(struct rlite *db, const unsigned char *key, long keylen, unsigned char *data, long datalen);
int rl_scard(struct rlite *db, const unsigned char *key, long keylen, long *card);
int rl_srem(struct rlite *db, const unsigned char *key, long keylen, int membersc, unsigned char **members, long *memberslen, long *delcount);
int rl_smove(struct rlite *db, const unsigned char *source, long sourcelen, const unsigned char *destination, long destinationlen, unsigned char *member, long memberlen);
int rl_smembers(struct rlite *db, rl_set_iterator **iterator, const unsigned char *key, long keylen);
int rl_srandmembers(struct rlite *db, const unsigned char *key, long keylen, int repeat, long *memberc, unsigned char ***members, long **memberslen);
int rl_spop(struct rlite *db, const unsigned char *key, long keylen, unsigned char **member, long *memberlen);
int rl_sdiff(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen);
int rl_sdiffstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added);
int rl_sinter(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen);
int rl_sinterstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added);
int rl_sunion(struct rlite *db, int keyc, unsigned char **keys, long *keyslen, long *_membersc, unsigned char ***_members, long **_memberslen);
int rl_sunionstore(struct rlite *db, unsigned char *target, long targetlen, int keyc, unsigned char **keys, long *keyslen, long *added);

int rl_set_pages(struct rlite *db, long page, short *pages);
int rl_set_delete(struct rlite *db, long value_page);

#endif
