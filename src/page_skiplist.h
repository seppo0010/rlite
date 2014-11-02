#ifndef _RL_PAGE_SKIPLIST_H
#define _RL_PAGE_SKIPLIST_H

#include "rlite.h"

#define RL_SKIPLIST_MAXLEVEL 32
#define RL_SKIPLIST_PROBABILITY 0.25

struct rl_data_type;

typedef struct {
	long value;
	double score;
	long left;
	long num_levels;
	struct rl_skiplist_node_level {
		long right;
		long span;
	} level[];
} rl_skiplist_node;

typedef struct {
	long left;
	long right;
	long size;
	long level;
} rl_skiplist;

int rl_skiplist_create(rlite *db, rl_skiplist **skiplist);
int rl_skiplist_destroy(rlite *db, void *skiplist);
int rl_skiplist_node_create(rlite *db, rl_skiplist_node **_node, long level, double score, long value);
int rl_skiplist_node_destroy(rlite *db, void *node);
int rl_skiplist_add(rlite *db, rl_skiplist *skiplist, double score, long value);
int rl_skiplist_first_node(rlite *db, rl_skiplist *skiplist, double score, rl_skiplist_node **node, long *rank);

int rl_skiplist_is_balanced(rlite *db, rl_skiplist *skiplist);
#ifdef DEBUG
int rl_skiplist_print(rlite *db, rl_skiplist *skiplist);
#endif

int rl_skiplist_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_skiplist_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);

int rl_skiplist_node_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_skiplist_node_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);

#endif
