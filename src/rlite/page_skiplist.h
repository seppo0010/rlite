#ifndef _RL_PAGE_SKIPLIST_H
#define _RL_PAGE_SKIPLIST_H

#include "rlite.h"

#define RL_SKIPLIST_MAXLEVEL 32
#define RL_SKIPLIST_PROBABILITY 0.25

#define RL_SKIPLIST_BEFORE_SCORE 1
#define RL_SKIPLIST_UPTO_SCORE 2
#define RL_SKIPLIST_INCLUDE_SCORE 3
#define RL_SKIPLIST_EXCLUDE_SCORE 4

struct rlite;
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

typedef struct rl_skiplist_iterator {
	struct rlite *db;
	rl_skiplist *skiplist;
	long node_page;
	int direction; // 1 for right, -1 for left
	int position;
	long size;
} rl_skiplist_iterator;

int rl_skiplist_create(struct rlite *db, rl_skiplist **skiplist);
int rl_skiplist_destroy(struct rlite *db, void *skiplist);
int rl_skiplist_node_create(struct rlite *db, rl_skiplist_node **_node, long level, double score, long value);
int rl_skiplist_node_destroy(struct rlite *db, void *node);
int rl_skiplist_iterator_create(struct rlite *db, rl_skiplist_iterator **iterator, rl_skiplist *skiplist, long node_page, int direction, int size);
int rl_skiplist_iterator_destroy(struct rlite *db, rl_skiplist_iterator *iterator);
int rl_skiplist_iterator_next(rl_skiplist_iterator *iterator, rl_skiplist_node **node);
int rl_skiplist_add(struct rlite *db, rl_skiplist *skiplist, long skiplist_page, double score, unsigned char *value, long valuelen);
int rl_skiplist_first_node(struct rlite *db, rl_skiplist *skiplist, double score, int range_mode, unsigned char *value, long valuelen, rl_skiplist_node **node, long *rank);
int rl_skiplist_node_by_rank(struct rlite *db, rl_skiplist *skiplist, long rank, rl_skiplist_node **node, long *node_page);
int rl_skiplist_delete(struct rlite *db, rl_skiplist *skiplist, long skiplist_page, double score, unsigned char *value, long valuelen);
int rl_skiplist_delete_all(struct rlite *db, rl_skiplist *skiplist);

int rl_skiplist_is_balanced(struct rlite *db, rl_skiplist *skiplist);
#ifdef RL_DEBUG
int rl_skiplist_print(struct rlite *db, rl_skiplist *skiplist);
#endif

int rl_skiplist_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_skiplist_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);

int rl_skiplist_node_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_skiplist_node_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);

int rl_skiplist_pages(struct rlite *db, rl_skiplist *skiplist, short *pages);


#endif
