#ifndef _BTREE_H
#define _BTREE_H

#include "rlite.h"

struct rl_data_type;
struct rl_btree;
struct rl_btree_node;

typedef struct {
	struct rl_data_type *btree_type;
	struct rl_data_type *btree_node_type;
	int score_size;
	int value_size;
	int (*cmp)(void *v1, void *v2);
	int (*formatter)(void *v, char **str, int *size);
} rl_btree_type;

int md5_cmp(void *v1, void *v2);
int md5_formatter(void *v, char **str, int *size);

rl_btree_type long_set;
rl_btree_type long_hash;
rl_btree_type btree_hash_md5_long;

void init_long_set();
void init_long_hash();

typedef struct rl_btree_node {
	void **scores;
	// children is null when the node is a leaf
	// when created, allocs size+1.
	long *children;
	void **values;
	// size is the number of children used; allocs the maximum on creation
	long size;
} rl_btree_node;

typedef struct rl_btree {
	struct rlite *db;
	long max_node_size; // maximum number of scores in a node
	long height;
	rl_btree_type *type;
	long root;
} rl_btree;

void rl_btree_init();
int rl_btree_create(struct rlite *db, rl_btree **btree, rl_btree_type *type, long max_node_size);
int rl_btree_destroy(struct rlite *db, void *btree);
int rl_btree_node_destroy(struct rlite *db, void *node);
int rl_btree_add_element(struct rlite *db, rl_btree *btree, void *score, void *value);
int rl_btree_remove_element(struct rlite *db, rl_btree *btree, void *score);
int rl_btree_find_score(struct rlite *db, rl_btree *btree, void *score, void **values, rl_btree_node *** nodes, long **positions);
int rl_print_btree(struct rlite *db, rl_btree *btree);
int rl_btree_is_balanced(struct rlite *db, rl_btree *btree);
int rl_flatten_btree(struct rlite *db, rl_btree *btree, void *** scores, long *size);

int rl_serialize_btree_hash_md5_long(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_btree_hash_md5_long(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_serialize_btree_node_hash_md5_long(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_btree_node_hash_md5_long(struct rlite *db, void **obj, void *context, unsigned char *data);

#endif
