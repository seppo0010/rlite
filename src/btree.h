#ifndef _BTREE_H
#define _BTREE_H

typedef struct {
	int score_size;
	int value_size;
	void *(*score_create)(void *btree);
	void (*score_destroy)(void *btree, void *score);
	long (*serialize_length)(void *btree);
	long (*serialize)(void *btree, void *node, unsigned char **data, long *data_size);
	long (*deserialize)(void *btree, unsigned char *data, void **node);
	int (*cmp)(void *v1, void *v2);
	void (*formatter)(void *v, char **str, int *size);
} rl_btree_type;

rl_btree_type long_set;
rl_btree_type long_hash;

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

typedef struct rl_accessor {
	void *context;
	long (*commit)(void *btree);
	long (*discard)(void *btree);
	void *(*select)(void *btree, long number);
	long (*update)(void *btree, long *number, void *node);
	long (*insert)(void *btree, long *number, void *node);
	long (*remove)(void *btree, void *node);
	long (*list)(void *btree, rl_btree_node *** nodes, long *size);
} rl_accessor;

typedef struct {
	long max_size; // maximum number of scores in a node
	long height;
	rl_btree_type *type;
	long root;
	rl_accessor *accessor;
} rl_btree;

rl_btree *rl_btree_create(rl_btree_type *type, long max_size, rl_accessor *accessor);
int rl_btree_destroy(rl_btree *btree);
long rl_btree_node_destroy(rl_btree *btree, rl_btree_node *node);
int rl_btree_add_element(rl_btree *btree, void *score, void *value);
int rl_btree_remove_element(rl_btree *btree, void *score);
long rl_btree_find_score(rl_btree *btree, void *score, rl_btree_node *** nodes, long **positions);
void rl_print_btree(rl_btree *btree);
int rl_btree_is_balanced(rl_btree *btree);
void rl_flatten_btree(rl_btree *btree, void *** scores, long *size);

#endif
