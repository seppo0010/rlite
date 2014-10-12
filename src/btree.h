#ifndef _BTREE_H
#define _BTREE_H

struct rl_btree;
struct rl_btree_node;

typedef struct {
	int score_size;
	int value_size;
	int (*serialize)(struct rl_btree *btree, struct rl_btree_node *node, unsigned char **data, long *data_size);
	int (*deserialize)(struct rl_btree *btree, unsigned char *data, struct rl_btree_node **node);
	int (*cmp)(void *v1, void *v2);
	int (*formatter)(void *v, char **str, int *size);
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
	int (*commit)(struct rl_btree *btree);
	int (*discard)(struct rl_btree *btree);
	int (*select)(struct rl_btree *btree, long number, struct rl_btree_node **_node);
	int (*update)(struct rl_btree *btree, long *number, struct rl_btree_node *node);
	int (*insert)(struct rl_btree *btree, long *number, struct rl_btree_node *node);
	int (*remove)(struct rl_btree *btree, struct rl_btree_node *node);
	int (*list)(struct rl_btree *btree, rl_btree_node *** nodes, long *size);
} rl_accessor;

typedef struct rl_btree {
	long max_node_size; // maximum number of scores in a node
	long height;
	rl_btree_type *type;
	long root;
	rl_accessor *accessor;
} rl_btree;

int rl_btree_create(rl_btree **btree, rl_btree_type *type, long max_node_size, rl_accessor *accessor);
int rl_btree_destroy(rl_btree *btree);
int rl_btree_node_destroy(rl_btree *btree, rl_btree_node *node);
int rl_btree_add_element(rl_btree *btree, void *score, void *value);
int rl_btree_remove_element(rl_btree *btree, void *score);
int rl_btree_find_score(rl_btree *btree, void *score, void **values, rl_btree_node *** nodes, long **positions);
int rl_print_btree(rl_btree *btree);
int rl_btree_is_balanced(rl_btree *btree);
int rl_flatten_btree(rl_btree *btree, void *** scores, long *size);

#endif
