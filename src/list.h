#ifndef _LIST_H
#define _LIST_H

struct rl_list;
struct rl_list_node;

typedef struct {
	int element_size;
	int (*serialize)(struct rl_list *list, struct rl_list_node *node, unsigned char **data, long *data_size);
	int (*deserialize)(struct rl_list *list, unsigned char *data, struct rl_list_node **node);
	int (*cmp)(void *v1, void *v2);
	int (*formatter)(void *v, char **str, int *size);
} rl_list_type;

rl_list_type long_list;

void init_long_list();

typedef struct rl_list_node {
	long size;
	long left;
	long right;
	void **elements;
} rl_list_node;

typedef struct rl_accessor {
	void *context;
	int (*commit)(struct rl_list *list);
	int (*discard)(struct rl_list *list);
	int (*select)(struct rl_list *list, long number, struct rl_list_node **_node);
	int (*update)(struct rl_list *list, long *number, struct rl_list_node *node);
	int (*insert)(struct rl_list *list, long *number, struct rl_list_node *node);
	int (*remove)(struct rl_list *list, struct rl_list_node *node);
	int (*list)(struct rl_list *list, rl_list_node *** nodes, long *size);
} rl_accessor;

typedef struct rl_list {
	long max_node_size; // maximum number of elements in a node
	long size;
	rl_list_type *type;
	long left;
	long right;
	rl_accessor *accessor;
} rl_list;

int rl_list_create(rl_list **_list, rl_list_type *type, long max_size, rl_accessor *accessor);
int rl_list_destroy(rl_list *list);
int rl_list_node_destroy(rl_list *list, rl_list_node *node);
int rl_list_add_element(rl_list *list, void *element, long position);
int rl_list_remove_element(rl_list *list, long position);
int rl_list_find_element(rl_list *list, void *element, long *position);
int rl_print_list(rl_list *list);
int rl_list_is_balanced(rl_list *list);
int rl_flatten_list(rl_list *list, void *** elements);

#endif
