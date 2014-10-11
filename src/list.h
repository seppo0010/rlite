#ifndef _LIST_H
#define _LIST_H

typedef struct {
	int element_size;
	long (*serialize_length)(void *list);
	long (*serialize)(void *list, void *node, unsigned char **data, long *data_size);
	long (*deserialize)(void *list, unsigned char *data, void **node);
	int (*cmp)(void *v1, void *v2);
	void (*formatter)(void *v, char **str, int *size);
} rl_list_type;

rl_list_type long_list;

void init_long_list();

typedef struct {
	long size;
	long left;
	long right;
	void **elements;
} rl_list_node;

typedef struct rl_accessor {
	void *context;
	long (*commit)(void *list);
	long (*discard)(void *list);
	void *(*select)(void *list, long number);
	long (*update)(void *list, long *number, void *node);
	long (*insert)(void *list, long *number, void *node);
	long (*remove)(void *list, void *node);
	long (*list)(void *list, rl_list_node *** nodes, long *size);
} rl_accessor;

typedef struct {
	long max_node_size; // maximum number of elements in a node
	long size;
	rl_list_type *type;
	long left;
	long right;
	rl_accessor *accessor;
} rl_list;

rl_list *rl_list_create(rl_list_type *type, long max_size, rl_accessor *accessor);
int rl_list_destroy(rl_list *list);
long rl_list_node_destroy(rl_list *list, rl_list_node *node);
int rl_list_add_element(rl_list *list, void *element, long position);
int rl_list_remove_element(rl_list *list, long position);
int rl_list_find_element(rl_list *list, void *element, long *position);
void rl_print_list(rl_list *list);
int rl_list_is_balanced(rl_list *list);
void rl_flatten_list(rl_list *list, void *** elements);

#endif
