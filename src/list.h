#ifndef _LIST_H
#define _LIST_H

struct rlite;
struct rl_list;
struct rl_list_node;

typedef struct {
	struct rl_data_type *list_type;
	struct rl_data_type *list_node_type;
	int element_size;
	int (*cmp)(void *v1, void *v2);
	int (*formatter)(void *v, char **str, int *size);
} rl_list_type;

rl_list_type list_long;

typedef struct rl_list_node {
	long size;
	long left;
	long right;
	void **elements;
} rl_list_node;

typedef struct rl_list {
	long max_node_size; // maximum number of elements in a node
	long size;
	rl_list_type *type;
	long left;
	long right;
} rl_list;

void rl_list_init();
int rl_list_create(struct rlite *db, rl_list **_list, rl_list_type *type, long max_size);
int rl_list_destroy(struct rlite *db, void *list);
int rl_list_node_destroy(struct rlite *db, void *node);
int rl_list_add_element(struct rlite *db, rl_list *list, void *element, long position);
int rl_list_remove_element(struct rlite *db, rl_list *list, long position);
int rl_list_find_element(struct rlite *db, rl_list *list, void *element, long *position);
int rl_print_list(struct rlite *db, rl_list *list);
int rl_list_is_balanced(struct rlite *db, rl_list *list);
int rl_flatten_list(struct rlite *db, rl_list *list, void *** elements);

int rl_serialize_list_long(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_list_long(struct rlite *db, void **obj, void *context, unsigned char *data);
int rl_serialize_list_node_long(struct rlite *db, void *obj, unsigned char *data);
int rl_deserialize_list_node_long(struct rlite *db, void **obj, void *context, unsigned char *data);


#endif
