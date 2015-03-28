#ifndef _RL_PAGE_LIST_H
#define _RL_PAGE_LIST_H

struct rlite;
struct rl_list;
struct rl_list_node;

#define rl_list_nocache_destroy(db, list) rl_list_destroy(db, list)
#define rl_list_node_nocache_destroy(db, node) rl_list_node_destroy(db, node)

typedef struct {
	struct rl_data_type *list_type;
	struct rl_data_type *list_node_type;
	int element_size;
	int (*cmp)(void *v1, void *v2);
#ifdef RL_DEBUG
	int (*formatter)(void *v, char **str, int *size);
#endif
} rl_list_type;

extern rl_list_type rl_list_type_long;

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

typedef struct rl_list_iterator {
	struct rlite *db;
	rl_list *list;
	rl_list_node *node;
	int direction; // 1 for right, -1 for left
	long node_position;
} rl_list_iterator;

void rl_list_init();
int rl_list_create(struct rlite *db, rl_list **_list, rl_list_type *type);
int rl_list_destroy(struct rlite *db, void *list);
int rl_list_node_destroy(struct rlite *db, void *node);
int rl_list_get_element(struct rlite *db, rl_list *list, void **element, long position);
int rl_list_add_element(struct rlite *db, rl_list *list, long list_page, void *element, long position);
int rl_list_remove_element(struct rlite *db, rl_list *list, long list_page, long position);
int rl_list_find_element(struct rlite *db, rl_list *list, void *element, void **found_element, long *position, rl_list_node **found_node, long *found_node_page);
int rl_list_iterator_create(struct rlite *db, rl_list_iterator **iterator, rl_list *list, int direction);
int rl_list_iterator_destroy(struct rlite *db, rl_list_iterator *iterator);
int rl_list_iterator_next(rl_list_iterator *iterator, void **element);
int rl_print_list(struct rlite *db, rl_list *list);
int rl_list_is_balanced(struct rlite *db, rl_list *list);
int rl_flatten_list(struct rlite *db, rl_list *list, void **elements);

int rl_list_serialize(struct rlite *db, void *obj, unsigned char *data);
int rl_list_deserialize(struct rlite *db, void **obj, void *context, unsigned char *data);

int rl_list_node_serialize_long(struct rlite *db, void *obj, unsigned char *data);
int rl_list_node_deserialize_long(struct rlite *db, void **obj, void *context, unsigned char *data);

int rl_list_pages(struct rlite *db, rl_list *list, short *pages);
int rl_list_delete(struct rlite *db, rl_list *list);

#endif
