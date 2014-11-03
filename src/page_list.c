#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "rlite.h"
#include "status.h"
#include "page_list.h"
#include "page_string.h"
#include "page_key.h"
#include "util.h"

#ifdef DEBUG
int rl_print_list_node(rl_list *list, rl_list_node *node);
#endif
int rl_list_node_create(rlite *db, rl_list *list, rl_list_node **node);

rl_list_type list_long = {
	0,
	0,
	sizeof(long),
	long_cmp,
#ifdef DEBUG
	long_formatter,
#endif
};

rl_list_type list_key = {
	0,
	0,
	sizeof(rl_key),
	key_cmp,
#ifdef DEBUG
	0,
#endif
};

int rl_list_serialize(rlite *db, void *obj, unsigned char *data)
{
	db = db;
	rl_list *list = obj;
	put_4bytes(data, list->left);
	put_4bytes(&data[4], list->right);
	put_4bytes(&data[8], list->max_node_size);
	put_4bytes(&data[12], list->size);
	return RL_OK;
}

int rl_list_deserialize(rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_list *list;
	int retval = rl_list_create(db, &list, context);
	if (retval != RL_OK) {
		return retval;
	}
	list->type = context;
	list->left = get_4bytes(data);
	list->right = get_4bytes(&data[4]);
	list->max_node_size = get_4bytes(&data[8]);
	list->size = get_4bytes(&data[12]);
	*obj = list;
	return retval;
}

int rl_list_node_serialize_long(rlite *db, void *obj, unsigned char *data)
{
	db = db;
	rl_list_node *node = obj;

	int retval = RL_OK;
	if (!data) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	put_4bytes(data, node->size);
	put_4bytes(&data[4], node->left);
	put_4bytes(&data[8], node->right);
	long i, pos = 12;
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->elements[i]));
		pos += 4;
	}
cleanup:
	return retval;
}

int rl_list_node_deserialize_long(rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_list *list = context;
	rl_list_node *node;
	int retval = rl_list_node_create(db, list, &node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node->size = (long)get_4bytes(data);
	node->left = (long)get_4bytes(&data[4]);
	node->right = (long)get_4bytes(&data[8]);
	long i, pos = 12;
	for (i = 0; i < node->size; i++) {
		node->elements[i] = malloc(sizeof(long));
		*(long *)node->elements[i] = get_4bytes(&data[pos]);
		pos += 4;
	}
	*obj = node;
cleanup:
	return retval;
}

int rl_list_node_serialize_key(rlite *db, void *obj, unsigned char *data)
{
	db = db;
	rl_list_node *node = obj;
	rl_key *key;

	int retval = RL_OK;
	if (!data) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	put_4bytes(data, node->size);
	put_4bytes(&data[4], node->left);
	put_4bytes(&data[8], node->right);
	long i, pos = 12;
	for (i = 0; i < node->size; i++) {
		key = node->elements[i];
		data[pos] = key->type;
		put_4bytes(&data[pos + 1], key->string_page);
		put_4bytes(&data[pos + 5], key->value_page);
		pos += 9;
	}
cleanup:
	return retval;
}

int rl_list_node_deserialize_key(rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_list *list = context;
	rl_list_node *node;
	rl_key *key;
	int retval = rl_list_node_create(db, list, &node);
	if (retval != RL_OK) {
		goto cleanup;
	}
	node->size = (long)get_4bytes(data);
	node->left = (long)get_4bytes(&data[4]);
	node->right = (long)get_4bytes(&data[8]);
	long i, pos = 12;
	for (i = 0; i < node->size; i++) {
		key = malloc(sizeof(rl_key));
		key->type = data[pos];
		key->string_page = get_4bytes(&data[pos + 1]);
		key->value_page = get_4bytes(&data[pos + 5]);
		node->elements[i] = key;
		pos += 9;
	}
	*obj = node;
cleanup:
	return retval;
}

void rl_list_init()
{
	list_long.list_type = &rl_data_type_list_long;
	list_long.list_node_type = &rl_data_type_list_node_long;
	list_key.list_type = &rl_data_type_list_key;
	list_key.list_node_type = &rl_data_type_list_node_key;
}

int rl_list_node_create(rlite *db, rl_list *list, rl_list_node **_node)
{
	db = db;
	rl_list_node *node = malloc(sizeof(rl_list_node));
	if (!node) {
		return RL_OUT_OF_MEMORY;
	}
	node->elements = malloc(sizeof(void *) * list->max_node_size);
	if (!node->elements) {
		free(node);
		return RL_OUT_OF_MEMORY;
	}
	node->size = 0;
	*_node = node;
	return RL_OK;
}

int rl_list_node_destroy(rlite *db, void *_node)
{
	db = db;
	rl_list_node *node = _node;
	long i;
	if (node->elements) {
		for (i = 0; i < node->size; i++) {
			free(node->elements[i]);
		}
		free(node->elements);
	}
	free(node);
	return 0;
}

int rl_list_create(rlite *db, rl_list **_list, rl_list_type *type)
{
	rl_list *list = malloc(sizeof(rl_list));
	if (!list) {
		return RL_OUT_OF_MEMORY;
	}
	int retval;
	list->max_node_size = (db->page_size - 12) / type->element_size;
	list->type = type;
	rl_list_node *node;
	retval = rl_list_node_create(db, list, &node);
	if (retval != RL_OK) {
		free(list);
		goto cleanup;
	}
	node->left = 0;
	node->right = 0;
	list->size = 0;
	list->left = db->next_empty_page;
	retval = rl_write(db, list->type->list_node_type, db->next_empty_page, node);
	if (retval != RL_OK) {
		free(node);
		free(list);
		goto cleanup;
	}
	list->right = list->left;
	*_list = list;
cleanup:
	return RL_OK;
}

int rl_list_destroy(rlite *db, void *list)
{
	db = db;
	free(list);
	return RL_OK;
}

int rl_list_find_element(rlite *db, rl_list *list, void *element, void **found_element, long *position, rl_list_node **found_node, long *found_node_page)
{
	if (!list->type->cmp) {
		fprintf(stderr, "Trying to find an element without cmp\n");
		return RL_UNEXPECTED;
	}
	void *_node;
	rl_list_node *node;
	long pos = 0, i, number = list->left;
	int retval = RL_OK;
	while (number != 0) {
		retval = rl_read(db, list->type->list_node_type, number, list, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		for (i = 0; i < node->size; i++) {
			if (list->type->cmp(node->elements[i], element) == 0) {
				if (found_node_page) {
					*found_node_page = number;
				}
				if (found_node) {
					*found_node = node;
				}
				if (found_element) {
					*found_element = node->elements[i];
				}
				if (position) {
					*position = pos + i;
				}
				retval = RL_FOUND;
				goto cleanup;
			}
		}
		pos += node->size;
		number = node->right;
	}
	retval = RL_NOT_FOUND;
cleanup:
	return retval;
}

int rl_find_element_by_position(rlite *db, rl_list *list, long *position, long *_pos, rl_list_node **_node, long *_number)
{
	rl_list_node *node;
	void *tmp_node;
	long pos = 0, number;
	if (*position > list->size + 1 || *position < - list->size - 1) {
		return RL_INVALID_PARAMETERS;
	}
	int retval = RL_OK;
	if (*position >= 0) {
		number = list->left;
		do {
			retval = rl_read(db, list->type->list_node_type, number, list, &tmp_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			node = tmp_node;
			if (pos + node->size >= *position) {
				break;
			}
			number = node->right;
			if (number != 0) {
				pos += node->size;
			}
		}
		while (number != 0 && pos < *position);
	}
	else {
		*position = list->size + *position + 1;
		pos = list->size;
		number = list->right;
		do {
			retval = rl_read(db, list->type->list_node_type, number, list, &tmp_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			node = tmp_node;
			pos -= node->size;
			if (pos <= *position) {
				break;
			}
			number = node->left;
		}
		while (number != 0);
	}
	*_pos = pos;
	*_node = node;
	if (_number) {
		*_number = number;
	}
cleanup:
	return retval;
}

int rl_list_get_element(struct rlite *db, rl_list *list, void **element, long position)
{
	long pos;
	rl_list_node *node;
	int retval = rl_find_element_by_position(db, list, &position, &pos, &node, NULL);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	*element = node->elements[position - pos];
cleanup:
	return retval;
}

int rl_list_add_element(rlite *db, rl_list *list, void *element, long position)
{
	rl_list_node *node, *sibling_node, *new_node, *old_node;
	long pos;
	long number, sibling_number;
	void *_node;
	int retval = rl_find_element_by_position(db, list, &position, &pos, &node, &number);
	if (retval != RL_FOUND) {
		return retval;
	}

	if (node->size != list->max_node_size) {
		if (position - pos + 1 < list->max_node_size) {
			memmove_dbg(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - (position - pos)), __LINE__);
		}
		node->elements[position - pos] = element;
		node->size++;

		retval = rl_write(db, list->type->list_node_type, number, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		if (node->left) {
			sibling_number = node->left;
			retval = rl_read(db, list->type->list_node_type, sibling_number, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			if (sibling_node->size != list->max_node_size) {
				if (position == pos) {
					sibling_node->elements[sibling_node->size] = element;
				}
				else {
					sibling_node->elements[sibling_node->size] = node->elements[0];
					memmove_dbg(&node->elements[0], &node->elements[1], sizeof(void *) * (position - pos - 1), __LINE__);
					node->elements[position - pos - 1] = element;
				}
				sibling_node->size++;
				retval = rl_write(db, list->type->list_node_type, number, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, list->type->list_node_type, sibling_number, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		if (node->right) {
			sibling_number = node->right;
			retval = rl_read(db, list->type->list_node_type, sibling_number, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			if (sibling_node->size != list->max_node_size) {
				memmove_dbg(&sibling_node->elements[1], &sibling_node->elements[0], sizeof(void *) * sibling_node->size, __LINE__);
				if (position - pos == node->size) {
					sibling_node->elements[0] = element;
				}
				else {
					sibling_node->elements[0] = node->elements[node->size - 1];
					memmove_dbg(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - position + pos - 1), __LINE__);
					node->elements[position - pos] = element;
				}
				sibling_node->size++;
				retval = rl_write(db, list->type->list_node_type, number, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, list->type->list_node_type, sibling_number, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		retval = rl_list_node_create(db, list, &new_node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (position - pos == node->size) {
			new_node->elements[0] = element;
		}
		else {
			new_node->elements[0] = node->elements[node->size - 1];
			memmove_dbg(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - position + pos - 1), __LINE__);
			node->elements[position - pos] = element;
		}
		new_node->size = 1;
		new_node->left = number;
		new_node->right = node->right;
		node->right = db->next_empty_page;
		retval = rl_write(db, list->type->list_node_type, db->next_empty_page, new_node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		retval = rl_write(db, list->type->list_node_type, number, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (new_node->right != 0) {
			retval = rl_read(db, list->type->list_node_type, new_node->right, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			old_node = _node;
			old_node->left = node->right;
			retval = rl_write(db, list->type->list_node_type, new_node->right, old_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
		else {
			list->right = node->right;
		}
	}

succeeded:
	list->size++;
cleanup:
	return retval;
}

int rl_list_remove_element(rlite *db, rl_list *list, long position)
{
	rl_list_node *node, *sibling_node;
	long pos, number;
	void *_node;
	int retval = rl_find_element_by_position(db, list, &position, &pos, &node, &number);
	if (retval != RL_FOUND) {
		return retval;
	}
	retval = RL_OK;

	if (node->size - (position - pos + 1) > 0) {
		free(node->elements[position - pos]);
		memmove_dbg(&node->elements[position - pos], &node->elements[position - pos + 1], sizeof(void *) * (node->size - (position - pos + 1)), __LINE__);
	}
	else {
		free(node->elements[node->size - 1]);
	}
	if (--node->size == 0) {
		if (list->left == number) {
			list->left = node->right;
		}
		if (list->right == number) {
			list->right = node->left;
		}
		if (node->left) {
			retval = rl_read(db, list->type->list_node_type, node->left, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			sibling_node->right = node->right;
		}
		if (node->right) {
			retval = rl_read(db, list->type->list_node_type, node->right, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			sibling_node->left = node->left;
		}
		retval = rl_delete(db, number);
		if (retval != RL_OK) {
			goto cleanup;
		}
		retval = rl_list_node_destroy(db, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		if (node->left) {
			retval = rl_read(db, list->type->list_node_type, node->left, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove_dbg(&sibling_node->elements[sibling_node->size], node->elements, sizeof(void *) * node->size, __LINE__);
				sibling_node->right = node->right;
				sibling_node->size += node->size;
				retval = rl_write(db, list->type->list_node_type, node->left, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				if (node->right) {
					retval = rl_read(db, list->type->list_node_type, node->right, list, &_node);
					if (retval != RL_FOUND) {
						goto cleanup;
					}
					sibling_node = _node;
					sibling_node->left = node->left;
					retval = rl_write(db, list->type->list_node_type, node->right, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
				}
				else {
					list->right = node->left;
				}
				rl_delete(db, number);
				if (retval != RL_OK) {
					goto cleanup;
				}
				// don't free each element
				free(node->elements);
				node->elements = NULL;
				retval = rl_list_node_destroy(db, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		if (node->right) {
			retval = rl_read(db, list->type->list_node_type, node->right, list, &_node);
			if (retval != RL_FOUND) {
				goto cleanup;
			}
			sibling_node = _node;
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove_dbg(&sibling_node->elements[node->size], sibling_node->elements, sizeof(void *) * sibling_node->size, __LINE__);
				memmove_dbg(sibling_node->elements, node->elements, sizeof(void *) * node->size, __LINE__);
				sibling_node->left = node->left;
				sibling_node->size += node->size;
				retval = rl_write(db, list->type->list_node_type, node->right, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				if (node->left) {
					retval = rl_read(db, list->type->list_node_type, node->left, list, &_node);
					if (retval != RL_FOUND) {
						goto cleanup;
					}
					sibling_node = _node;
					sibling_node->right = node->right;
					retval = rl_write(db, list->type->list_node_type, node->left, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
				}
				else {
					list->left = node->right;
				}
				rl_delete(db, number);
				if (retval != RL_OK) {
					goto cleanup;
				}
				// don't free each element
				free(node->elements);
				node->elements = NULL;
				retval = rl_list_node_destroy(db, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		retval = rl_write(db, list->type->list_node_type, number, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
succeeded:
	retval = RL_OK;
	list->size--;
cleanup:
	return 0;
}

int rl_list_is_balanced(rlite *db, rl_list *list)
{
	long i = 0, number = list->left, size = 0;
	long prev_size;
	long max_node = (list->size / list->max_node_size + 1) * 2;
	int retval = RL_OK;
	long *left = NULL;
	long *right = malloc(sizeof(long) * max_node);
	if (!right) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	left = malloc(sizeof(long) * max_node);
	if (!left) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	rl_list_node *node;
	void *_node;
	while (number != 0) {
		retval = rl_read(db, list->type->list_node_type, number, list, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		size += node->size;
		left[i] = node->left;
		right[i] = node->right;
		if (i != 0) {
			if (node->size + prev_size < list->max_node_size) {
				fprintf(stderr, "Two continous node could be merged\n");
				retval = RL_INVALID_STATE;
				goto cleanup;
			}
		}
		if (node->size == 0) {
			fprintf(stderr, "Empty node in list\n");
			retval = RL_INVALID_STATE;
			goto cleanup;
		}
		prev_size = node->size;
		number = node->right;
		if (i >= 2 && right[i - 2] != left[i]) {
			fprintf(stderr, "Left and right pointers mismatch at position %ld\n", i);
			retval = RL_INVALID_STATE;
			goto cleanup;
		}
		i++;
	}

	if (size != list->size) {
		fprintf(stderr, "Expected size %ld, got %ld\n", size, list->size);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	free(right);
	free(left);
	return retval;
}

#ifdef DEBUG
int rl_print_list(rlite *db, rl_list *list)
{
	if (!list->type->formatter) {
		fprintf(stderr, "Trying to print an element without formatter\n");
		return RL_UNEXPECTED;
	}
	printf("-------\n");
	rl_list_node *node;
	void *_node;
	char *element;
	int size;
	long i, number = list->left;
	int retval = RL_OK;
	while (number != 0) {
		retval = rl_read(db, list->type->list_node_type, number, list, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		for (i = 0; i < node->size; i++) {
			retval = list->type->formatter(node->elements[i], &element, &size);
			if (retval != RL_OK) {
				goto cleanup;
			}
			fwrite(element, sizeof(char), size, stdout);
			free(element);
			printf("\n");
		}
		number = node->right;
	}
	printf("-------\n");
cleanup:
	return retval;
}
#endif

int rl_flatten_list(rlite *db, rl_list *list, void **scores)
{
	long i, number = list->left, pos = 0;
	rl_list_node *node;
	void *_node;
	int retval = RL_OK;
	while (number != 0) {
		retval = rl_read(db, list->type->list_node_type, number, list, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		for (i = 0; i < node->size; i++) {
			scores[pos++] = node->elements[i];
		}
		number = node->right;
	}
	retval = RL_OK;
cleanup:
	return retval;
}
