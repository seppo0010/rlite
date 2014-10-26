#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "page_list.h"
#include "page_string.h"
#include "obj_string.h"

int rl_obj_string_get(struct rlite *db, long number, unsigned char **_data, long *size)
{
	rl_list *list;
	rl_list_node *node;
	void *_list, *_node;
	int retval = rl_read(db, &rl_data_type_list_long, number, &list_long, &_list);
	if (retval != RL_FOUND) {
		return retval;
	}
	list = _list;
	unsigned char *tmp_data;
	unsigned char *data = NULL;
	long node_number = list->left, i, pos = 0, to_copy;
	while (node_number > 0) {
		retval = rl_read(db, list->type->list_node_type, node_number, list, &_node);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node = _node;
		i = 0;
		if (!data) {
			*size = *(long *)node->elements[0];
			data = malloc(sizeof(unsigned char) * (*size));
			i = 1;
		}
		for (; i < node->size; i++) {
			retval = rl_string_get(db, &tmp_data, *(long *)node->elements[i]);
			if (retval != RL_OK) {
				goto cleanup;
			}
			to_copy = db->page_size;
			if (pos + to_copy > *size) {
				to_copy = *size - pos;
			}
			memcpy(&data[pos], tmp_data, sizeof(unsigned char) * to_copy);
			pos += to_copy;
		}
		node_number = node->right;
	}
	*_data = data;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_obj_string_set(struct rlite *db, long *number, unsigned char *data, long size)
{
	rl_list *list;
	int retval = rl_list_create(db, &list, &list_long);
	if (retval != RL_OK) {
		return retval;
	}
	*number = db->next_empty_page;
	retval = rl_write(db, &rl_data_type_list_long, *number, list);
	if (retval != RL_OK) {
		free(list);
		return retval;
	}
	long pos = 0;
	long *page, to_copy;
	unsigned char *string;
	page = malloc(sizeof(long));
	if (!page) {
		retval = RL_OUT_OF_MEMORY;
		goto cleanup;
	}
	*page = size;
	retval = rl_list_add_element(db, list, page, -1);
	if (retval != RL_OK) {
		goto cleanup;
	}
	while (pos < size) {
		page = malloc(sizeof(long));
		if (!page) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		retval = rl_string_create(db, &string, page);
		if (retval != RL_OK) {
			goto cleanup;
		}
		to_copy = db->page_size;
		if (pos + to_copy > size) {
			to_copy = size - pos;
		}
		memcpy(string, &data[pos], sizeof(unsigned char) * to_copy);
		retval = rl_list_add_element(db, list, page, -1);
		if (retval != RL_OK) {
			goto cleanup;
		}
		pos += to_copy;
	}
cleanup:
	return retval;
}

int rl_obj_string_delete(struct rlite *db, long number)
{
	db = db;
	number = number;
	return RL_UNEXPECTED;
}

