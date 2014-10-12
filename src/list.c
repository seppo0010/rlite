#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "rlite.h"
#include "list.h"
#include "util.h"

int rl_print_list_node(rl_list *list, rl_list_node *node);
int rl_list_node_create(rl_list *list, rl_list_node **node);

int long_cmp(void *v1, void *v2)
{
	long a = *((long *)v1), b = *((long *)v2);
	if (a == b) {
		return 0;
	}
	return a > b ? 1 : -1;
}

int long_list_list_node_serialize(rl_list *list, rl_list_node *node, unsigned char **_data, long *data_size)
{
	unsigned char *data = malloc(sizeof(unsigned char) * ((4 * list->max_node_size) + 12));
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
	*_data = data;
	if (data_size) {
		*data_size = pos;
	}
cleanup:
	return retval;
}

int long_list_list_node_deserialize(rl_list *list, unsigned char *data, rl_list_node **_node)
{
	rl_list_node *node;
	int retval = rl_list_node_create(list, &node);
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
	*_node = node;
cleanup:
	return retval;
}

int long_formatter(void *v2, char **formatted, int *size)
{
	*formatted = malloc(sizeof(char) * 22);
	if (*formatted == NULL) {
		return RL_OUT_OF_MEMORY;
	}
	*size = snprintf(*formatted, 22, "%ld", *(long *)v2);
	return RL_OK;
}

void init_long_list()
{
	long_list.element_size = sizeof(long);
	long_list.formatter = long_formatter;
	long_list.serialize = long_list_list_node_serialize;
	long_list.deserialize = long_list_list_node_deserialize;
	long_list.cmp = long_cmp;
}

int rl_list_node_create(rl_list *list, rl_list_node **_node)
{
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

int rl_list_node_destroy(rl_list *list, rl_list_node *node)
{
	list = list;
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

int rl_list_create(rl_list **_list, rl_list_type *type, long max_node_size, rl_accessor *accessor)
{
	rl_list *list = malloc(sizeof(rl_list));
	if (!list) {
		return RL_OUT_OF_MEMORY;
	}
	int retval;
	list->max_node_size = max_node_size;
	list->type = type;
	rl_list_node *node;
	retval = rl_list_node_create(list, &node);
	if (retval != RL_OK) {
		free(list);
		goto cleanup;
	}
	node->left = 0;
	node->right = 0;
	list->accessor = accessor;
	list->size = 0;
	retval = list->accessor->insert(list, &list->left, node);
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

int rl_list_destroy(rl_list *list)
{
	long i;
	rl_list_node **nodes;
	long size;
	int retval = list->accessor->list(list, &nodes, &size);
	if (retval != RL_OK) {
		goto cleanup;
	}
	for (i = 0; i < size; i++) {
		retval = rl_list_node_destroy(list, nodes[i]);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
cleanup:
	free(list);
	return retval;
}

int rl_list_find_element(rl_list *list, void *element, long *position)
{
	rl_list_node *node;
	long pos = 0, i, number = list->left;
	int retval = RL_OK;
	while (number != 0) {
		retval = list->accessor->select(list, number, &node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		for (i = 0; i < node->size; i++) {
			if (list->type->cmp(node->elements[i], element) == 0) {
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
cleanup:
	return retval;
}

int rl_find_element_by_position(rl_list *list, long *position, long *_pos, rl_list_node **_node, long *_number)
{
	rl_list_node *node;
	long pos = 0, number;
	if (*position > list->size + 1 || *position < - list->size - 1) {
		return RL_INVALID_PARAMETERS;
	}
	int retval = RL_OK;
	if (*position >= 0) {
		number = list->left;
		do {
			retval = list->accessor->select(list, number, &node);
			if (retval != RL_OK) {
				goto cleanup;
			}
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
			retval = list->accessor->select(list, number, &node);
			if (retval != RL_OK) {
				goto cleanup;
			}
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

int rl_list_add_element(rl_list *list, void *element, long position)
{
	rl_list_node *node;
	long pos;
	int retval = rl_find_element_by_position(list, &position, &pos, &node, NULL);
	if (retval != RL_OK) {
		return retval;
	}

	if (node->size != list->max_node_size) {
		if (position - pos + 1 < list->max_node_size) {
			memmove_dbg(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - (position - pos)), __LINE__);
		}
		node->elements[position - pos] = element;
		node->size++;
		retval = list->accessor->update(list, NULL, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		rl_list_node *sibling_node;
		if (node->left) {
			retval = list->accessor->select(list, node->left, &sibling_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
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
				retval = list->accessor->update(list, NULL, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = list->accessor->update(list, NULL, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		if (node->right) {
			retval = list->accessor->select(list, node->right, &sibling_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
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
				retval = list->accessor->update(list, NULL, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = list->accessor->update(list, NULL, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		rl_list_node *new_node, *old_node;
		retval = rl_list_node_create(list, &new_node);
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
		new_node->right = node->right;
		retval = list->accessor->insert(list, &node->right, new_node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		retval = list->accessor->update(list, &new_node->left, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		if (new_node->right != 0) {
			retval = list->accessor->select(list, new_node->right, &old_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			old_node->left = node->right;
			retval = list->accessor->update(list, NULL, old_node);
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

int rl_list_remove_element(rl_list *list, long position)
{
	rl_list_node *node, *sibling_node;
	long pos, number;
	int retval = rl_find_element_by_position(list, &position, &pos, &node, &number);
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
			retval = list->accessor->select(list, node->left, &sibling_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			sibling_node->right = node->right;
		}
		if (node->right) {
			retval = list->accessor->select(list, node->right, &sibling_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			sibling_node->left = node->left;
		}
		retval = list->accessor->remove(list, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		retval = rl_list_node_destroy(list, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
	else {
		if (node->left) {
			retval = list->accessor->select(list, node->left, &sibling_node);
			if (retval != RL_OK) {
				goto cleanup;
			}
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove_dbg(&sibling_node->elements[sibling_node->size], node->elements, sizeof(void *) * node->size, __LINE__);
				sibling_node->right = node->right;
				sibling_node->size += node->size;
				retval = list->accessor->update(list, NULL, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				if (node->right) {
					retval = list->accessor->select(list, node->right, &sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					sibling_node->left = node->left;
					retval = list->accessor->update(list, NULL, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
				}
				else {
					list->right = node->left;
				}
				retval = list->accessor->remove(list, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				// don't free each element
				free(node->elements);
				node->elements = NULL;
				retval = rl_list_node_destroy(list, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		if (node->right) {
			retval = list->accessor->select(list, node->right, &sibling_node);
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove_dbg(&sibling_node->elements[node->size], sibling_node->elements, sizeof(void *) * sibling_node->size, __LINE__);
				memmove_dbg(sibling_node->elements, node->elements, sizeof(void *) * node->size, __LINE__);
				sibling_node->left = node->left;
				sibling_node->size += node->size;
				retval = list->accessor->update(list, NULL, sibling_node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				if (node->left) {
					retval = list->accessor->select(list, node->left, &sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
					sibling_node->right = node->right;
					retval = list->accessor->update(list, NULL, sibling_node);
					if (retval != RL_OK) {
						goto cleanup;
					}
				}
				else {
					list->left = node->right;
				}
				retval = list->accessor->remove(list, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				// don't free each element
				free(node->elements);
				node->elements = NULL;
				retval = rl_list_node_destroy(list, node);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		retval = list->accessor->update(list, NULL, node);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}
succeeded:
	list->size--;
cleanup:
	return 0;
}

int rl_list_is_balanced(rl_list *list)
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
	while (number != 0) {
		retval = list->accessor->select(list, number, &node);
		if (retval != RL_OK) {
			goto cleanup;
		}
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
		retval = 0;
		goto cleanup;
	}
cleanup:
	free(right);
	free(left);
	return retval;
}

int rl_print_list(rl_list *list)
{
	printf("-------\n");
	rl_list_node *node;
	char *element;
	int size;
	long i, number = list->left;
	int retval = RL_OK;
	while (number != 0) {
		retval = list->accessor->select(list, number, &node);
		if (retval != RL_OK) {
			goto cleanup;
		}
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

int rl_flatten_list(rl_list *list, void *** scores)
{
	long i, number = list->left, pos = 0;
	rl_list_node *node;
	int retval = RL_OK;
	while (number != 0) {
		retval = list->accessor->select(list, number, &node);
		if (retval != RL_OK) {
			goto cleanup;
		}
		for (i = 0; i < node->size; i++) {
			(*scores)[pos++] = node->elements[i];
		}
		number = node->right;
	}
cleanup:
	return retval;
}
