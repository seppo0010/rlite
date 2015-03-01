#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include "rlite.h"
#include "status.h"
#include "page_list.h"
#include "page_string.h"
#include "util.h"

#ifdef RL_DEBUG
int rl_print_list_node(rl_list *list, rl_list_node *node);
#endif
int rl_list_node_create(rlite *db, rl_list *list, rl_list_node **node);

rl_list_type rl_list_type_long = {
	0,
	0,
	sizeof(long),
	long_cmp,
#ifdef RL_DEBUG
	long_formatter,
#endif
};

int rl_list_serialize(rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_list *list = obj;
	put_4bytes(data, list->left);
	put_4bytes(&data[4], list->right);
	put_4bytes(&data[8], list->max_node_size);
	put_4bytes(&data[12], list->size);
	return RL_OK;
}

int rl_list_deserialize(rlite *UNUSED(db), void **obj, void *context, unsigned char *data)
{
	rl_list *list;
	int retval = RL_OK;
	RL_MALLOC(list, sizeof(*list));
	list->type = context;
	list->left = get_4bytes(data);
	list->right = get_4bytes(&data[4]);
	list->max_node_size = get_4bytes(&data[8]);
	list->size = get_4bytes(&data[12]);
	*obj = list;
cleanup:
	return retval;
}

int rl_list_node_serialize_long(rlite *UNUSED(db), void *obj, unsigned char *data)
{
	rl_list_node *node = obj;

	put_4bytes(data, node->size);
	put_4bytes(&data[4], node->left);
	put_4bytes(&data[8], node->right);
	long i, pos = 12;
	for (i = 0; i < node->size; i++) {
		put_4bytes(&data[pos], *(long *)(node->elements[i]));
		pos += 4;
	}
	return RL_OK;
}

int rl_list_node_deserialize_long(rlite *db, void **obj, void *context, unsigned char *data)
{
	rl_list *list = context;
	rl_list_node *node = NULL;
	long i = 0, pos = 12;
	int retval;
	RL_CALL(rl_list_node_create, RL_OK, db, list, &node);
	node->size = (long)get_4bytes(data);
	node->left = (long)get_4bytes(&data[4]);
	node->right = (long)get_4bytes(&data[8]);
	for (i = 0; i < node->size; i++) {
		RL_MALLOC(node->elements[i], sizeof(long))
		*(long *)node->elements[i] = get_4bytes(&data[pos]);
		pos += 4;
	}
	*obj = node;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK && node) {
		node->size = i;
		rl_list_node_destroy(db, node);
	}
	return retval;
}

void rl_list_init()
{
	rl_list_type_long.list_type = &rl_data_type_list_long;
	rl_list_type_long.list_node_type = &rl_data_type_list_node_long;
}

int rl_list_node_create(rlite *UNUSED(db), rl_list *list, rl_list_node **_node)
{
	int retval;
	rl_list_node *node;
	RL_MALLOC(node, sizeof(rl_list_node));
	RL_MALLOC(node->elements, sizeof(void *) * list->max_node_size);
	node->size = 0;
	node->left = 0;
	node->right = 0;
	*_node = node;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (node) {
			rl_free(node->elements);
		}
		rl_free(node);
	}
	return retval;
}

int rl_list_node_destroy(rlite *UNUSED(db), void *_node)
{
	rl_list_node *node = _node;
	long i;
	if (node->elements) {
		for (i = 0; i < node->size; i++) {
			rl_free(node->elements[i]);
		}
		rl_free(node->elements);
	}
	rl_free(node);
	return 0;
}

int rl_list_create(rlite *db, rl_list **_list, rl_list_type *type)
{
	int retval;
	rl_list *list;
	RL_MALLOC(list, sizeof(rl_list))
	list->max_node_size = (db->page_size - 12) / type->element_size;
	list->type = type;
	rl_list_node *node;
	RL_CALL(rl_list_node_create, RL_OK, db, list, &node);
	node->left = 0;
	node->right = 0;
	list->size = 0;
	list->left = db->next_empty_page;
	RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, db->next_empty_page, node);
	list->right = list->left;
	*_list = list;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_list_destroy(db, list);
	}
	return retval;
}

int rl_list_destroy(rlite *UNUSED(db), void *list)
{
	rl_free(list);
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
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
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

static int rl_find_element_by_position(rlite *db, rl_list *list, long *position, long *_pos, rl_list_node **_node, long *_number, int add)
{
	rl_list_node *node;
	void *tmp_node;
	long pos = 0, number;
	if (*position >= list->size + add || *position <= - list->size - 1 - add) {
		return RL_INVALID_PARAMETERS;
	}
	int retval = RL_OK;
	if (*position >= 0) {
		number = list->left;
		while (1) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &tmp_node, 1);
			node = tmp_node;
			if (pos + node->size > *position) {
				break;
			}
			if (node->right != 0) {
				number = node->right;
				pos += node->size;
			}
			else {
				break;
			}
		}
	}
	else {
		*position = list->size + *position + add;
		pos = list->size;
		number = list->right;
		while (1) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &tmp_node, 1);
			node = tmp_node;
			pos -= node->size;
			if (pos <= *position) {
				break;
			}
			if (node->left != 0) {
				number = node->left;
			}
			else {
				break;
			}
		}
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
	int retval;
	RL_CALL(rl_find_element_by_position, RL_FOUND, db, list, &position, &pos, &node, NULL, 0);
	*element = node->elements[position - pos];
cleanup:
	return retval;
}

int rl_list_add_element(rlite *db, rl_list *list, long list_page, void *element, long position)
{
	rl_list_node *node, *sibling_node, *new_node, *old_node;
	long pos;
	long number, sibling_number;
	void *_node;
	int retval;
	RL_CALL(rl_find_element_by_position, RL_FOUND, db, list, &position, &pos, &node, &number, 1);

	if (node->size != list->max_node_size) {
		if (position - pos + 1 < list->max_node_size) {
			memmove(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - (position - pos)));
		}
		node->elements[position - pos] = element;
		element = NULL;
		node->size++;

		RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, number, node);
	}
	else {
		if (node->left) {
			sibling_number = node->left;
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, sibling_number, list, &_node, 1);
			sibling_node = _node;
			if (sibling_node->size != list->max_node_size) {
				if (position == pos) {
					sibling_node->elements[sibling_node->size] = element;
				}
				else {
					sibling_node->elements[sibling_node->size] = node->elements[0];
					memmove(&node->elements[0], &node->elements[1], sizeof(void *) * (position - pos - 1));
					node->elements[position - pos - 1] = element;
				}
				element = NULL;
				sibling_node->size++;
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, number, node);
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, sibling_number, sibling_node);
				goto succeeded;
			}
		}
		if (node->right) {
			sibling_number = node->right;
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, sibling_number, list, &_node, 1);
			sibling_node = _node;
			if (sibling_node->size != list->max_node_size) {
				memmove(&sibling_node->elements[1], &sibling_node->elements[0], sizeof(void *) * sibling_node->size);
				if (position - pos == node->size) {
					sibling_node->elements[0] = element;
				}
				else {
					sibling_node->elements[0] = node->elements[node->size - 1];
					memmove(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - position + pos - 1));
					node->elements[position - pos] = element;
				}
				element = NULL;
				sibling_node->size++;
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, number, node);
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, sibling_number, sibling_node);
				goto succeeded;
			}
		}
		RL_CALL(rl_list_node_create, RL_OK, db, list, &new_node);
		if (position - pos == node->size) {
			new_node->elements[0] = element;
		}
		else {
			new_node->elements[0] = node->elements[node->size - 1];
			memmove(&node->elements[position - pos + 1], &node->elements[position - pos], sizeof(void *) * (node->size - position + pos - 1));
			node->elements[position - pos] = element;
		}
		element = NULL;
		new_node->size = 1;
		new_node->left = number;
		new_node->right = node->right;
		node->right = db->next_empty_page;
		RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, db->next_empty_page, new_node);
		RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, number, node);
		if (new_node->right != 0) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, new_node->right, list, &_node, 1);
			old_node = _node;
			old_node->left = node->right;
			RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, new_node->right, old_node);
		}
		else {
			list->right = node->right;
		}
	}

succeeded:
	list->size++;
	RL_CALL(rl_write, RL_OK, db, list->type->list_type, list_page, list);
cleanup:
	if (retval != RL_OK) {
		rl_free(element);
	}
	return retval;
}

int rl_list_remove_element(rlite *db, rl_list *list, long list_page, long position)
{
	rl_list_node *node, *sibling_node;
	long pos, number;
	void *_node;
	int retval;
	RL_CALL(rl_find_element_by_position, RL_FOUND, db, list, &position, &pos, &node, &number, 0);

	if (node->size - (position - pos + 1) > 0) {
		rl_free(node->elements[position - pos]);
		memmove(&node->elements[position - pos], &node->elements[position - pos + 1], sizeof(void *) * (node->size - (position - pos + 1)));
	}
	else {
		rl_free(node->elements[node->size - 1]);
	}
	if (--node->size == 0) {
		if (list->left == number) {
			list->left = node->right;
		}
		if (list->right == number) {
			list->right = node->left;
		}
		if (node->left) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->left, list, &_node, 1);
			sibling_node = _node;
			sibling_node->right = node->right;
			RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->left, sibling_node);
		}
		if (node->right) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->right, list, &_node, 1);
			sibling_node = _node;
			sibling_node->left = node->left;
			RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->right, sibling_node);
		}
		RL_CALL(rl_delete, RL_OK, db, number);
	}
	else {
		if (node->left) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->left, list, &_node, 1);
			sibling_node = _node;
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove(&sibling_node->elements[sibling_node->size], node->elements, sizeof(void *) * node->size);
				sibling_node->right = node->right;
				sibling_node->size += node->size;
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->left, sibling_node);
				if (node->right) {
					RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->right, list, &_node, 1);
					sibling_node = _node;
					sibling_node->left = node->left;
					RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->right, sibling_node);
				}
				else {
					list->right = node->left;
				}
				// don't rl_free each element
				rl_free(node->elements);
				node->elements = NULL;

				rl_delete(db, number);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		if (node->right) {
			RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->right, list, &_node, 1);
			sibling_node = _node;
			if (sibling_node->size + node->size <= list->max_node_size) {
				memmove(&sibling_node->elements[node->size], sibling_node->elements, sizeof(void *) * sibling_node->size);
				memmove(sibling_node->elements, node->elements, sizeof(void *) * node->size);
				sibling_node->left = node->left;
				sibling_node->size += node->size;
				RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->right, sibling_node);
				if (node->left) {
					RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node->left, list, &_node, 1);
					sibling_node = _node;
					sibling_node->right = node->right;
					RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, node->left, sibling_node);
				}
				else {
					list->left = node->right;
				}
				// don't rl_free each element
				rl_free(node->elements);
				node->elements = NULL;

				rl_delete(db, number);
				if (retval != RL_OK) {
					goto cleanup;
				}
				goto succeeded;
			}
		}
		RL_CALL(rl_write, RL_OK, db, list->type->list_node_type, number, node);
	}
succeeded:
	retval = RL_OK;
	list->size--;
	if (list->size == 0) {
		RL_CALL(rl_delete, RL_OK, db, list_page);
		retval = RL_DELETED;
	}
	else {
		RL_CALL(rl_write, RL_OK, db, list->type->list_type, list_page, list);
	}
cleanup:
	return retval;
}

int rl_list_is_balanced(rlite *db, rl_list *list)
{
	rl_list_iterator *iterator;
	long i = 0, number = list->left, size = 0;
	long prev_size;
	long max_node = (list->size / list->max_node_size + 1) * 2;
	int retval = RL_OK;
	long *left = NULL;
	long *right;
	RL_MALLOC(right, sizeof(long) * max_node);
	RL_MALLOC(left, sizeof(long) * max_node);
	rl_list_node *node;
	void *_node;
	while (number != 0) {
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
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

	i = 0;
	retval = rl_list_iterator_create(db, &iterator, list, 1);
	while ((retval = rl_list_iterator_next(iterator, NULL)) == RL_OK) {
		i++;
	}
	if (retval != RL_END) {
		goto cleanup;
	}
	if (i != size) {
		fprintf(stderr, "Expected to iterate %ld times but only did %ld\n", size, i);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	i = 0;
	retval = rl_list_iterator_create(db, &iterator, list, -1);
	while ((retval = rl_list_iterator_next(iterator, NULL)) == RL_OK) {
		i++;
	}
	if (retval != RL_END) {
		goto cleanup;
	}
	if (i != size) {
		fprintf(stderr, "Expected to iterate %ld times but only did %ld\n", size, i);
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	retval = RL_OK;
cleanup:
	rl_free(right);
	rl_free(left);
	return retval;
}

#ifdef RL_DEBUG
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
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
		node = _node;
		for (i = 0; i < node->size; i++) {
			RL_CALL(list->type->formatter, RL_OK, node->elements[i], &element, &size);
			fwrite(element, sizeof(char), size, stdout);
			rl_free(element);
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
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
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

int rl_list_iterator_create(rlite *db, rl_list_iterator **_iterator, rl_list *list, int direction)
{
	void *_node;
	int retval;
	rl_list_iterator *iterator = NULL;
	RL_MALLOC(iterator, sizeof(*iterator));
	iterator->db = db;
	iterator->list = list;
	if (direction < 0) {
		iterator->direction = -1;
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, list->right, list, &_node, 0);
		iterator->node = _node;
		iterator->node_position = iterator->node->size - 1;
	}
	else {
		iterator->direction = 1;
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, list->left, list, &_node, 0);
		iterator->node = _node;
		iterator->node_position = 0;
	}
	*_iterator = iterator;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_list_iterator_destroy(db, iterator);
	}
	return retval;
}

int rl_list_iterator_destroy(rlite *UNUSED(db), rl_list_iterator *iterator)
{
	if (iterator->node) {
		rl_list_node_nocache_destroy(iterator->db, iterator->node);
	}
	rl_free(iterator);
	return RL_OK;
}

int rl_list_iterator_next(rl_list_iterator *iterator, void **element)
{
	int retval;
	if (iterator->node == NULL) {
		retval = RL_END;
		goto cleanup;
	}
	if (element) {
		RL_MALLOC(*element, iterator->list->type->element_size);
		memcpy(*element, iterator->node->elements[iterator->node_position], iterator->list->type->element_size);
	}
	iterator->node_position += iterator->direction;
	if (iterator->node_position < 0 || iterator->node_position == iterator->node->size) {
		long next_node_page = iterator->direction == 1 ? iterator->node->right : iterator->node->left;
		RL_CALL(rl_list_node_nocache_destroy, RL_OK, iterator->db, iterator->node);
		iterator->node = NULL;
		if (next_node_page) {
			void *_node;
			RL_CALL(rl_read, RL_FOUND, iterator->db, iterator->list->type->list_node_type, next_node_page, iterator->list, &_node, 0);
			iterator->node = _node;
			iterator->node_position = iterator->direction == 1 ? 0 : (iterator->node->size - 1);
		}
	}
	if (iterator->node && iterator->node_position < -1) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator->node) {
			rl_list_node_nocache_destroy(iterator->db, iterator->node);
		}
		rl_list_iterator_destroy(iterator->db, iterator);
	}
	return retval;
}

int rl_list_pages(struct rlite *db, rl_list *list, short *pages)
{
	rl_list_node *node;
	void *_node;
	long number = list->left;
	int retval = RL_OK;
	pages[number] = 1;
	while (number != 0) {
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
		node = _node;
		number = node->right;
		if (number) {
			pages[number] = 1;
		}
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_list_delete(struct rlite *db, rl_list *list)
{
	rl_list_node *node;
	void *_node;
	long number = list->left, new_number;
	int retval = RL_OK;
	while (number != 0) {
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, number, list, &_node, 1);
		node = _node;
		new_number = node->right;
		RL_CALL(rl_delete, RL_OK, db, number);
		number = new_number;
	}
	retval = RL_OK;
cleanup:
	return retval;
}
