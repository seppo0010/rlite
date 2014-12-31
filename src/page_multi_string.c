#include <stdlib.h>
#include <string.h>
#include "../deps/sha1.h"
#include "rlite.h"
#include "page_list.h"
#include "page_string.h"
#include "page_multi_string.h"
#include "util.h"

int rl_multi_string_cmp(struct rlite *db, long p1, long p2, int *cmp)
{
	rl_list *list1 = NULL, *list2 = NULL;
	rl_list_node *node1 = NULL, *node2 = NULL;
	void *_list, *_node;
	unsigned char *str1, *str2;

	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, p1, &rl_list_type_long, &_list, 0);
	list1 = _list;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, p2, &rl_list_type_long, &_list, 0);
	list2 = _list;

	long node_number1 = list1->left, node_number2 = list2->left, i;
	int first = 1;
	while (1) {
		if (node_number1 == 0 || node_number2 == 0) {
			if (node_number1 == 0 && node_number2 == 0) {
				*cmp = 0;
			}
			else if (node_number1 == 0) {
				*cmp = -1;
			}
			else {
				*cmp = 1;
			}
			retval = RL_OK;
			goto cleanup;
		}
		RL_CALL(rl_read, RL_FOUND, db, list1->type->list_node_type, node_number1, list1, &_node, 0);
		node1 = _node;
		RL_CALL(rl_read, RL_FOUND, db, list2->type->list_node_type, node_number2, list2, &_node, 0);
		node2 = _node;
		if (first) {
			if (*(long *)node1->elements[0] == 0) {
				*cmp = -1;
				retval = RL_OK;
				goto cleanup;
			}
			if (*(long *)node2->elements[0] == 0) {
				*cmp = 1;
				retval = RL_OK;
				goto cleanup;
			}
		}
		for (i = first ? 1 : 0; i < node1->size; i++) {
			first = 0;
			RL_CALL(rl_string_get, RL_OK, db, &str1, *(long *)node1->elements[i]);
			RL_CALL(rl_string_get, RL_OK, db, &str2, *(long *)node2->elements[i]);
			*cmp = memcmp(str1, str2, db->page_size);
			if (*cmp != 0) {
				if (*cmp < 0) {
					*cmp = -1;
				}
				else {
					*cmp = 1;
				}
				goto cleanup;
			}
		}
		node_number1 = node1->right;
		node_number2 = node2->right;
		rl_list_node_nocache_destroy(db, node1);
		rl_list_node_nocache_destroy(db, node2);
		node1 = node2 = NULL;
	}
cleanup:
	rl_list_nocache_destroy(db, list1);
	rl_list_nocache_destroy(db, list2);
	if (node1) {
		rl_list_node_nocache_destroy(db, node1);
	}
	if (node2) {
		rl_list_node_nocache_destroy(db, node2);
	}
	return retval;
}

int rl_multi_string_cmp_str(struct rlite *db, long p1, unsigned char *str, long len, int *cmp)
{
	rl_list *list1 = NULL;
	rl_list_node *node1 = NULL;
	void *_list, *_node;
	unsigned char *str1;

	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, p1, &rl_list_type_long, &_list, 0);
	list1 = _list;

	long node_number1 = list1->left, i, pos = 0;
	int first = 1;
	long cmplen;
	long stored_length = 0;
	*cmp = 0;
	do {
		if (node_number1 == 0) {
			*cmp = len == 0 ? 0 : -1;
			break;
		}
		RL_CALL(rl_read, RL_FOUND, db, list1->type->list_node_type, node_number1, list1, &_node, 0);
		node1 = _node;
		if (first) {
			stored_length = *(long *)node1->elements[0];
			if (stored_length == 0) {
				*cmp = len == 0 ? 0 : -1;
			}
		}
		for (i = first ? 1 : 0; i < node1->size; i++) {
			RL_CALL(rl_string_get, RL_OK, db, &str1, *(long *)node1->elements[i]);
			cmplen = db->page_size < len - pos ? db->page_size : len - pos;
			if (cmplen == 0) {
				// finished with str, the page_multi_string starts with str
				*cmp = 1;
				break;
			}
			*cmp = memcmp(str1, &str[pos], cmplen);
			if (*cmp != 0) {
				if (*cmp < 0) {
					*cmp = -1;
				}
				else {
					*cmp = 1;
				}
				goto cleanup;
			}
			pos += cmplen;
		}
		if (*cmp == 0 && len == pos && stored_length != len) {
			*cmp = stored_length > len ? 1 : -1;
		}
		first = 0;
		node_number1 = node1->right;
		rl_list_node_nocache_destroy(db, node1);
		node1 = NULL;
	}
	while (len > pos);
	retval = RL_OK;
cleanup:
	rl_list_nocache_destroy(db, list1);
	if (node1) {
		rl_list_node_nocache_destroy(db, node1);
	}
	return retval;
}

int rl_multi_string_get(struct rlite *db, long number, unsigned char **_data, long *size)
{
	rl_list *list = NULL;
	rl_list_node *node = NULL;
	void *_list, *_node;
	unsigned char *data = NULL;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &_list, 0);
	list = _list;
	unsigned char *tmp_data;
	long node_number = list->left, i, pos = 0, to_copy;
	while (node_number > 0) {
		RL_CALL(rl_read, RL_FOUND, db, list->type->list_node_type, node_number, list, &_node, 0);
		node = _node;
		i = 0;
		if (!data) {
			*size = *(long *)node->elements[0];
			RL_MALLOC(data, sizeof(unsigned char) * (*size));
			i = 1;
		}
		for (; i < node->size; i++) {
			RL_CALL(rl_string_get, RL_OK, db, &tmp_data, *(long *)node->elements[i]);
			to_copy = db->page_size;
			if (pos + to_copy > *size) {
				to_copy = *size - pos;
			}
			memcpy(&data[pos], tmp_data, sizeof(unsigned char) * to_copy);
			pos += to_copy;
		}
		node_number = node->right;
		rl_list_node_nocache_destroy(db, node);
		node = NULL;
	}
	*_data = data;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(data);
	}
	if (list) {
		rl_list_nocache_destroy(db, list);
	}
	if (node) {
		rl_list_node_nocache_destroy(db, node);
	}
	return retval;
}

int rl_multi_string_set(struct rlite *db, long *number, const unsigned char *data, long size)
{
	int retval;
	long *page = NULL, to_copy;
	unsigned char *string = NULL;
	rl_list *list = NULL;
	RL_CALL(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	*number = db->next_empty_page;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_list_long, *number, list);
	long pos = 0;
	RL_MALLOC(page, sizeof(*page));
	*page = size;
	RL_CALL(rl_list_add_element, RL_OK, db, list, *number, page, -1);
	page = NULL;
	while (pos < size) {
		RL_MALLOC(page, sizeof(*page));
		retval = rl_string_create(db, &string, page);
		if (retval != RL_OK) {
			goto cleanup;
		}
		to_copy = db->page_size;
		if (pos + to_copy > size) {
			to_copy = size - pos;
		}
		memcpy(string, &data[pos], sizeof(unsigned char) * to_copy);
		string = NULL;
		RL_CALL(rl_list_add_element, RL_OK, db, list, *number, page, -1);
		page = NULL;
		pos += to_copy;
	}
cleanup:
	if (retval != RL_OK) {
		rl_free(page);
		rl_free(string);
	}
	return retval;
}

int rl_multi_string_sha1(struct rlite *db, unsigned char digest[20], long number)
{
	unsigned char *data;
	long datalen;
	SHA1_CTX sha;
	SHA1Init(&sha);

	void *tmp;
	rl_list *list;
	rl_list_iterator *iterator = NULL;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &tmp, 0);
	list = tmp;

	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);

	long size = 0;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		if (size == 0) {
			size = *(long *)tmp;
			rl_free(tmp);
			continue;
		}
		RL_CALL(rl_string_get, RL_OK, db, &data, *(long *)tmp);
		datalen = size > db->page_size ? db->page_size : size;
		SHA1Update(&sha, data, datalen);
		size -= datalen;
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(rl_list_nocache_destroy, RL_OK, db, list);

	SHA1Final(digest, &sha);
	retval = RL_OK;
cleanup:
	if (iterator) {
		rl_list_iterator_destroy(db, iterator);
	}
	return retval;
}

int rl_multi_string_pages(struct rlite *db, long page, short *pages)
{
	void *tmp;
	rl_list *list;
	rl_list_iterator *iterator = NULL;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, page, &rl_list_type_long, &tmp, 0);
	list = tmp;

	RL_CALL(rl_list_pages, RL_OK, db, list, pages);
	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);

	int first = 1;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		if (first) {
			first = 0;
		}
		else {
			pages[*(long *)tmp] = 1;
		}
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(rl_list_nocache_destroy, RL_OK, db, list);

	retval = RL_OK;
cleanup:
	if (iterator) {
		rl_list_iterator_destroy(db, iterator);
	}
	return retval;
}

int rl_multi_string_delete(struct rlite *db, long page)
{
	void *tmp;
	rl_list *list;
	rl_list_iterator *iterator = NULL;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, page, &rl_list_type_long, &tmp, 1);
	list = tmp;

	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);

	int first = 1;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		if (first) {
			first = 0;
		}
		else {
			RL_CALL(rl_delete, RL_OK, db, *(long *)tmp);
		}
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(rl_list_delete, RL_OK, db, list);
	RL_CALL(rl_delete, RL_OK, db, page);

	retval = RL_OK;
cleanup:
	if (iterator) {
		rl_list_iterator_destroy(db, iterator);
	}
	return retval;
}
