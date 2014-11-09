#include <stdlib.h>
#include <string.h>
#include <openssl/sha.h>
#include "rlite.h"
#include "page_list.h"
#include "page_string.h"
#include "page_multi_string.h"

int rl_multi_string_cmp(struct rlite *db, long p1, long p2, int *cmp)
{
	rl_list *list1 = NULL, *list2 = NULL;
	rl_list_node *node1 = NULL, *node2 = NULL;
	void *_list, *_node;
	unsigned char *str1, *str2;

	int retval = rl_read(db, &rl_data_type_list_long, p1, &list_long, &_list, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	list1 = _list;
	retval = rl_read(db, &rl_data_type_list_long, p2, &list_long, &_list, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
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
		retval = rl_read(db, list1->type->list_node_type, node_number1, list1, &_node, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node1 = _node;
		retval = rl_read(db, list2->type->list_node_type, node_number2, list2, &_node, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node2 = _node;
		for (i = first ? 1 : 0; i < node1->size; i++) {
			first = 0;
			retval = rl_string_get(db, &str1, *(long *)node1->elements[i]);
			if (retval != RL_OK) {
				goto cleanup;
			}
			retval = rl_string_get(db, &str2, *(long *)node2->elements[i]);
			if (retval != RL_OK) {
				goto cleanup;
			}
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

	int retval = rl_read(db, &rl_data_type_list_long, p1, &list_long, &_list, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	list1 = _list;

	long node_number1 = list1->left, i, pos = 0;
	int first = 1;
	long cmplen;
	long stored_length = 0;
	do {
		if (node_number1 == 0) {
			*cmp = 0;
			break;
		}
		retval = rl_read(db, list1->type->list_node_type, node_number1, list1, &_node, 0);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		node1 = _node;
		if (first) {
			stored_length = *(long *)node1->elements[0];
		}
		for (i = first ? 1 : 0; i < node1->size; i++) {
			retval = rl_string_get(db, &str1, *(long *)node1->elements[i]);
			if (retval != RL_OK) {
				goto cleanup;
			}
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
	rl_list *list;
	rl_list_node *node = NULL;
	void *_list, *_node;
	int retval = rl_read(db, &rl_data_type_list_long, number, &list_long, &_list, 0);
	if (retval != RL_FOUND) {
		return retval;
	}
	list = _list;
	unsigned char *tmp_data;
	unsigned char *data = NULL;
	long node_number = list->left, i, pos = 0, to_copy;
	while (node_number > 0) {
		retval = rl_read(db, list->type->list_node_type, node_number, list, &_node, 0);
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
		rl_list_node_nocache_destroy(db, node);
		node = NULL;
	}
	*_data = data;
	retval = RL_OK;
cleanup:
	rl_list_nocache_destroy(db, list);
	if (node) {
		rl_list_node_nocache_destroy(db, node);
	}
	return retval;
}

int rl_multi_string_set(struct rlite *db, long *number, const unsigned char *data, long size)
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

int rl_multi_string_sha1(struct rlite *db, unsigned char digest[20], long number)
{
	unsigned char *data;
	long datalen;
	SHA_CTX sha;
	SHA1_Init(&sha);

	void *tmp;
	rl_list *list;
	rl_list_iterator *iterator;
	int retval = rl_read(db, &rl_data_type_list_long, number, &list_long, &tmp, 0);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	list = tmp;

	retval = rl_list_iterator_create(db, &iterator, list, 1);
	if (retval != RL_OK) {
		goto cleanup;
	}

	long size = 0;
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		if (size == 0) {
			size = *(long *)tmp;
			free(tmp);
			continue;
		}
		retval = rl_string_get(db, &data, *(long *)tmp);
		if (retval != RL_OK) {
			goto cleanup;
		}
		datalen = size > db->page_size ? db->page_size : size;
		SHA1_Update(&sha, data, datalen);
		size -= datalen;
		free(tmp);
	}

	retval = rl_list_iterator_destroy(db, iterator);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_list_nocache_destroy(db, list);
	if (retval != RL_OK) {
		goto cleanup;
	}

	SHA1_Final(digest, &sha);
cleanup:
	return RL_OK;
}
