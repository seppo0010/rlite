#include <stdlib.h>
#include <string.h>
#include "rlite/sha1.h"
#include "rlite/rlite.h"
#include "rlite/page_list.h"
#include "rlite/page_string.h"
#include "rlite/page_multi_string.h"
#include "rlite/util.h"

int rl_normalize_string_range(long totalsize, long *start, long *stop)
{
	if (*start < 0) {
		*start += totalsize;
		if (*start < 0) {
			*start = 0;
		}
	}
	if (*stop < 0) {
		*stop += totalsize;
		if (*stop < 0) {
			*stop = 0;
		}
	}
	if (*start >= totalsize) {
		*start = totalsize;
	}
	if (*stop >= totalsize) {
		*stop = totalsize - 1;
	}
	return RL_OK;
}

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

static int append(struct rlite *db, rl_list *list, long list_page_number, const unsigned char *data, long size)
{
	int retval = RL_OK;
	long *page = NULL;
	long pos = 0, to_copy;
	unsigned char *string = NULL;
	while (pos < size) {
		RL_MALLOC(page, sizeof(*page));
		RL_CALL(rl_string_create, RL_OK, db, &string, page);
		to_copy = db->page_size;
		if (pos + to_copy > size) {
			to_copy = size - pos;
		}
		memcpy(string, &data[pos], sizeof(unsigned char) * to_copy);
		string = NULL;
		RL_CALL(rl_list_add_element, RL_OK, db, list, list_page_number, page, -1);
		page = NULL;
		pos += to_copy;
	}
cleanup:
	rl_free(string);
	rl_free(page);
	return retval;
}

int rl_multi_string_append(struct rlite *db, long number, const unsigned char *data, long datasize, long *newlength)
{
	rl_list *list = NULL;
	unsigned char *tmp_data;
	void *tmp;
	int retval;
	long size, cpsize;
	long string_page_number;

	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &tmp, 0);
	list = tmp;

	RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, 0);
	size = *(long *)tmp;
	RL_MALLOC(tmp, sizeof(long));
	*(long *)tmp = size + datasize;
	RL_CALL(rl_list_add_element, RL_OK, db, list, number, tmp, 0);
	RL_CALL(rl_list_remove_element, RL_OK, db, list, number, 1);
	if (newlength) {
		*newlength = size + datasize;
	}

	size = size % db->page_size;

	if (size > 0) {
		RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, -1);
		string_page_number = *(long *)tmp;
		RL_CALL(rl_string_get, RL_OK, db, &tmp_data, string_page_number);
		cpsize = db->page_size - size;
		if (cpsize > datasize) {
			cpsize = datasize;
		}
		memcpy(&tmp_data[size], data, cpsize * sizeof(unsigned char));
		RL_CALL(rl_write, RL_OK, db, &rl_data_type_string, string_page_number, tmp_data);
		data = &data[cpsize];
		datasize -= cpsize;
	}

	if (datasize > 0) {
		RL_CALL(append, RL_OK, db, list, number, data, datasize);
	}

	retval = RL_OK;
cleanup:
	return retval;
}

int rl_multi_string_cpyrange(struct rlite *db, long number, unsigned char *data, long *_size, long start, long stop)
{
	long totalsize;
	rl_list *list = NULL;
	rl_list_node *node = NULL;
	void *_list, *tmp;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &_list, 0);
	list = _list;
	unsigned char *tmp_data;
	long i, pos = 0, pagesize, pagestart;
	long size;

	RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, 0);
	totalsize = *(long *)tmp;
	if (totalsize == 0) {
		if (_size) {
			*_size = 0;
		}
		retval = RL_OK;
		goto cleanup;
	}
	rl_normalize_string_range(totalsize, &start, &stop);
	if (stop < start) {
		if (_size) {
			*_size = 0;
		}
		retval = RL_OK;
		goto cleanup;
	}
	size = stop - start + 1;
	if (_size) {
		*_size = size;
	}

	i = start / db->page_size;
	pagestart = start % db->page_size;
	// pos = i * db->page_size + pagestart;
	// the first element in the list is the length of the array, skip to the second
	for (i++; i < list->size; i++) {
		RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, i);
		RL_CALL(rl_string_get, RL_OK, db, &tmp_data, *(long *)tmp);
		pagesize = db->page_size - pagestart;
		if (pos + pagesize > size) {
			pagesize = size - pos;
		}
		memcpy(&data[pos], &tmp_data[pagestart], sizeof(unsigned char) * pagesize);
		pos += pagesize;
		pagestart = 0;
	}
	retval = RL_OK;
cleanup:
	if (list) {
		rl_list_nocache_destroy(db, list);
	}
	if (node) {
		rl_list_node_nocache_destroy(db, node);
	}
	return retval;
}

int rl_multi_string_getrange(struct rlite *db, long number, unsigned char **_data, long *size, long start, long stop)
{
	long totalsize;
	rl_list *list = NULL;
	rl_list_node *node = NULL;
	void *_list, *tmp;
	unsigned char *data = NULL;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &_list, 0);
	list = _list;
	unsigned char *tmp_data;
	long i, pos = 0, pagesize, pagestart;

	RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, 0);
	totalsize = *(long *)tmp;
	if (totalsize == 0) {
		*size = 0;
		if (_data) {
			*_data = NULL;
		}
		retval = RL_OK;
		goto cleanup;
	}
	rl_normalize_string_range(totalsize, &start, &stop);
	if (stop < start) {
		*size = 0;
		if (_data) {
			*_data = NULL;
		}
		retval = RL_OK;
		goto cleanup;
	}
	*size = stop - start + 1;
	if (!_data) {
		retval = RL_OK;
		goto cleanup;
	}

	RL_MALLOC(data, sizeof(unsigned char) * (*size + 1));

	i = start / db->page_size;
	pagestart = start % db->page_size;
	// pos = i * db->page_size + pagestart;
	// the first element in the list is the length of the array, skip to the second
	for (i++; i < list->size; i++) {
		RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, i);
		RL_CALL(rl_string_get, RL_OK, db, &tmp_data, *(long *)tmp);
		pagesize = db->page_size - pagestart;
		if (pos + pagesize > *size) {
			pagesize = *size - pos;
		}
		memcpy(&data[pos], &tmp_data[pagestart], sizeof(unsigned char) * pagesize);
		pos += pagesize;
		pagestart = 0;
	}
	data[*size] = 0;
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

int rl_multi_string_get(struct rlite *db, long number, unsigned char **_data, long *size)
{
	return rl_multi_string_getrange(db, number, _data, size, 0, -1);
}

int rl_multi_string_cpy(struct rlite *db, long number, unsigned char *data, long *size)
{
	return rl_multi_string_cpyrange(db, number, data, size, 0, -1);
}

int rl_multi_string_set(struct rlite *db, long *number, const unsigned char *data, long size)
{
	int retval;
	long *page = NULL;
	rl_list *list = NULL;
	RL_CALL(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	*number = db->next_empty_page;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_list_long, *number, list);
	RL_MALLOC(page, sizeof(*page));
	*page = size;
	RL_CALL(rl_list_add_element, RL_OK, db, list, *number, page, -1);
	page = NULL;
	RL_CALL(append, RL_OK, db, list, *number, data, size);
cleanup:
	return retval;
}
int rl_multi_string_setrange(struct rlite *db, long number, const unsigned char *data, long size, long offset, long *newlength)
{
	long oldsize, newsize;
	rl_list *list = NULL;
	void *_list, *tmp;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, number, &rl_list_type_long, &_list, 1);
	list = _list;
	unsigned char *tmp_data;
	long i, pagesize, pagestart, page;
	if (offset < 0) {
		retval = RL_INVALID_PARAMETERS;
		goto cleanup;
	}

	RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, 0);
	oldsize = *(long *)tmp;
	if (oldsize > offset) {
		i = offset / db->page_size;
		pagestart = offset % db->page_size;

		newsize = offset + size;
		if (newsize > (list->size - 1) * db->page_size) {
			newsize = (list->size - 1) * db->page_size;
		}
		if (newsize > oldsize) {
			RL_CALL(rl_list_remove_element, RL_OK, db, list, number, 0);
			RL_MALLOC(tmp, sizeof(long));
			*(long *)tmp = newsize;
			RL_CALL(rl_list_add_element, RL_OK, db, list, number, tmp, 0);
		}

		for (i++; size > 0 && i < list->size; i++) {
			RL_CALL(rl_list_get_element, RL_FOUND, db, list, &tmp, i);
			page = *(long *)tmp;
			RL_CALL(rl_string_get, RL_OK, db, &tmp_data, page);
			pagesize = db->page_size - pagestart;
			if (pagesize > size) {
				pagesize = size;
			}
			memcpy(&tmp_data[pagestart], data, sizeof(char) * pagesize);
			RL_CALL(rl_write, RL_OK, db, &rl_data_type_string, page, tmp_data);

			// if there's more bytes that did not enter in this page it will be caught with an append
			if (oldsize < pagesize + pagestart) {
				oldsize = pagesize + pagestart;
			}
			data += pagesize;
			offset += pagesize;
			size -= pagesize;
			pagestart = 0;
		}
		if (newlength) {
			*newlength = oldsize;
		}
	}

	if (oldsize < offset) {
		RL_MALLOC(tmp_data, sizeof(char) * (size + offset - oldsize));
		memset(tmp_data, 0, offset - oldsize);
		memcpy(&tmp_data[offset - oldsize], data, sizeof(char) * size);
		RL_CALL(rl_multi_string_append, RL_OK, db, number, tmp_data, size + offset - oldsize, newlength);
		rl_free(tmp_data);
	}
	else if (oldsize == offset && size > 0) {
		RL_CALL(rl_multi_string_append, RL_OK, db, number, data, size, newlength);
	}
	retval = RL_OK;
cleanup:
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
