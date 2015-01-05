#include <stdlib.h>
#include "rlite.h"
#include "page_multi_string.h"
#include "type_list.h"
#include "page_list.h"
#include "util.h"

static int rl_llist_create(rlite *db, long list_page, rl_list **_list)
{
	rl_list *list = NULL;

	int retval;
	RL_CALL(rl_list_create, RL_OK, db, &list, &rl_list_type_long);
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_list_long, list_page, list);

	if (_list) {
		*_list = list;
	}
cleanup:
	return retval;
}

static int rl_llist_read(rlite *db, long list_page_number, rl_list **list)
{
	void *tmp;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, list_page_number, &rl_list_type_long, &tmp, 1);
	*list = tmp;
	retval = RL_OK;
cleanup:
	return retval;
}

static int rl_llist_get_objects(rlite *db, const unsigned char *key, long keylen, long *_list_page_number, rl_list **list, int create)
{
	long list_page_number;
	int retval;
	if (create) {
		retval = rl_key_get_or_create(db, key, keylen, RL_TYPE_LIST, &list_page_number);
		if (retval != RL_FOUND && retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		else if (retval == RL_NOT_FOUND) {
			retval = rl_llist_create(db, list_page_number, list);
		}
		else {
			retval = rl_llist_read(db, list_page_number, list);
		}
	}
	else {
		unsigned char type;
		retval = rl_key_get(db, key, keylen, &type, NULL, &list_page_number, NULL);
		if (retval != RL_FOUND) {
			goto cleanup;
		}
		if (type != RL_TYPE_LIST) {
			retval = RL_WRONG_TYPE;
			goto cleanup;
		}
		retval = rl_llist_read(db, list_page_number, list);
	}
	if (_list_page_number) {
		*_list_page_number = list_page_number;
	}
cleanup:
	return retval;
}

int rl_lpush(struct rlite *db, const unsigned char *key, long keylen, int create, int valuec, unsigned char **values, long *valueslen, long *size)
{
	rl_list *list;
	long list_page_number;
	int retval, i;
	long *value = NULL;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page_number, &list, create ? 1 : 0);
	for (i = 0; i < valuec; i++) {
		RL_MALLOC(value, sizeof(*value));
		RL_CALL(rl_multi_string_set, RL_OK, db, value, values[i], valueslen[i]);
		RL_CALL(rl_list_add_element, RL_OK, db, list, list_page_number, value, 0);
	}
	if (size) {
		*size = list->size;
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_llen(struct rlite *db, const unsigned char *key, long keylen, long *len)
{
	rl_list *list;
	int retval;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, NULL, &list, 0);
	*len = list->size;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_lpop(struct rlite *db, const unsigned char *key, long keylen, unsigned char **value, long *valuelen)
{
	rl_list *list;
	int retval;
	void *tmp;
	long page, list_page;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page, &list, 0);
	RL_CALL(rl_list_get_element, RL_FOUND, db, list, (void **)&tmp, 0);
	page = *(long *)tmp;
	retval = rl_list_remove_element(db, list, list_page, 0);
	if (retval == RL_DELETED) {
		RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	}
	else if (retval != RL_OK) {
		goto cleanup;
	}
	RL_CALL(rl_multi_string_get, RL_OK, db, page, value, valuelen);
	RL_CALL(rl_multi_string_delete, RL_OK, db, page);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_lindex(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char **value, long *valuelen)
{
	rl_list *list;
	int retval;
	void *tmp;
	long page, list_page;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page, &list, 0);
	RL_CALL(rl_list_get_element, RL_FOUND, db, list, (void **)&tmp, index);
	page = *(long *)tmp;
	RL_CALL(rl_multi_string_get, RL_OK, db, page, value, valuelen);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_lrange(struct rlite *db, const unsigned char *key, long keylen, long start, long stop, long *size, unsigned char ***_values, long **_valueslen)
{
	rl_list *list;
	rl_list_iterator *iterator;
	int retval;
	long len;
	long i;
	void *tmp = NULL;
	unsigned char **values = NULL;
	long *valueslen = NULL;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, NULL, &list, 0);
	len = list->size;

	if (start < 0) {
		start += len;
		if (start < 0) {
			start = 0;
		}
	}

	if (stop < 0) {
		stop += len;
		if (stop < 0) {
			stop = 0;
		}
	}
	if (stop >= len) {
		stop = len - 1;
	}
	if (start > stop) {
		*size = 0;
		retval = RL_OK;
		goto cleanup;
	}

	*size = stop - start + 1;

	RL_MALLOC(values, sizeof(unsigned char *) * (*size));
	RL_MALLOC(valueslen, sizeof(unsigned char *) * (*size));
	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);
	i = 0;
	while (i <= stop && (retval = rl_list_iterator_next(iterator, i >= start ? &tmp : NULL)) == RL_OK) {
		if (tmp) {
			RL_CALL(rl_multi_string_get, RL_OK, db, *(long *)tmp, &values[i - start], &valueslen[i - start]);
			rl_free(tmp);
		}
		i++;
	}

	if (retval != RL_END) {
		rl_list_iterator_destroy(db, iterator);
		if (retval != RL_OK) {
			goto cleanup;
		}
	}

	*_values = values;
	*_valueslen = valueslen;
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		rl_free(values);
		rl_free(valueslen);
	}
	return retval;
}

int rl_linsert(struct rlite *db, const unsigned char *key, long keylen, int after, unsigned char *pivot, long pivotlen, unsigned char *value, long valuelen, long *size)
{
	rl_list *list;
	rl_list_iterator *iterator;
	int retval, cmp;
	void *tmp;
	long list_page, *value_page;
	long member;
	long pos = 0;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page, &list, 0);
	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		member = *(long *)tmp;
		rl_free(tmp);
		RL_CALL(rl_multi_string_cmp_str, RL_OK, db, member, pivot, pivotlen, &cmp);
		if (cmp == 0) {
			retval = RL_FOUND;
			break;
		}
		pos++;
	}

	if (retval == RL_END) {
		retval = RL_NOT_FOUND;
	}
	else {
		rl_list_iterator_destroy(db, iterator);
	}
	if (retval == RL_FOUND) {
		RL_MALLOC(value_page, sizeof(*value_page));
		RL_CALL(rl_multi_string_set, RL_OK, db, value_page, value, valuelen);
		RL_CALL(rl_list_add_element, RL_OK, db, list, list_page, value_page, pos + (after ? 1 : 0));
		retval = RL_OK;
	}
	if (size) {
		*size = list->size;
	}
cleanup:
	return retval;
}

static int search(struct rlite *db, rl_list *list, unsigned char *value, long valuelen, long start, int direction, long *page, long *position)
{
	rl_list_iterator *iterator;
	int retval, cmp;
	long member;
	long pos = direction > 0 ? 0 : list->size - 1;
	void *tmp = NULL;
	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, direction);
	while ((retval = rl_list_iterator_next(iterator, ((direction > 0 && pos >= start) || (direction < 0 && pos <= start)) ? &tmp : NULL)) == RL_OK) {
		if (tmp == NULL) {
			pos += direction;
			continue;
		}
		member = *(long *)tmp;
		rl_free(tmp);
		RL_CALL(rl_multi_string_cmp_str, RL_OK, db, member, value, valuelen, &cmp);
		if (cmp == 0) {
			if (page) {
				*page = member;
			}
			*position = pos;
			retval = RL_FOUND;
			break;
		}
		else {
			pos += direction;
		}
	}

	if (retval != RL_END) {
		rl_list_iterator_destroy(db, iterator);
	}
	else {
		retval = RL_NOT_FOUND;
	}
cleanup:
	return retval;
}

int rl_lrem(struct rlite *db, const unsigned char *key, long keylen, int direction, long maxcount, unsigned char *value, long valuelen, long *_count)
{
	rl_list *list;
	int retval;
	long list_page, count = 0, pos, page = 0;

	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page, &list, 0);
	pos = direction > 0 ? 0 : list->size - 1;
	if (maxcount == 0) {
		maxcount = list->size;
	}

	while (1) {
		retval = search(db, list, value, valuelen, pos, direction, &page, &pos);
		if (retval == RL_NOT_FOUND) {
			break;
		}
		else if (retval == RL_FOUND) {
			count++;
			RL_CALL(rl_multi_string_delete, RL_OK, db, page);
			retval = rl_list_remove_element(db, list, list_page, pos);
			if (retval == RL_DELETED) {
				RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
				retval = RL_DELETED;
				break;
			}
			else if (retval != RL_OK) {
				goto cleanup;
			}
			if (direction < 0) {
				// this element was already skipped
				pos += direction;
			}
			if (count == maxcount) {
				break;
			}
		}
		else {
			goto cleanup;
		}
	}
	*_count = count;
	if (retval != RL_DELETED) {
		retval = RL_OK;
	}
cleanup:
	return retval;
}

int rl_lset(struct rlite *db, const unsigned char *key, long keylen, long index, unsigned char *value, long valuelen)
{
	rl_list *list;
	int retval;
	long list_page, *value_page;
	void *tmp;

	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page, &list, 0);

	// first add the new one and then delete the old one
	// we could instead add an update to page_list
	// but don't delete first since we might delete the only element

	if (index < 0) {
		index += list->size;
		if (index < 0) {
			retval = RL_NOT_FOUND;
			goto cleanup;
		}
	}
	if (index >= list->size) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}

	RL_MALLOC(value_page, sizeof(*value_page));
	RL_CALL(rl_multi_string_set, RL_OK, db, value_page, value, valuelen);
	RL_CALL(rl_list_add_element, RL_OK, db, list, list_page, value_page, index + 1);

	RL_CALL(rl_list_get_element, RL_FOUND, db, list, (void **)&tmp, index);
	RL_CALL(rl_multi_string_delete, RL_OK, db, *(long *)tmp);
	RL_CALL(rl_list_remove_element, RL_OK, db, list, list_page, index);
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_llist_pages(struct rlite *db, long page, short *pages)
{
	rl_list *list;
	rl_list_iterator *iterator;
	int retval;
	void *tmp;
	long member;

	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_list_long, page, &rl_list_type_long, &tmp, 1);
	list = tmp;

	RL_CALL(rl_list_pages, RL_OK, db, list, pages);

	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		member = *(long *)tmp;
		pages[member] = 1;
		RL_CALL(rl_multi_string_pages, RL_OK, db, member, pages);
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}
	retval = RL_OK;
cleanup:
	if (retval != RL_OK) {
		if (iterator) {
			rl_list_iterator_destroy(db, iterator);
		}
	}
	return retval;
}

int rl_llist_delete(rlite *db, const unsigned char *key, long keylen)
{
	long list_page_number = 0;
	rl_list *list = NULL;
	rl_list_iterator *iterator;
	long member;
	int retval;
	void *tmp;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page_number, &list, 0);
	RL_CALL(rl_list_iterator_create, RL_OK, db, &iterator, list, 1);
	while ((retval = rl_list_iterator_next(iterator, &tmp)) == RL_OK) {
		member = *(long *)tmp;
		rl_multi_string_delete(db, member);
		rl_free(tmp);
	}
	iterator = NULL;

	if (retval != RL_END) {
		goto cleanup;
	}

	RL_CALL(rl_list_delete, RL_OK, db, list);
	RL_CALL(rl_delete, RL_OK, db, list_page_number);
	RL_CALL(rl_key_delete, RL_OK, db, key, keylen);
	retval = RL_OK;
cleanup:
	return retval;
}
