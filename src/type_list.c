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

int rl_lpush(struct rlite *db, const unsigned char *key, long keylen, int valuec, unsigned char **values, long *valueslen, long *size)
{
	rl_list *list;
	long list_page_number;
	int retval, i;
	long *value = NULL;
	RL_CALL(rl_llist_get_objects, RL_OK, db, key, keylen, &list_page_number, &list, 1);
	for (i = 0; i < valuec; i++) {
		RL_MALLOC(value, sizeof(*value));
		RL_CALL(rl_multi_string_set, RL_OK, db, value, values[i], valueslen[i]);
		RL_CALL(rl_list_add_element, RL_OK, db, list, list_page_number, value, 0);
	}
	*size = list->size;
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
