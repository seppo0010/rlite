#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "page_btree.h"
#include "page_list.h"
#include "page_key.h"
#include "page_multi_string.h"

struct page_key {
	rlite *db;
	long keylen;
	unsigned char *key;
};

static int rl_key_get_list(rlite *db, unsigned char *key, long keylen, int create, rl_list **list)
{
	rl_btree *btree;
	void *_list;
	long keypage;
	int retval = rl_get_key_btree(db, &btree);
	if (retval != RL_OK) {
		goto cleanup;
	}
	retval = rl_get_key(db, key, keylen, &keypage);
	if (retval != RL_FOUND) {
		if (retval == RL_NOT_FOUND && create) {
			retval = rl_list_create(db, list, &list_key);
			if (retval != RL_OK) {
				goto cleanup;
			}
			keypage = db->next_empty_page;
			retval = rl_write(db, &rl_data_type_list_key, keypage, *list);
			if (retval != RL_OK) {
				goto cleanup;
			}
			retval = rl_set_key(db, key, keylen, keypage);
			if (retval != RL_OK) {
				goto cleanup;
			}
		}
		else {
			goto cleanup;
		}
	}
	else {
		retval = rl_read(db, &rl_data_type_list_key, keypage, &list_key, &_list, 1);
		if (retval != RL_FOUND) {
			if (retval == RL_NOT_FOUND && create) {
				retval = rl_list_create(db, list, &list_key);
				if (retval != RL_OK) {
					goto cleanup;
				}
				retval = rl_write(db, &rl_data_type_list_key, db->next_empty_page, *list);
				if (retval != RL_OK) {
					goto cleanup;
				}
			}
			else {
				goto cleanup;
			}
		}
		else {
			*list = _list;
		}
	}
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_key_get(struct rlite *db, unsigned char *key, long keylen, unsigned char *type, long *page)
{
	rl_list *list = NULL;
	int retval = rl_key_get_list(db, key, keylen, 0, &list);
	if (retval != RL_OK) {
		goto cleanup;
	}
	if (list == NULL) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	struct page_key search_key;
	search_key.db = db;
	search_key.keylen = keylen;
	search_key.key = key;
	void *_element;
	retval = rl_list_find_element(db, list, &search_key, &_element, NULL, NULL, NULL);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	rl_key *element = _element;
	*type = element->type;
	*page = element->value_page;
cleanup:
	return retval;
}

int rl_key_set(struct rlite *db, unsigned char *key, long keylen, unsigned char type, long page)
{
	rl_list *list = NULL;
	int retval = rl_key_get_list(db, key, keylen, 1, &list);
	if (retval != RL_OK) {
		goto cleanup;
	}
	if (list == NULL) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	struct page_key search_key;
	search_key.db = db;
	search_key.keylen = keylen;
	search_key.key = key;
	void *_element;
	rl_list_node *node;
	long node_page;
	retval = rl_list_find_element(db, list, &search_key, &_element, NULL, &node, &node_page);
	if (retval == RL_FOUND) {
		rl_key *element = _element;
		element->value_page = page;
		rl_write(db, list->type->list_node_type, node_page, node);
		retval = RL_OK;
		goto cleanup;
	}
	else if (retval != RL_NOT_FOUND) {
		goto cleanup;
	}

	long string_page;
	retval = rl_multi_string_set(db, &string_page, key, keylen);
	if (retval != RL_OK) {
		goto cleanup;
	}

	rl_key *new_key = malloc(sizeof(rl_key));
	new_key->type = type;
	new_key->string_page = string_page;
	new_key->value_page = page;
	retval = rl_list_add_element(db, list, new_key, 0);
	if (retval != RL_OK) {
		goto cleanup;
	}
cleanup:
	return retval;
}

int rl_key_get_or_create(struct rlite *db, unsigned char *key, long keylen, unsigned char type, long *page)
{
	unsigned char existing_type;
	int retval = rl_key_get(db, key, keylen, &existing_type, page);
	if (retval == RL_FOUND) {
		if (existing_type != type) {
			return RL_WRONG_TYPE;
		}
	}
	else if (retval == RL_NOT_FOUND) {
		rl_alloc_page_number(db, page);
		retval = rl_key_set(db, key, keylen, type, *page);
		if (retval != RL_OK) {
			return retval;
		}
		retval = RL_NOT_FOUND;

	}
	return retval;
}

int rl_key_delete(struct rlite *db, unsigned char *key, long keylen)
{
	rl_list *list = NULL;
	int retval = rl_key_get_list(db, key, keylen, 0, &list);
	if (retval != RL_OK) {
		goto cleanup;
	}
	if (list == NULL) {
		retval = RL_UNEXPECTED;
		goto cleanup;
	}
	struct page_key search_key;
	search_key.db = db;
	search_key.keylen = keylen;
	search_key.key = key;
	void *_element;
	long position;
	retval = rl_list_find_element(db, list, &search_key, &_element, &position, NULL, NULL);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	retval = rl_list_remove_element(db, list, position);
	if (retval != RL_FOUND) {
		goto cleanup;
	}
	// TODO: delete value, key string
cleanup:
	return RL_OK;
}

int key_cmp(void *v1, void *v2)
{
	rl_key *key1 = v1;
	struct page_key *key2 = v2;
	rlite *db = key2->db;

	unsigned char *data;
	long size;
	int retval = rl_multi_string_get(db, key1->string_page, &data, &size);
	if (retval != RL_OK) {
		data = NULL;
		fprintf(stderr, "Failed to read string value to cmp\n");
		goto cleanup;
	}
	if (size == key2->keylen) {
		retval = memcmp(data, key2->key, size);
		goto cleanup;
	}
	retval = memcmp(data, key2->key, size < key2->keylen ? size : key2->keylen);
	if (retval == 0) {
		retval = size > key2->keylen ? 1 : -1;
		goto cleanup;
	}
cleanup:
	free(data);
	return retval;
}
