#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "page_string.h"

int rl_serialize_string(rlite *db, void *obj, unsigned char *data)
{
	memcpy(data, obj, sizeof(char) * db->page_size);
	return RL_OK;
}

int rl_deserialize_string(rlite *db, void **obj, void *context, unsigned char *data)
{
	context = context;
	unsigned char *new_data = malloc(sizeof(char) * db->page_size);
	if (!new_data) {
		return RL_OUT_OF_MEMORY;
	}
	memcpy(new_data, data, sizeof(char) * db->page_size);
	*obj = new_data;
	return RL_OK;
}

int rl_destroy_string(rlite *db, void *obj)
{
	db = db;
	free(obj);
	return RL_OK;
}

int rl_string_create(rlite *db, unsigned char **_data, long *number)
{
	unsigned char *data = malloc(sizeof(char) * db->page_size);
	if (!data) {
		return RL_OUT_OF_MEMORY;
	}
	*number = db->next_empty_page;
	int retval = rl_write(db, &rl_data_type_string, db->next_empty_page, data);
	if (retval != RL_OK) {
		return retval;
	}
	*_data = data;
	return RL_OK;
}

int rl_string_get(rlite *db, unsigned char **_data, long number)
{
	void *data;
	int retval = rl_read(db, &rl_data_type_string, number, NULL, &data);
	if (retval != RL_FOUND) {
		return retval;
	}
	*_data = data;
	return RL_OK;
}
