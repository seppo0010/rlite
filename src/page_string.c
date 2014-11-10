#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "page_string.h"
#include "util.h"

int rl_string_serialize(rlite *db, void *obj, unsigned char *data)
{
	memcpy(data, obj, sizeof(char) * db->page_size);
	return RL_OK;
}

int rl_string_deserialize(rlite *db, void **obj, void *UNUSED(context), unsigned char *data)
{
	unsigned char *new_data = malloc(sizeof(char) * db->page_size);
	if (!new_data) {
		return RL_OUT_OF_MEMORY;
	}
	memcpy(new_data, data, sizeof(char) * db->page_size);
	*obj = new_data;
	return RL_OK;
}

int rl_string_destroy(rlite *UNUSED(db), void *obj)
{
	free(obj);
	return RL_OK;
}

int rl_string_create(rlite *db, unsigned char **_data, long *number)
{
	unsigned char *data = calloc(db->page_size, sizeof(char));
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
	int retval = rl_read(db, &rl_data_type_string, number, NULL, &data, 1);
	if (retval != RL_FOUND) {
		return retval;
	}
	*_data = data;
	return RL_OK;
}
