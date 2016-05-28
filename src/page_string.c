#include <stdlib.h>
#include <string.h>
#include "rlite/rlite.h"
#include "rlite/page_string.h"
#include "rlite/util.h"

int rl_string_serialize(rlite *db, void *obj, unsigned char *data)
{
	memcpy(data, obj, sizeof(char) * db->page_size);
	return RL_OK;
}

int rl_string_deserialize(rlite *db, void **obj, void *UNUSED(context), unsigned char *data)
{
	int retval;
	unsigned char *new_data;
	RL_MALLOC(new_data, sizeof(char) * db->page_size);
	memcpy(new_data, data, sizeof(char) * db->page_size);
	*obj = new_data;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_string_destroy(rlite *UNUSED(db), void *obj)
{
	rl_free(obj);
	return RL_OK;
}

int rl_string_create(rlite *db, unsigned char **_data, long *number)
{
	unsigned char *data = calloc(db->page_size, sizeof(char));
	if (!data) {
		return RL_OUT_OF_MEMORY;
	}
	*number = db->next_empty_page;
	int retval;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_string, db->next_empty_page, data);
	*_data = data;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_string_get(rlite *db, unsigned char **_data, long number)
{
	void *data;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_string, number, NULL, &data, 1);
	*_data = data;
	retval = RL_OK;
cleanup:
	return retval;
}
