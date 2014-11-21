#include <stdlib.h>
#include <string.h>
#include "rlite.h"
#include "page_long.h"
#include "util.h"

int rl_long_serialize(rlite *UNUSED(db), void *obj, unsigned char *data)
{
	put_4bytes(data, *(long *)obj);
	return RL_OK;
}

int rl_long_deserialize(rlite *UNUSED(db), void **obj, void *UNUSED(context), unsigned char *data)
{
	int retval;
	long *value;
	RL_MALLOC(value, sizeof(long));
	*value = get_4bytes(data);
	*obj = value;
	retval = RL_OK;
cleanup:
	return retval;
}

int rl_long_destroy(rlite *UNUSED(db), void *obj)
{
	free(obj);
	return RL_OK;
}

int rl_long_set(rlite *db, long value, long number)
{
	int retval;
	long *val;
	RL_MALLOC(val, sizeof(*val));
	*val = value;
	RL_CALL(rl_write, RL_OK, db, &rl_data_type_long, number, val);

	retval = RL_OK;
cleanup:
	return retval;
}

int rl_long_create(struct rlite *db, long value, long *number)
{
	if (number) {
		*number = db->next_empty_page;
	}
	return rl_long_set(db, value, db->next_empty_page);
}

int rl_long_get(rlite *db, long *value, long number)
{
	void *data;
	int retval;
	RL_CALL(rl_read, RL_FOUND, db, &rl_data_type_long, number, NULL, &data, 1);
	*value = *(long *)data;
	retval = RL_OK;
cleanup:
	return retval;
}
