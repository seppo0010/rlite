#include "rlite.h"
#include "util.h"
#include "fifo.h"

#define FILENAME_LENGTH 8

static char *get_filename(rlite *db, char *identifier)
{
	rl_file_driver *driver = db->driver;
	return rl_get_filename_with_suffix(driver->filename, identifier);
}

int rl_subscribe(rlite *db, unsigned char *channel, size_t channellen, char **data, size_t *datalen)
{
	unsigned char *identifier = NULL;
	char *filename = NULL;
	int retval, i;
	long added = 0, identifierlen = FILENAME_LENGTH;
	if (db->driver_type != RL_FILE_DRIVER) {
		return RL_NOT_IMPLEMENTED;
	}
	identifier = rl_malloc(sizeof(char) * (FILENAME_LENGTH + 1));
	identifier[0] = '.';
	identifier[FILENAME_LENGTH] = 0;

	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_PUBSUB);
	do {
		for (i = 1; i < FILENAME_LENGTH; i++) {
			identifier[i] = (rand() % 26) + 'a';
		}
		RL_CALL(rl_sadd, RL_OK, db, channel, channellen, 1, (unsigned char **)&identifier, &identifierlen, &added);
	} while (added == 0);
	// important! commit to release the exclusive lock
	RL_CALL(rl_commit, RL_OK, db);

	filename = get_filename(db, (char *)identifier);

	RL_CALL(rl_create_fifo, RL_OK, filename);
	RL_CALL(rl_read_fifo, RL_OK, filename, data, datalen);
cleanup:
	rl_free(identifier);
	rl_free(filename);
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen)
{
	if (db->driver_type != RL_FILE_DRIVER) {
		return RL_NOT_IMPLEMENTED;
	}
	char *filename = NULL;
	int retval;
	unsigned char *identifier = NULL;
	long identifierlen;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_PUBSUB);

	// will break when there's nothing to pop
	for (;;) {
		RL_CALL(rl_spop, RL_OK, db, channel, channellen, &identifier, &identifierlen);
		identifier = realloc(identifier, sizeof(char) * (identifierlen + 1));
		if (!identifier) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
		identifier[identifierlen] = 0;
		filename = get_filename(db, (char *)identifier);
		RL_CALL(rl_write_fifo, RL_OK, filename, data, datalen);
		rl_free(filename);
		filename = NULL;
		rl_free(identifier);
		identifier = NULL;
	}
cleanup:
	rl_free(filename);
	rl_free(identifier);
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
	}
	return retval;
}
