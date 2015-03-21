#include "rlite.h"
#include "util.h"
#include "fifo.h"

#define FILENAME_LENGTH 8

static void generate_subscriptor_id(rlite *db) {
	// TODO: check for collisions and retry?
	// this is probably the worst random hash in the history of programming
	char str[60];
	unsigned char digest[20];
	memset(str, 0, 60);
	snprintf(str, 60, "%llu", rl_mstime());
	snprintf(&str[40], 20, "%d", rand());
	sha1((unsigned char *)str, 60, digest);
	sha1_formatter(digest, &db->subscriptor_id, NULL);
}

int rl_subscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen)
{
	int i, retval;
	long identifierlen[1] = {40};
	if (db->subscriptor_id == NULL) {
		generate_subscriptor_id(db);
		if (db->subscriptor_id == NULL) {
			retval = RL_OUT_OF_MEMORY;
			goto cleanup;
		}
	}
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS);
	for (i = 0; i < channelc; i++) {
		RL_CALL(rl_sadd, RL_OK, db, channelv[i], channelvlen[i], 1, (unsigned char **)&db->subscriptor_id, identifierlen, NULL);
	}
	// important! commit to release the exclusive lock
	RL_CALL(rl_commit, RL_OK, db);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_poll(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen)
{
	int retval;
	unsigned char *len = NULL, i;
	long lenlen;
	unsigned char **elements = NULL;
	long *elementslen = NULL;
	if (db->subscriptor_id == NULL) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	RL_CALL(rl_refresh, RL_OK, db);
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_MESSAGES);
	RL_CALL(rl_pop, RL_OK, db, (unsigned char *)db->subscriptor_id, 40, &len, &lenlen, 1);
	if (lenlen != 1) {
		// TODO: delete the key and commit? PANIC
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	*elementc = len[0];
	RL_MALLOC(elements, sizeof(unsigned char *) * len[0]);
	RL_MALLOC(elementslen, sizeof(long) * len[0]);
	for (i = 0; i < len[0]; i++) {
		RL_CALL(rl_pop, RL_OK, db, (unsigned char *)db->subscriptor_id, 40, &elements[i], &elementslen[i], 1);
	}
	*_elements = elements;
	*_elementslen = elementslen;
cleanup:
	rl_free(len);
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen, long *_recipients)
{
	unsigned char *value = NULL;
	long valuelen, recipients = 0;
	int retval;
	unsigned char *values[4];
	long valueslen[4];
	rl_set_iterator *iterator;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS);
	RL_CALL(rl_smembers, RL_OK, db, &iterator, channel, channellen);

#define MESSAGE "message"
	unsigned char q = 3;
	values[0] = &q;
	valueslen[0] = 1;
	values[1] = (unsigned char *)MESSAGE;
	valueslen[1] = (long)strlen(MESSAGE);
	values[2] = channel;
	valueslen[2] = channellen;
	values[3] = (unsigned char *)data;
	valueslen[3] = datalen;

	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_MESSAGES);
	while ((retval = rl_set_iterator_next(iterator, &value, &valuelen)) == RL_OK) {
		recipients++;
		RL_CALL(rl_push, RL_OK, db, value, valuelen, 1, 0, 4, values, valueslen, NULL);
		rl_free(value);
		value = NULL;
	}
	if (retval == RL_END) {
		retval = RL_OK;
	}
	if (_recipients) {
		*_recipients = recipients;
	}
cleanup:
	rl_free(value);
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}
