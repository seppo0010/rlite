#include "rlite/rlite.h"
#include "rlite/util.h"
#include "rlite/signal.h"
#include "rlite/flock.h"
#include "rlite/pubsub.h"

#define ENSURE_SUBSCRIBER_ID(ret) \
	if (db->subscriber_id == NULL) {\
		generate_subscriber_id(db);\
		if (db->subscriber_id == NULL) {\
			return ret;\
		}\
	}

static void generate_subscriber_id(rlite *db);

static char *get_lock_filename(rlite *db, char *subscriber_id)
{
	char suffix[46];
	if (db->driver_type != RL_FILE_DRIVER) {
		return NULL;
	}
	rl_file_driver *driver = db->driver;
	// 40 come from subscriber_id, 5 ".lock" and then a null
	memcpy(suffix, subscriber_id, 40);
	memcpy(&suffix[40], ".lock", 6);
	return rl_get_filename_with_suffix(driver->filename, suffix);
}

static void generate_subscriber_id(rlite *db)
{
	// TODO: check for collisions and retry?
	// this is probably the worst random hash in the history of programming
	char str[60];
	unsigned char digest[20];
	memset(str, 0, 60);
	snprintf(str, 60, "%llu", rl_mstime());
	snprintf(&str[40], 20, "%d", rand());
	sha1((unsigned char *)str, 60, digest);
	sha1_formatter(digest, &db->subscriber_id, NULL);

	db->subscriber_lock_filename = get_lock_filename(db, db->subscriber_id);
	if (db->subscriber_lock_filename == NULL) {
		rl_free(db->subscriber_id);
		db->subscriber_id = NULL;
		return;
	}
	db->subscriber_lock_fp = fopen(db->subscriber_lock_filename, "w");
	if (db->subscriber_lock_fp == NULL) {
		rl_free(db->subscriber_lock_filename);
		rl_free(db->subscriber_id);
		db->subscriber_id = NULL;
		return;
	}
	int retval = rl_flock(db->subscriber_lock_fp, RLITE_FLOCK_EX);
	if (retval != RL_OK) {
		fclose(db->subscriber_lock_fp);
		remove(db->subscriber_lock_filename);
		rl_free(db->subscriber_lock_filename);
		rl_free(db->subscriber_id);
		db->subscriber_id = NULL;
	}
}

static char *get_signal_filename(rlite *db, char *subscriber_id)
{
	rl_file_driver *driver = db->driver;
	return rl_get_filename_with_suffix(driver->filename, subscriber_id);
}

static int do_subscribe(rlite *db, int internal_db_to_subscriber, int internal_db_to_subscription, int subscriptionc, unsigned char **subscriptionv, long *subscriptionvlen)
{
	int i, retval;
	long identifierlen[1] = {40};
	ENSURE_SUBSCRIBER_ID(RL_UNEXPECTED);
	RL_CALL(rl_select_internal, RL_OK, db, internal_db_to_subscriber);
	for (i = 0; i < subscriptionc; i++) {
		RL_CALL(rl_sadd, RL_OK, db, subscriptionv[i], subscriptionvlen[i], 1, (unsigned char **)&db->subscriber_id, identifierlen, NULL);
	}
	RL_CALL(rl_select_internal, RL_OK, db, internal_db_to_subscription);
	RL_CALL(rl_sadd, RL_OK, db, (unsigned char *)db->subscriber_id, identifierlen[0], subscriptionc, subscriptionv, subscriptionvlen, NULL);
	// important! commit to release the exclusive lock
	RL_CALL(rl_commit, RL_OK, db);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_subscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen)
{
	return do_subscribe(db, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS, channelc, channelv, channelvlen);
}

int rl_psubscribe(rlite *db, int patternc, unsigned char **patternv, long *patternvlen)
{
	return do_subscribe(db, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_PATTERNS, patternc, patternv, patternvlen);
}

static int do_unsubscribe(rlite *db, int internal_db_to_subscriber, int internal_db_to_subscription, int subscriptionc, unsigned char **subscriptionv, long *subscriptionvlen)
{
	int i, retval;
	long identifierlen[1] = {40};
	if (db->subscriber_id == NULL) {
		// if there's no subscriber id, then the connection is not subscribed to anything
		return RL_OK;
	}
	RL_CALL(rl_select_internal, RL_OK, db, internal_db_to_subscription);
	RL_CALL2(rl_srem, RL_OK, RL_NOT_FOUND, db, (unsigned char *)db->subscriber_id, identifierlen[0], subscriptionc, subscriptionv, subscriptionvlen, NULL);
	RL_CALL(rl_select_internal, RL_OK, db, internal_db_to_subscriber);
	for (i = 0; i < subscriptionc; i++) {
		RL_CALL2(rl_srem, RL_OK, RL_NOT_FOUND, db, subscriptionv[i], subscriptionvlen[i], 1, (unsigned char **)&db->subscriber_id, identifierlen, NULL);
	}
	// important! commit to release the exclusive lock
	RL_CALL(rl_commit, RL_OK, db);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_unsubscribe(rlite *db, int channelc, unsigned char **channelv, long *channelvlen)
{
	return do_unsubscribe(db, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS, channelc, channelv, channelvlen);
}

int rl_punsubscribe(rlite *db, int patternc, unsigned char **patternv, long *patternvlen)
{
	return do_unsubscribe(db, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_PATTERNS, patternc, patternv, patternvlen);
}

static int rl_unsubscribe_all_type(rlite *db, char *subscriber_id, int internal_db_to_subscriber, int internal_db_to_subscription)
{
	int i, retval;
	rl_set_iterator *iterator;
	int subscriptionc = 0;
	unsigned char **subscriptionv = NULL, *subscription = NULL;
	long *subscriptionvlen = NULL, subscriptionlen;
	RL_CALL(rl_select_internal, RL_OK, db, internal_db_to_subscription);
	RL_CALL(rl_smembers, RL_OK, db, &iterator, (unsigned char *)subscriber_id, 40);
	RL_MALLOC(subscriptionv, sizeof(char *) * iterator->size);
	RL_MALLOC(subscriptionvlen, sizeof(long) * iterator->size);
	subscriptionc = 0;
	while ((retval = rl_set_iterator_next(iterator, NULL, &subscription, &subscriptionlen)) == RL_OK) {
		subscriptionv[subscriptionc] = subscription;
		subscriptionvlen[subscriptionc] = subscriptionlen;
		subscriptionc++;
		subscription = NULL;
	}
	if (retval != RL_END) {
		goto cleanup;
	}
	RL_CALL(do_unsubscribe, RL_OK, db, internal_db_to_subscriber, internal_db_to_subscription, subscriptionc, subscriptionv, subscriptionvlen);
cleanup:
	if (subscriptionv) {
		for (i = 0; i < subscriptionc; i++) {
			rl_free(subscriptionv[i]);
		}
		rl_free(subscriptionv);
	}
	rl_free(subscriptionvlen);
	rl_free(subscription);
	return retval;
}

static int rl_unsubscribe_all_subscriber(rlite *db, char *subscriber_id)
{
	int retval;
	char *subscriber_lock_filename = NULL;

	RL_CALL2(rl_unsubscribe_all_type, RL_OK, RL_NOT_FOUND, db, subscriber_id, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS);
	RL_CALL2(rl_unsubscribe_all_type, RL_OK, RL_NOT_FOUND, db, subscriber_id, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS, RLITE_INTERNAL_DB_SUBSCRIBER_PATTERNS);

	subscriber_lock_filename = get_lock_filename(db, subscriber_id);
	char *filename = get_signal_filename(db, subscriber_id);
	remove(filename);
	rl_free(filename);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	if (retval == RL_NOT_FOUND) {
		retval = RL_OK;
	}
	rl_free(subscriber_lock_filename);
	return retval;
}

int rl_unsubscribe_all(rlite *db)
{
	if (db->subscriber_id == NULL) {
		// if there's no subscriber id, then the connection is not subscribed to anything
		return RL_OK;
	}
    rl_refresh(db);
	int retval = rl_unsubscribe_all_subscriber(db, db->subscriber_id);
	if (retval == RL_OK) {
		fclose(db->subscriber_lock_fp);
		rl_free(db->subscriber_id);
		db->subscriber_id = NULL;
	}
	return retval;
}

int rl_poll_wait(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen, struct timeval *timeout)
{
	int retval = rl_poll(db, elementc, _elements, _elementslen);
	if (retval == RL_NOT_FOUND) {
		// TODO: possible race condition between poll and signal read?
		// can we atomically do both without locking the database?
		ENSURE_SUBSCRIBER_ID(RL_UNEXPECTED);
		char *filename = get_signal_filename(db, db->subscriber_id);
		rl_discard(db);
		rl_create_signal(filename);
		rl_read_signal(filename, timeout, NULL, NULL);
		rl_free(filename);
		rl_refresh(db);
		retval = rl_poll(db, elementc, _elements, _elementslen);
	}
	return retval;
}

int rl_poll(rlite *db, int *elementc, unsigned char ***_elements, long **_elementslen)
{
	int retval;
	unsigned char *len = NULL, i;
	long lenlen;
	unsigned char **elements = NULL;
	long *elementslen = NULL;
	if (db->subscriber_id == NULL) {
		retval = RL_NOT_FOUND;
		goto cleanup;
	}
	RL_CALL(rl_refresh, RL_OK, db);
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_MESSAGES);
	RL_CALL(rl_pop, RL_OK, db, (unsigned char *)db->subscriber_id, 40, &len, &lenlen, 1);
	if (lenlen != 1) {
		// TODO: delete the key and commit? PANIC
		retval = RL_UNEXPECTED;
		goto cleanup;
	}

	*elementc = len[0];
	RL_MALLOC(elements, sizeof(unsigned char *) * len[0]);
	RL_MALLOC(elementslen, sizeof(long) * len[0]);
	for (i = 0; i < len[0]; i++) {
		RL_CALL(rl_pop, RL_OK, db, (unsigned char *)db->subscriber_id, 40, &elements[i], &elementslen[i], 1);
	}
	*_elements = elements;
	*_elementslen = elementslen;
	RL_CALL(rl_commit, RL_OK, db);
cleanup:
	rl_free(len);
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

static int do_publish(rlite *db, char *subscriber_id, long subscribed_idlen, int valuec, unsigned char *values[], long valueslen[])
{
	int retval;
	char *filename = NULL;
	filename = get_lock_filename(db, subscriber_id);
	if (rl_is_flocked(filename, RLITE_FLOCK_EX) == RL_NOT_FOUND) {
		// the client has "disconnected"; clean up for them
		RL_CALL(rl_unsubscribe_all_subscriber, RL_OK, db, subscriber_id);
		retval = RL_NOT_FOUND;
	} else {
		RL_CALL(rl_push, RL_OK, db, (unsigned char *)subscriber_id, subscribed_idlen, 1, 0, valuec, values, valueslen, NULL);

		rl_free(filename); // reusing variables?! WHO WROTE THIS?
		filename = get_signal_filename(db, subscriber_id);
		// this only signals the signal recipient that new data is available
		// - Maybe this should be executed AFTER the push was commited?
		// - Ah, we have an exclusive lock anyway, so they'll have to wait
		// when they poll
		rl_write_signal(filename, "1", 1);
	}
cleanup:
	rl_free(filename);
	return retval;
}

static int publish_to_members(rlite *db, rl_set_iterator *iterator, int valuec, unsigned char *values[], long valueslen[], long *recipients)
{
	int retval;
	unsigned char *value = NULL;
	void *tmp;
	long valuelen;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_MESSAGES);
	while ((retval = rl_set_iterator_next(iterator, NULL, &value, &valuelen)) == RL_OK) {
		RL_REALLOC(value, sizeof(unsigned char) * (valuelen + 1));
		value[valuelen] = 0;

		retval = do_publish(db, (char *)value, valuelen, valuec, values, valueslen);
		if (retval == RL_OK) {
			(*recipients)++;
		} else if (retval != RL_NOT_FOUND) {
			goto cleanup;
		}
		rl_free(value);
		value = NULL;
	}
	if (retval == RL_END) {
		retval = RL_OK;
	}

cleanup:
	rl_free(value);
	return retval;
}

int rl_publish(rlite *db, unsigned char *channel, size_t channellen, const char *data, size_t datalen, long *recipients)
{
	int retval;
	long i, patternc = 0;
	unsigned char **patternv = NULL;
	long *patternvlen = NULL;
	unsigned char *values[5];
	long valueslen[5];
	rl_set_iterator *iterator;
	*recipients = 0;

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
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS);
	RL_CALL2(rl_smembers, RL_OK, RL_NOT_FOUND, db, &iterator, channel, channellen);
	if (retval == RL_OK) {
		RL_CALL(publish_to_members, RL_OK, db, iterator, 4, values, valueslen, recipients)
	}

#define PMESSAGE "pmessage"
	q = 4;
	values[0] = &q;
	valueslen[0] = 1;
	values[1] = (unsigned char *)PMESSAGE;
	valueslen[1] = (long)strlen(PMESSAGE);
	values[3] = channel;
	valueslen[3] = channellen;
	values[4] = (unsigned char *)data;
	valueslen[4] = datalen;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS);
	RL_CALL2(rl_keys, RL_OK, RL_NOT_FOUND, db, (unsigned char *)"*", 1, &patternc, &patternv, &patternvlen);
	if (retval == RL_NOT_FOUND || patternc == 0) {
		retval = RL_OK;
		goto postpattern;
	}
	for (i = 0; i < patternc; i++) {
		if (!rl_stringmatchlen((char *)patternv[i], patternvlen[i], (char *)channel, channellen, 0)) {
			continue;
		}
		RL_CALL2(rl_smembers, RL_OK, RL_NOT_FOUND, db, &iterator, patternv[i], patternvlen[i]);
		if (retval == RL_OK) {
			values[2] = patternv[i];
			valueslen[2] = patternvlen[i];
			RL_CALL(publish_to_members, RL_OK, db, iterator, 5, values, valueslen, recipients)
		}
	}
postpattern:
	// this is equivalent to cleanup, but I'd rather leave this here for clarity
	// since an early not found needs to skip a bunch of code

cleanup:
	for (i = 0; i < patternc; i++) {
		rl_free(patternv[i]);
	}
	rl_free(patternv);
	rl_free(patternvlen);

	if (retval == RL_NOT_FOUND) {
		// no subscriber? no problem
		retval = RL_OK;
	}
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_pubsub_patterns(rlite *db, long *patternc, unsigned char ***patternv, long **patternvlen)
{
	int retval;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS);
	RL_CALL2(rl_keys, RL_OK, RL_NOT_FOUND, db, (unsigned char *)"*", 1, patternc, patternv, patternvlen);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_pubsub_channels(rlite *db, unsigned char *pattern, long patternlen, long *channelc, unsigned char ***channelv, long **channelvlen)
{
	int retval;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS);
	RL_CALL2(rl_keys, RL_OK, RL_NOT_FOUND, db, pattern ? pattern : (unsigned char *)"*", pattern ? patternlen : 1, channelc, channelv, channelvlen);
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_pubsub_numsub(rlite *db, int channelc, unsigned char **channelv, long *channelvlen, long *numsub)
{
	int i, retval;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_CHANNEL_SUBSCRIBERS);
	for (i = 0; i < channelc; i++) {
		numsub[i] = 0;
		RL_CALL2(rl_scard, RL_OK, RL_NOT_FOUND, db, channelv[i], channelvlen[i], &numsub[i]);
	}
	retval = RL_OK;
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_pubsub_numpat(rlite *db, long *numpat)
{
	int retval;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_PATTERN_SUBSCRIBERS);
	RL_CALL(rl_dbsize, RL_OK, db, numpat);
	retval = RL_OK;
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}

int rl_pubsub_count_subscriptions(rlite *db, long *numsubscriptions)
{
	int retval;
	if (!db->subscriber_id) {
		*numsubscriptions = 0;
		retval = RL_OK;
		goto cleanup;
	}
	long count = 0, count2 = 0;
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_CHANNELS);
	RL_CALL2(rl_scard, RL_OK, RL_NOT_FOUND, db, (unsigned char *)db->subscriber_id, 40, &count);
	RL_CALL(rl_select_internal, RL_OK, db, RLITE_INTERNAL_DB_SUBSCRIBER_PATTERNS);
	RL_CALL2(rl_scard, RL_OK, RL_NOT_FOUND, db, (unsigned char *)db->subscriber_id, 40, &count2);
	*numsubscriptions = count + count2;
	retval = RL_OK;
cleanup:
	rl_select_internal(db, RLITE_INTERNAL_DB_NO);
	return retval;
}
