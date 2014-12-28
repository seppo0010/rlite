#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "hirlite.h"

static int populateArgvlen(char *argv[], size_t argvlen[]) {
	int i;
	for (i = 0; argv[i] != NULL; i++) {
		argvlen[i] = strlen(argv[i]);
	}
	return i;
}

static void sadd(rliteContext* context, char *key, char *element) {
	rliteReply* reply;
	size_t argvlen[100];
	char* argv[100] = {"sadd", key, element, NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	rliteFreeReplyObject(reply);
}

static int randomSadd(rliteContext* context, char *key, int elements) {
	rliteReply* reply;
	if (elements == 0) {
		elements = 3;
	}
	char* argv[100] = {"sadd", key, NULL};
	int i, j, len;
	for (i = 0; i < elements; i++) {
		len = 5 + floor(((float)rand() / RAND_MAX) * 10);
		argv[2 + i] = malloc(sizeof(char) * len);
		for (j = 0; j < len - 1; j++) {
			argv[2 + i][j] = 'a' + (int)floor(((float)rand() / RAND_MAX) * 25);
		}
		argv[2 + i][len - 1] = 0;
	}
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != elements) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", elements, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);
	for (i = 0; i < elements; i++) {
		free(argv[2 + i]);
	}
	return 0;
}

static int test_sadd() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"sadd", "myset", "member1", "member2", "anothermember", "member1", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 3) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 3, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

static int test_scard() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	if (randomSadd(context, "myset", 5) != 0) {
		return 1;
	}

	rliteReply* reply;
	char* argv[100] = {"scard", "myset", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 5) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 5, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	if (randomSadd(context, "myset", 3) != 0) {
		return 1;
	}

	char* argv2[100] = {"scard", "myset", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 8) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 8, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

static int test_sismember() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	sadd(context, "myset", "mymember");

	rliteReply* reply;
	char* argv[100] = {"sismember", "myset", "mymember", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 1) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"sismember", "myset", "not a member", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	if (reply->type != RLITE_REPLY_INTEGER) {
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
		return 1;
	}

	if (reply->integer != 0) {
		fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 0, reply->integer, __LINE__);
		return 1;
	}
	rliteFreeReplyObject(reply);

	rliteFree(context);
	return 0;
}

static int test_smove() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	sadd(context, "myset", "mymember");

	rliteReply* reply;
	{
		char* argv[100] = {"smove", "myset", "otherset", "mymember", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 1) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "myset", "mymember", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 0) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 0, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "otherset", "mymember", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 1) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 1, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"smove", "otherset", "myset", "member2", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 0) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 0, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"sismember", "myset", "member2", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_INTEGER) {
			fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (reply->integer != 0) {
			fprintf(stderr, "Expected reply to be %d, got %lld instead on line %d\n", 0, reply->integer, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_spop() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"spop", "myset", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			fprintf(stderr, "Expected reply to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"spop", "myset", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			fprintf(stderr, "Expected reply to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	{
		char* argv[100] = {"spop", "myset", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_NIL) {
			fprintf(stderr, "Expected reply to be NIL, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}

	rliteFree(context);
	return 0;
}

static int test_srandmember_nocount() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_STRING) {
			fprintf(stderr, "Expected reply to be STRING, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}

		if (!(reply->len == (int)strlen(m1) && memcmp(reply->str, m1, reply->len) == 0) &&
			!(reply->len == (int)strlen(m2) && memcmp(reply->str, m2, reply->len) == 0)) {
			fprintf(stderr, "Expected reply to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->str, reply->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

static int test_srandmember_1() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "1", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}
		if (reply->elements != 1) {
			fprintf(stderr, "Expected reply size to be 1, got %lu instead on line %d\n", reply->elements, __LINE__);
			return 1;
		}

		if (!(reply->element[0]->len == (int)strlen(m1) && memcmp(reply->element[0]->str, m1, reply->element[0]->len) == 0) &&
			!(reply->element[0]->len == (int)strlen(m2) && memcmp(reply->element[0]->str, m2, reply->element[0]->len) == 0)) {
			fprintf(stderr, "Expected reply->element[0] to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->element[0]->str, reply->element[0]->len, __LINE__);
			return 1;
		}
		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

static int test_srandmember_10_unique() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "10", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}
		if (reply->elements != 2) {
			fprintf(stderr, "Expected reply size to be 2, got %lu instead on line %d\n", reply->elements, __LINE__);
			return 1;
		}

		if (!(reply->element[0]->len == (int)strlen(m1) && memcmp(reply->element[0]->str, m1, reply->element[0]->len) == 0) &&
			!(reply->element[0]->len == (int)strlen(m2) && memcmp(reply->element[0]->str, m2, reply->element[0]->len) == 0)) {
			fprintf(stderr, "Expected reply->element[0] to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->element[0]->str, reply->element[0]->len, __LINE__);
			return 1;
		}

		if (!(reply->element[1]->len == (int)strlen(m1) && memcmp(reply->element[1]->str, m1, reply->element[1]->len) == 0) &&
			!(reply->element[1]->len == (int)strlen(m2) && memcmp(reply->element[1]->str, m2, reply->element[1]->len) == 0)) {
			fprintf(stderr, "Expected reply->element[1] to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", m1, m2, reply->element[1]->str, reply->element[1]->len, __LINE__);
			return 1;
		}

		if (reply->element[0]->len == reply->element[1]->len) {
			fprintf(stderr, "Expected reply elements to be different on line %d\n", __LINE__);
			return 1;
		}

		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

static int test_srandmember_10_non_unique() {
	rliteContext *context = rliteConnect(":memory:", 0);
	size_t argvlen[100];

	long i;
	char *m1 = "mymember", *m2 = "member2";
	sadd(context, "myset", m1);
	sadd(context, "myset", m2);

	rliteReply* reply;
	{
		char* argv[100] = {"srandmember", "myset", "-10", NULL};

		reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
		if (reply->type != RLITE_REPLY_ARRAY) {
			fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);
			return 1;
		}
		if (reply->elements != 10) {
			fprintf(stderr, "Expected reply size to be 10, got %lu instead on line %d\n", reply->elements, __LINE__);
			return 1;
		}

		for (i = 0; i < 10; i++) {
			if (!(reply->element[i]->len == (int)strlen(m1) && memcmp(reply->element[i]->str, m1, reply->element[i]->len) == 0) &&
				!(reply->element[i]->len == (int)strlen(m2) && memcmp(reply->element[i]->str, m2, reply->element[i]->len) == 0)) {
				fprintf(stderr, "Expected reply->element[%ld] to be \"%s\" or \"%s\", got \"%s\" (%d) instead on line %d\n", i, m1, m2, reply->element[i]->str, reply->element[i]->len, __LINE__);
				return 1;
			}
		}

		rliteFreeReplyObject(reply);
	}
	rliteFree(context);
	return 0;
}

int run_set() {
	if (test_sadd() != 0) {
		return 1;
	}
	if (test_scard() != 0) {
		return 1;
	}
	if (test_sismember() != 0) {
		return 1;
	}
	if (test_smove() != 0) {
		return 1;
	}
	if (test_spop() != 0) {
		return 1;
	}
	if (test_srandmember_nocount() != 0) {
		return 1;
	}
	if (test_srandmember_1() != 0) {
		return 1;
	}
	if (test_srandmember_10_unique() != 0) {
		return 1;
	}
	if (test_srandmember_10_non_unique() != 0) {
		return 1;
	}
	return 0;
}
