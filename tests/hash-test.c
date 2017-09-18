#include <string.h>
#include "rlite/hirlite.h"
#include "util.h"

TEST test_hset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hsetnx() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hsetnx", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hsetnx", "mykey", "myfield", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hget", "mykey", "myfield", NULL};
	EXPECT_REPLY_STR(reply, "mydata", 6);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hget() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hget", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_STR(reply, "mydata", 6);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hget", "mykey", "non existing field", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_NIL(reply);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hexists() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hexists", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hexists", "mykey", "non existing field", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hdel() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;

	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hset", "mykey", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hdel", "mykey", "myfield", "nonexistentfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hexists", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_INTEGER(reply, 0);
	rliteFreeReplyObject(reply);

	char *argv5[100] = {"hexists", "mykey", "myfield2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hlen() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;

	char* argv[100] = {"hset", "mykey", "myfield", "mydata", NULL};
	size_t argvlen[100];

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hset", "mykey", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	char *argv5[100] = {"hdel", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	char *argv6[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv6, argvlen), argv6, argvlen);
	EXPECT_REPLY_INTEGER(reply, 1);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hmset() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_STATUS(reply, "OK", 2);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hlen", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 2);
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hmset", "mykey", "myfield3", "mydata3", "field2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hget", "mykey", "myfield", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_STR(reply, "mydata", 6);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hincrby() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hincrby", "mykey", "myfield", "123", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_INTEGER(reply, 123);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hincrby", "mykey", "myfield", "345", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	EXPECT_REPLY_INTEGER(reply, 468);
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hincrby", "mykey", "myfield", "not a number", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hset", "mykey", "myfield", "not a number", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char* argv5[100] = {"hincrby", "mykey", "myfield", "1", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	EXPECT_REPLY_ERROR(reply);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hincrbyfloat() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hincrbyfloat", "mykey", "myfield", "123.4", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STRING);
	ASSERT_EQ(memcmp(reply->str, "123.4", 5), 0);
	rliteFreeReplyObject(reply);

	char *argv2[100] = {"hincrbyfloat", "mykey", "myfield", "345.7", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STRING);
	ASSERT_EQ(memcmp(reply->str, "469.1", 5), 0);
	rliteFreeReplyObject(reply);

	char* argv3[100] = {"hincrbyfloat", "mykey", "myfield", "not a number", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_ERROR(reply);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hset", "mykey", "myfield", "not a number", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_NO_ERROR(reply);
	rliteFreeReplyObject(reply);

	char* argv5[100] = {"hincrbyfloat", "mykey", "myfield", "1.2", NULL};

	reply = rliteCommandArgv(context, populateArgvlen(argv5, argvlen), argv5, argvlen);
	EXPECT_REPLY_ERROR(reply);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hgetall() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hgetall", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STATUS);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hgetall", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_LEN(reply, 4);
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING ||
			 reply->element[2]->type != RLITE_REPLY_STRING || reply->element[3]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d,%d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				reply->element[2]->type,
				reply->element[3]->type,
				__LINE__);
		FAIL();
	}
	EXPECT_REPLY_STR(reply->element[2], "myfield", 7);
	EXPECT_REPLY_STR(reply->element[3], "mydata", 6);
	EXPECT_REPLY_STR(reply->element[0], "myfield2", 8);
	EXPECT_REPLY_STR(reply->element[1], "mydata2", 7);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hkeys() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hkeys", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STATUS);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hkeys", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				__LINE__);
		FAIL();
	}
	EXPECT_REPLY_STR(reply->element[1], "myfield", 7);
	EXPECT_REPLY_STR(reply->element[0], "myfield2", 8);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hvals() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv[100] = {"hvals", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv, argvlen), argv, argvlen);
	EXPECT_REPLY_LEN(reply, 0);
	rliteFreeReplyObject(reply);

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STATUS);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hvals", "mykey", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[1]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[1]->type,
				__LINE__);
		FAIL();
	}
	EXPECT_REPLY_STR(reply->element[1], "mydata", 6);
	EXPECT_REPLY_STR(reply->element[0], "mydata2", 7);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

TEST test_hmget() {
	rliteContext *context = rliteConnect(":memory:", 0);

	rliteReply* reply;
	size_t argvlen[100];

	char* argv2[100] = {"hmset", "mykey", "myfield", "mydata", "myfield2", "mydata2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv2, argvlen), argv2, argvlen);
	ASSERT_EQ(reply->type, RLITE_REPLY_STATUS);
	rliteFreeReplyObject(reply);

	char *argv3[100] = {"hmget", "mykey2", "myfield", "myfield2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv3, argvlen), argv3, argvlen);
	EXPECT_REPLY_LEN(reply, 2);
	ASSERT_EQ(reply->element[0]->type, RLITE_REPLY_NIL);
	ASSERT_EQ(reply->element[1]->type, RLITE_REPLY_NIL);
	rliteFreeReplyObject(reply);

	char *argv4[100] = {"hmget", "mykey", "myfield", "nofield", "myfield2", NULL};
	reply = rliteCommandArgv(context, populateArgvlen(argv4, argvlen), argv4, argvlen);
	EXPECT_REPLY_LEN(reply, 3);
	if (reply->element[0]->type != RLITE_REPLY_STRING || reply->element[2]->type != RLITE_REPLY_STRING) {
		fprintf(stderr, "Expected reply->element[i] to be STRING, got %d,%d instead on line %d\n",
				reply->element[0]->type,
				reply->element[2]->type,
				__LINE__);
		FAIL();
	}
	ASSERT_EQ(reply->element[1]->type, RLITE_REPLY_NIL);
	EXPECT_REPLY_STR(reply->element[0], "mydata", 6);
	EXPECT_REPLY_STR(reply->element[2], "mydata2", 7);
	rliteFreeReplyObject(reply);

	rliteFree(context);
	PASS();
}

SUITE(hash_test)
{
	RUN_TEST(test_hset);
	RUN_TEST(test_hget);
	RUN_TEST(test_hexists);
	RUN_TEST(test_hdel);
	RUN_TEST(test_hlen);
	RUN_TEST(test_hmset);
	RUN_TEST(test_hincrby);
	RUN_TEST(test_hincrbyfloat);
	RUN_TEST(test_hgetall);
	RUN_TEST(test_hkeys);
	RUN_TEST(test_hvals);
	RUN_TEST(test_hmget);
}
