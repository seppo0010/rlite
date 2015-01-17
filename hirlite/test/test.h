#define NO_ERROR(reply)\
	if (reply->type == RLITE_REPLY_ERROR) {\
		fprintf(stderr, "Unexpected error \"%s\" on line %d\n", reply->str, __LINE__);\
		return 1;\
	}\

#define EXPECT_OBJ(expectedtype, reply, string, strlen)\
	if (expectedtype != RLITE_REPLY_ERROR) {\
		NO_ERROR(reply);\
	}\
	if (reply->type != expectedtype) {\
		fprintf(stderr, "Expected reply to be %d, got %d instead on line %d\n", expectedtype, reply->type, __LINE__);\
		return 1;\
	}\
	if (reply->len != strlen || memcmp(reply->str, string, strlen) != 0) {\
		fprintf(stderr, "Expected reply to be \"%s\" (%d), got \"%s\" (%d) instead on line %d\n", string, strlen, reply->str, reply->len, __LINE__);\
		return 1;\
	}

#define EXPECT_STATUS(reply, string, strlen) EXPECT_OBJ(RLITE_REPLY_STATUS, reply, string, strlen)
#define EXPECT_STR(reply, string, strlen) EXPECT_OBJ(RLITE_REPLY_STRING, reply, string, strlen)
#define EXPECT_INTEGER(reply, expectedinteger)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_INTEGER) {\
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on line %d\n", reply->type, __LINE__);\
		return 1;\
	}\
	if (reply->integer != expectedinteger) {\
		fprintf(stderr, "Expected reply to be %lld, got %lld instead on line %d\n", (long long)expectedinteger, reply->integer, __LINE__);\
		return 1;\
	}

#define EXPECT_NIL(reply)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_NIL) {\
		fprintf(stderr, "Expected reply to be NIL, got %d instead on line %d\n", reply->type, __LINE__);\
		return 1;\
	}
#define EXPECT_LEN(reply, expectedlen)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_ARRAY) {\
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on line %d\n", reply->type, __LINE__);\
		return 1;\
	}\
	if (reply->elements != expectedlen) {\
		fprintf(stderr, "Expected reply length to be %ld got %ld instead on line %d\n", (long)expectedlen, reply->elements, __LINE__);\
		return 1;\
	}
