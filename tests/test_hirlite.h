#define NO_ERROR(reply)\
	if (reply->type == RLITE_REPLY_ERROR) {\
		fprintf(stderr, "Unexpected error \"%s\" on %s:%d\n", reply->str, __FILE__, __LINE__);\
		return 1;\
	}\

#define EXPECT_OBJ(expectedtype, reply, string, strlen)\
	if (expectedtype != RLITE_REPLY_ERROR) {\
		NO_ERROR(reply);\
	}\
	if (reply->type != expectedtype) {\
		fprintf(stderr, "Expected reply to be %d, got %d instead on %s:%d\n", expectedtype, reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->len != strlen || memcmp(reply->str, string, strlen) != 0) {\
		fprintf(stderr, "Expected reply to be \"%s\" (%d), got \"%s\" (%d) instead on %s:%d\n", string, strlen, reply->str, reply->len, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_STATUS(reply, string, strlen) EXPECT_OBJ(RLITE_REPLY_STATUS, reply, string, strlen)
#define EXPECT_STR(reply, string, strlen) EXPECT_OBJ(RLITE_REPLY_STRING, reply, string, strlen)
#define EXPECT_ERROR_STR(reply, string, strlen) EXPECT_OBJ(RLITE_REPLY_ERROR, reply, string, strlen)

#define EXPECT_ERROR(reply)\
	if (reply->type != RLITE_REPLY_ERROR) {\
		fprintf(stderr, "Expected error got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\

#define EXPECT_INTEGER(reply, expectedinteger)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_INTEGER) {\
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->integer != (expectedinteger)) {\
		fprintf(stderr, "Expected reply to be %lld, got %lld instead on %s:%d\n", (long long)(expectedinteger), reply->integer, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_INTEGER_LTE(reply, expectedinteger)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_INTEGER) {\
		fprintf(stderr, "Expected reply to be INTEGER, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->integer > expectedinteger) {\
		fprintf(stderr, "Expected reply to be at most %lld, got %lld instead on %s:%d\n", (long long)expectedinteger, reply->integer, __FILE__, __LINE__);\
		return 1;\
	}

#define EXPECT_NIL(reply)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_NIL) {\
		fprintf(stderr, "Expected reply to be NIL, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}
#define EXPECT_LEN(reply, expectedlen)\
	NO_ERROR(reply);\
	if (reply->type != RLITE_REPLY_ARRAY) {\
		fprintf(stderr, "Expected reply to be ARRAY, got %d instead on %s:%d\n", reply->type, __FILE__, __LINE__);\
		return 1;\
	}\
	if (reply->elements != expectedlen) {\
		fprintf(stderr, "Expected reply length to be %ld got %ld instead on %s:%d\n", (long)expectedlen, reply->elements, __FILE__, __LINE__);\
		return 1;\
	}
