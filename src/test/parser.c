#include <string.h>
#include "hirlite.h"

int test_format_noparam() {
	rliteClient client;
	if (rliteFormatCommand(&client, "ECHO helloworld") != 0) {
		return 1;
	}
	if (client.argc != 2) {
		fprintf(stderr, "Expected argc to be 2, got %d instead on line %d\n", client.argc, __LINE__);
		return 1;
	}
	if (client.argvlen[0] != 4) {
		fprintf(stderr, "Expected argvlen[0] to be 4, got %lu instead on line %d\n", client.argvlen[0], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[0], "ECHO", 4) != 0) {
		fprintf(stderr, "Expected argv[0] to be \"ECHO\", got \"%s\" instead on line %d\n", client.argv[0], __LINE__);
		return 1;
	}
	if (client.argvlen[1] != 10) {
		fprintf(stderr, "Expected argvlen[1] to be 10, got %lu instead on line %d\n", client.argvlen[1], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[1], "helloworld", 10) != 0) {
		fprintf(stderr, "Expected argv[1] to be \"helloworld\", got \"%s\" instead on line %d\n", client.argv[1], __LINE__);
		return 1;
	}
	return 0;
}

int test_format_sparam() {
	rliteClient client;
	if (rliteFormatCommand(&client, "ECHO %s", "helloworld") != 0) {
		return 1;
	}
	if (client.argc != 2) {
		fprintf(stderr, "Expected argc to be 2, got %d instead on line %d\n", client.argc, __LINE__);
		return 1;
	}
	if (client.argvlen[0] != 4) {
		fprintf(stderr, "Expected argvlen[0] to be 4, got %lu instead on line %d\n", client.argvlen[0], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[0], "ECHO", 4) != 0) {
		fprintf(stderr, "Expected argv[0] to be \"ECHO\", got \"%s\" instead on line %d\n", client.argv[0], __LINE__);
		return 1;
	}
	if (client.argvlen[1] != 10) {
		fprintf(stderr, "Expected argvlen[1] to be 10, got %lu instead on line %d\n", client.argvlen[1], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[1], "helloworld", 10) != 0) {
		fprintf(stderr, "Expected argv[1] to be \"helloworld\", got \"%s\" instead on line %d\n", client.argv[1], __LINE__);
		return 1;
	}
	return 0;
}

int test_format_bparam() {
	rliteClient client;
	// extra 1 to test that's not being read
	if (rliteFormatCommand(&client, "ECHO %b", "helloworld1", 10) != 0) {
		return 1;
	}
	if (client.argc != 2) {
		fprintf(stderr, "Expected argc to be 2, got %d instead on line %d\n", client.argc, __LINE__);
		return 1;
	}
	if (client.argvlen[0] != 4) {
		fprintf(stderr, "Expected argvlen[0] to be 4, got %lu instead on line %d\n", client.argvlen[0], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[0], "ECHO", 4) != 0) {
		fprintf(stderr, "Expected argv[0] to be \"ECHO\", got \"%s\" instead on line %d\n", client.argv[0], __LINE__);
		return 1;
	}
	if (client.argvlen[1] != 10) {
		fprintf(stderr, "Expected argvlen[1] to be 10, got %lu instead on line %d\n", client.argvlen[1], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[1], "helloworld", 10) != 0) {
		fprintf(stderr, "Expected argv[1] to be \"helloworld\", got \"%s\" instead on line %d\n", client.argv[1], __LINE__);
		return 1;
	}
	return 0;
}

int test_format_dparam() {
	rliteClient client;
	if (rliteFormatCommand(&client, "ECHO %d", 10) != 0) {
		return 1;
	}
	if (client.argc != 2) {
		fprintf(stderr, "Expected argc to be 2, got %d instead on line %d\n", client.argc, __LINE__);
		return 1;
	}
	if (client.argvlen[0] != 4) {
		fprintf(stderr, "Expected argvlen[0] to be 4, got %lu instead on line %d\n", client.argvlen[0], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[0], "ECHO", 4) != 0) {
		fprintf(stderr, "Expected argv[0] to be \"ECHO\", got \"%s\" instead on line %d\n", client.argv[0], __LINE__);
		return 1;
	}
	if (client.argvlen[1] != 2) {
		fprintf(stderr, "Expected argvlen[1] to be 2, got %lu instead on line %d\n", client.argvlen[1], __LINE__);
		return 1;
	}
	if (memcmp(client.argv[1], "10", 2) != 0) {
		fprintf(stderr, "Expected argv[1] to be \"10\", got \"%s\" instead on line %d\n", client.argv[1], __LINE__);
		return 1;
	}
	return 0;
}

int run_parser() {
	if (test_format_noparam() != 0) {
		return 1;
	}
	if (test_format_sparam() != 0) {
		return 1;
	}
	if (test_format_bparam() != 0) {
		return 1;
	}
	return 0;
}
