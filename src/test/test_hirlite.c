#include "db.h"
#include "multi.h"
#include "echo.h"
#include "hash.h"
#include "parser.h"
#include "list.h"
#include "set.h"
#include "string.h"
#include "zset.h"
#include "hsort.h"
#include "scripting-test.h"

int main() {
	if (run_echo() != 0) { return 1; }
	if (run_parser() != 0) { return 1; }
	if (run_db() != 0) { return 1; }
	if (run_multi() != 0) { return 1; }
	if (run_list() != 0) { return 1; }
	if (run_set() != 0) { return 1; }
	if (run_string() != 0) { return 1; }
	if (run_zset() != 0) { return 1; }
	if (run_hash() != 0) { return 1; }
	if (run_sort() != 0) { return 1; }
	if (run_scripting_test() != 0) { return 1; }
	return 0;
}
