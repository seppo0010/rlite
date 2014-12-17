#include "echo.h"
#include "zset.h"

int main() {
	if (run_echo() != 0) { return 1; }
	if (run_zset() != 0) { return 1; }
	return 0;
}
