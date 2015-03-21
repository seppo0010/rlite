#include "compat_win.h"
#include <windows.h>
#include <stdio.h>
#include <sys\timeb.h>

int rl_file_exists(const char *path) {
	WIN32_FIND_DATA FindFileData;
	HANDLE handle = FindFirstFile(path, &FindFileData) ;
	int found = handle != INVALID_HANDLE_VALUE;
	if(found) {
		FindClose(handle);
	}
	return found;
}

int rl_file_is_readable(const char *path)
{
	// TODO
	return 1;
}

int rl_file_is_writable(const char *path)
{
	// TODO
	return 1;
}

unsigned long long rl_mstime() {
	struct timeb now;
	ftime(&now);
	return now.millitm;
}
