#include <time.h>
#define u_int32_t int
#define fileno _fileno

struct timeval {
	long int tv_sec;
	long int tv_usec;
};

struct timespec {
    time_t   tv_sec;        /* seconds */
    long     tv_nsec;       /* nanoseconds */
};

#ifdef RLITEDLL_EXPORTS
#define RLITE_API __declspec(dllexport)
#else
#define RLITE_API __declspec(dllimport)
#endif
