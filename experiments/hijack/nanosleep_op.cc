#include "nanosleep_op.h"
#include <cstdio>
#include <dlfcn.h>
#include <cstdlib>
#include <thread>
#include <pthread.h>
#include <sys/prctl.h>
#include <string>
#include <cstring>
#include <ctime>
#include <cmath>

using ghost_test::Ghost_Status;

int nanosleep (const struct timespec *__requested_time,
		       struct timespec *__remaining){
    // fprintf(stderr, "enter nanosleep\n");
    // exit(0);
    if(__requested_time->tv_sec == 1){
        // fprintf(stderr, "thread_wait_runnable %ld\n", __requested_time->tv_nsec);
        Ghost_Status::thread_wait_runnable(__requested_time->tv_nsec);
    } else if (__requested_time->tv_sec == 0) {
        // fprintf(stderr, "thread_set_runnable %ld\n", __requested_time->tv_nsec);
        Ghost_Status::thread_set_runnable(__requested_time->tv_nsec);
    } else {
        Ghost_Status::ghost_.MarkRunnable(__requested_time->tv_nsec);
    }

    return 0;
}