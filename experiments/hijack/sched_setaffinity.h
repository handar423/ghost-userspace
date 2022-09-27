#pragma once
#define _GNU_SOURCE
#include "ghost_status.h"
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#undef nanosleep

typedef int (*nanosleep_type)(const struct timespec *, struct timespec *);
int nanosleep(const struct timespec *rqtp, struct timespec *rmtp);