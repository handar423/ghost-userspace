#pragma once

#include "ghost_status.h"
#include "unistd.h"
#define MILLION 1000000

int nanosleep (const struct timespec *__requested_time,
		       struct timespec *__remaining);