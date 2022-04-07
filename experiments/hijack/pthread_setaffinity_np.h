#pragma once
#include "ghost_status.h"
#include <pthread.h>

typedef int (*pthread_setaffinity_np_type)(pthread_t, size_t, const cpu_set_t*);
int pthread_setaffinity_np(pthread_t thread, size_t cpusetsize,
                             const cpu_set_t *cpuset);