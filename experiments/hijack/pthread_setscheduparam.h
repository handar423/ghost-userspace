#pragma once
#include "ghost_status.h"
#include <pthread.h>

typedef int (*pthread_setschedparam_type)(pthread_t, int, const struct sched_param*);
int pthread_setschedparam (pthread_t __target_thread, int __policy,
				  const struct sched_param *__param);