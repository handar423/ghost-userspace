#pragma once
#include "ghost_status.h"
#include <stdio.h>
#include <stdint.h>
#include <bits/pthreadtypes.h>
#include <dlfcn.h>
#include <pthread.h>
#include <asm-generic/errno-base.h>
#include <sys/time.h>
#include <sys/prctl.h>
#include <string>
#include <cstring>

#undef pthread_cond_wait

typedef int (*pthread_cond_wait_type)(pthread_cond_t *, pthread_mutex_t *);
int pthread_cond_wait (pthread_cond_t * __cond, pthread_mutex_t * __mutex);