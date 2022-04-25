#include "pthread_setscheduparam.h"
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
#include <assert.h>
#include <sched.h>

static pthread_setschedparam_type old_pthread_setschedparam = (pthread_setschedparam_type)(dlsym(RTLD_NEXT, "pthread_setschedparam"));
int pthread_setschedparam(pthread_t __target_thread, int __policy, const struct sched_param *__param) {
    char tname[80];
    prctl(PR_GET_NAME, tname);
    //printf("!!!!!!!!!!==== %s enter sem_wait ====!!!!!!!!!!!!\n", tname);

    // DEBUG only
    // return old_sem_wait(sem);

    if (std::strncmp("bbupool_rt_", tname, 11) != 0) 
		return old_pthread_setschedparam(__target_thread, __policy, __param);
    return 0;
}