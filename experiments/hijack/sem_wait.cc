#include "sem_wait.h"
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

static sem_op_type old_sem_wait = (sem_op_type)(dlsym(RTLD_NEXT, "sem_wait"));
int sem_wait(sem_t *sem) {
    char tname[80];
    prctl(PR_GET_NAME, tname);
    //printf("!!!!!!!!!!==== %s enter sem_wait ====!!!!!!!!!!!!\n", tname);

    // DEBUG only
    // return old_sem_wait(sem);

    if (std::strncmp("bbupool_rt_", tname, 11) != 0) return old_sem_wait(sem);
    else {
        // Yield to scheduler
        sched_yield();
    }
    return 0;
}