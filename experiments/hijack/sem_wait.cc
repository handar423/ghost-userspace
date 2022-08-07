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
    // char tname[80];
    // prctl(PR_GET_NAME, tname);
    //printf("!!!!!!!!!!==== %s enter sem_wait ====!!!!!!!!!!!!\n", tname);

    // DEBUG only
    // return old_sem_wait(sem);

    if (sid == -1) return old_sem_wait(sem);
    else {
        ghost::sched_item si;
        Ghost_Status::ghost_.GetSchedItem(sid, si);
        if(si.yield_flag){
            si.empty_time = 0;
            si.yield_flag = 0;
            sched_yield();
            Ghost_Status::ghost_.SetSchedEmptyUnsafe(sid, si);
        } else {
            si.empty_time += 1;
            // printf("si.empty_time %d\n", si.empty_time);
            Ghost_Status::ghost_.SetSchedEmptyUnsafe(sid, si);
        }
    }
    return 0;
}