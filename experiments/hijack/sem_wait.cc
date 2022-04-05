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

// TODO 加一个线程名
int sem_wait(sem_t *sem) {
    if (Ghost_Status::thread_num == WORKER_NUM) {
        // Yield to scheduler
        sched_yield();
    } else {
        // Ghost initialization
        int sid = Ghost_Status::thread_num.fetch_add(1);
        if(sid == 0){
            Ghost_Status::global_init();
        }
        // spin until global inistalization is finished
        while(Ghost_Status::have_global_init == 0);
        // Ghost Thread initialization
        Ghost_Status::thread_init(sid);
        printf("Ghost Thread %d initialization finished\n", sid);
        // spin until all the threads have initialized
        while(Ghost_Status::thread_num != WORKER_NUM);
        // Yield to scheduler
        sched_yield();
    }
    return 0;
}