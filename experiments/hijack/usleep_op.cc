#include "usleep_op.h"
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

int usleep(__useconds_t __useconds){
    if((int)__useconds < 0){
        return Ghost_Status::ghost_.IsIdle(-__useconds);
    }
    // fprintf(stderr, "enter usleep\n");

    // exit(0);
    if(Ghost_Status::need_global_init.fetch_add(1) == 0){
        Ghost_Status::global_init();
    }
    while(Ghost_Status::have_global_init == 0);
    if(__useconds != 1){
        Ghost_Status::thread_init(__useconds - 1);
    }
    return 0;
}
