#include "pthread_setaffinity_np.h"
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

using ghost_test::Ghost_Status;

static pthread_setaffinity_np_type old_pthread_setaffinity_np = (pthread_setaffinity_np_type)(dlsym(RTLD_NEXT, "pthread_setaffinity_np"));
int pthread_setaffinity_np(pthread_t thread, size_t cpusetsize, const cpu_set_t *cpuset) {
    char tname[80];
    prctl(PR_GET_NAME, tname);
    // only hijack bbupool_rt_x thread
    if (std::strncmp("bbupool_rt_", tname, 11) != 0) return old_pthread_setaffinity_np(thread, cpusetsize, cpuset);

    printf("!!!!!!!!!!==== %s enter pthread_setaffinity_np ====!!!!!!!!!!!!\n", tname);
    //printf("cpusetsize: %ld, cpunum: %d\n", cpusetsize, CPU_COUNT(cpuset));
    // bbupool_rt_x thread should be hijacked only once by setaffinity to initialize Ghost
    assert(Ghost_Status::thread_num != Ghost_Status::worker_num);

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
    while(Ghost_Status::thread_num != Ghost_Status::worker_num);

    return old_pthread_setaffinity_np(thread, cpusetsize, cpuset);
}