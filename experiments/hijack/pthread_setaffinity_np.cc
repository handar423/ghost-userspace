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
#include <sched.h>

#define CPU_NUM 40
using ghost_test::Ghost_Status;

// static int get_cpu_num(const cpu_set_t *cpuset) {
//     for (int cpu = 0; cpu < CPU_NUM; cpu++) {
//         if (CPU_ISSET(cpu, cpuset)) return cpu;
//     }
//     // default cpu0
//     return 0;
// }

static pthread_setaffinity_np_type old_pthread_setaffinity_np = (pthread_setaffinity_np_type)(dlsym(RTLD_NEXT, "pthread_setaffinity_np"));
int pthread_setaffinity_np(pthread_t thread, size_t cpusetsize, const cpu_set_t *cpuset) {
    char tname[80];
    prctl(PR_GET_NAME, tname);
    // only hijack bbupool_rt_x thread
    if (std::strncmp("bbupool_rt_", tname, 11) != 0) return old_pthread_setaffinity_np(thread, cpusetsize, cpuset);

    printf("!!!!!!!!!!==== %s enter pthread_setaffinity_np ====!!!!!!!!!!!!\n", tname);
    // bbupool_rt_x thread should be hijacked only once by setaffinity to initialize Ghost
    assert(Ghost_Status::thread_num != Ghost_Status::worker_num);

    // Ghost initialization
    sid = Ghost_Status::thread_num.fetch_add(1);
    if(sid == 0){
        Ghost_Status::global_init();
    }
    // spin until global inistalization is finished
    while(Ghost_Status::have_global_init == 0);
    
    // set_affinity
    // int cpunum = get_cpu_num(cpuset);
    // printf("Bind %s onto cpu %d", tname, cpunum);
    ghost::Ghost::SchedSetAffinity(
                ghost::Gtid::Current(),
                ghost::MachineTopology()->ToCpuList(std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}));

    // Ghost Thread initialization
    Ghost_Status::thread_init(sid);
    printf("Ghost Thread %d initialization finished\n", sid);

    // spin until all the threads have initialized
    while(Ghost_Status::thread_num != Ghost_Status::worker_num);

    return 0;
}