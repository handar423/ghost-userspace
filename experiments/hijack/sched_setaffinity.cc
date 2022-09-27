#include "sched_setaffinity.h"

#define CPU_NUM 40
using ghost_test::Ghost_Status;

int nanosleep(const struct timespec *rqtp, struct timespec *rmtp){
    char tname[80];
    prctl(PR_GET_NAME, tname);
    // only hijack bbupool_rt_x thread
    // if (std::strncmp("nginx", tname, 5) != 0) 
    fprintf(stderr, "hello %s\n", tname);

    // return old_sched_setaffinity(__pid, __cpusetsize, __cpuset);

    // printf("!!!!!!!!!!==== %d enter pthread_setaffinity_np ====!!!!!!!!!!!!\n", 0);
    // // bbupool_rt_x thread should be hijacked only once by setaffinity to initialize Ghost
    // if (Ghost_Status::thread_num >= Ghost_Status::worker_num) return 0;

    // Ghost initialization
    // sid = Ghost_Status::thread_num.fetch_add(1);
    // if(sid == 0){
    //     Ghost_Status::global_init();
    // }
    // // spin until global inistalization is finished
    // while(Ghost_Status::have_global_init == 0);
    // fprintf(stderr, "there 0\n");
    
    // set_affinity
    // int cpunum = get_cpu_num(cpuset);
    // printf("Bind %s onto cpu %d", tname, cpunum);
    // ghost::Ghost::SchedSetAffinity(
    //             ghost::Gtid::Current(),
    //             ghost::MachineTopology()->ToCpuList(std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}));

    // fprintf(stderr, "there %d\n", sid);
    // // Ghost Thread initialization
    // Ghost_Status::thread_init(sid);
    // fprintf(stderr, "Ghost Thread %d initialization finished\n", sid);

    // spin until all the threads have initialized
    // while(Ghost_Status::thread_num != Ghost_Status::worker_num);

    return 0;
}