#include "pthread_cond.h"

#define CPU_NUM 40
using ghost_test::Ghost_Status;

static pthread_cond_wait_type old_pthread_cond_wait = (pthread_cond_wait_type)(dlvsym(RTLD_NEXT, "pthread_cond_wait", "GLIBC_2.3.2"));
int pthread_cond_wait (pthread_cond_t * __cond, pthread_mutex_t * __mutex)
{
    static int init_flag = 0;
    char tname[80];
    prctl(PR_GET_NAME, tname);

    fprintf(stderr, "hello %s\n", tname);
    if (std::strncmp("Tpool", tname, 5) != 0) return old_pthread_cond_wait(__cond, __mutex);
    else {

        if(init_flag == 0){
            // set_affinity
            // int cpunum = get_cpu_num(cpuset);
            // printf("Bind %s onto cpu %d", tname, cpunum);
            ghost::Ghost::SchedSetAffinity(
                        ghost::Gtid::Current(),
                        ghost::MachineTopology()->ToCpuList(std::vector<int>{12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}));

            printf("!!!!!!!!!!==== %s enter pthread_cond_wait ====!!!!!!!!!!!!\n", tname);
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

            // spin until all the threads have initialized
            // printf("%s pthread_mutex_unlock\n", tname);
            pthread_mutex_unlock(__mutex);
            while(Ghost_Status::thread_num != Ghost_Status::worker_num)
                sched_yield();
            // printf("%s pthread_mutex_lock\n", tname);
            pthread_mutex_lock(__mutex);
            
            init_flag = 1;
            printf("Ghost Thread %d initialization finished\n", sid);
        }
        
        // sched_yield();
        // return 0;
        // printf("%s old_pthread_cond_wait\n", tname);
        return old_pthread_cond_wait(__cond, __mutex);

        // struct timeval now;
        // struct timespec outtime;
        // printf("hello pthread_cond_wait\n");
        // int rc = 0;

        // while(1){
        //     printf("%s try to get pthread lock %p\n", tname, __mutex);
        //     rc = pthread_mutex_trylock(__mutex);
        //     if (rc != 0){
        //         printf("%s cannot get lock, sleep\n", tname);
        //         sched_yield();
        //         continue;
        //     }
        //     gettimeofday(&now, NULL);
        //     outtime.tv_sec = now.tv_sec;
        //     outtime.tv_nsec = now.tv_usec * 1000 + 1000;
        //     printf("%s try to get pthread cond\n", tname);
        //     rc = pthread_cond_timedwait(__cond, __mutex, &outtime);
        //     if (rc != 0){
        //         printf("%s cannot get pthread cond, sleep\n", tname);
        //         pthread_mutex_unlock(__mutex);
        //         sched_yield();
        //         continue;
        //     }
        //     return 0;
        // }
    }

    return 0;
}