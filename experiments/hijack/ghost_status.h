#pragma once

#include <cstdlib>
#include <ctime>
#include <atomic>
#include "experiments/shared/ghost.h"
#include "lib/ghost.h"
#include "absl/time/time.h"

// the number is set according to the hijacked application 
#define WORKER_NUM 5
using std::atomic_int;

typedef int (*usleep_op_type)(useconds_t __useconds);
typedef int (*nanosleep_op_type)(const struct timespec *__requested_time,
		                         struct timespec *__remaining);

namespace ghost_test {

class Ghost_Status
{
public:
    // Ghost对象
    static Ghost ghost_;

    static atomic_int thread_num;

    static atomic_int have_global_init;

    static const absl::Duration deadline;

    static void global_init();

    static void thread_init(int sid);

    static void thread_set_runnable(int sid);

    static void thread_wait_runnable(int sid);

    static constexpr uint32_t kWorkClassIdentifier = 0;

};

}