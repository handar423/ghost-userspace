#pragma once

#include <cstdlib>
#include <ctime>
#include <atomic>
#include <mutex>
#include "experiments/shared/ghost.h"
#include "lib/ghost.h"
#include "absl/time/time.h"

// the number is set according to the hijacked application 
extern thread_local int sid;
#define DEFAULT_WORKER_NUM 40
#define DEFAULT_QOS 16
using std::atomic_int;
using std::mutex;

typedef int (*usleep_op_type)(useconds_t __useconds);
typedef int (*nanosleep_op_type)(const struct timespec *__requested_time,
		                         struct timespec *__remaining);

namespace ghost_test {

class Ghost_Status
{
public:
    // Ghost对象
    static Ghost ghost_;

    static int worker_num;

    static int qos;

    static atomic_int thread_num;

    static mutex thread_init_mutex;

    static atomic_int have_global_init;

    static const absl::Duration deadline;

    static constexpr uint32_t kWorkClassIdentifier = 0;

    static void global_init();

    static void thread_init(int sid);

};

}