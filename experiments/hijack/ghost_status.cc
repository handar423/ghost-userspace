#include "ghost_status.h"
#include <stdio.h>
using std::getenv;

// 线程间静态变量，sid
thread_local int sid = -1;

namespace ghost_test {

namespace {
// We do not need a different class of service (e.g., different expected
// runtimes, different QoS (Quality-of-Service) classes, etc.) across workers in
// our experiments. Furthermore, all workers are ghOSt one-shots. Thus, put all
// worker sched items in the same work class.
static constexpr uint32_t kWorkClassIdentifier = 0;
}  // namespace


atomic_int Ghost_Status::have_global_init(0);
atomic_int Ghost_Status::thread_num(0);
mutex Ghost_Status::thread_init_mutex;
int Ghost_Status::worker_num(DEFAULT_WORKER_NUM);
int Ghost_Status::qos(DEFAULT_QOS);
Ghost Ghost_Status::ghost_(DEFAULT_WORKER_NUM, 10);

const absl::Duration Ghost_Status::deadline = absl::Microseconds(100);

void Ghost_Status::global_init(){
    printf("global_init started\n");
    // read in environment variables 
    char* worker_num_ptr = getenv("GHOST_WORKER_NUM");
    char* qos_ptr = getenv("GHOST_QOS");
    worker_num = worker_num_ptr == NULL ? DEFAULT_WORKER_NUM : atoi(worker_num_ptr);
    qos = qos_ptr == NULL ? DEFAULT_QOS : atoi(qos_ptr);
    fprintf(stderr, "worker_num: %d\n", worker_num);
    fprintf(stderr, "qos: %d\n", qos);
    // initialize ghost 
    ghost::work_class wc;
    ghost_.GetWorkClass(qos / 16, wc);
    wc.id = qos / 16;
    wc.flags = WORK_CLASS_ONESHOT;
    wc.qos = qos;
    // Write the max unsigned 64-bit integer as the deadline just in case we want
    // to run the experiment with the ghOSt EDF (Earliest-Deadline-First)
    // scheduler.
    wc.exectime = std::numeric_limits<uint64_t>::max();
    // 'period' is irrelevant because all threads scheduled by ghOSt are
    // one-shots.
    wc.period = 0;
    ghost_.SetWorkClass(qos / 16, wc);
    have_global_init = 1;
    fprintf(stderr, "global_init finished\n");
}

void Ghost_Status::thread_init(int sid){
    printf("thread sid %d init\n", sid);
    ghost::GhostThread::SetGlobalEnclaveCtlFdOnce();
    ghost::Gtid gtid_ = ghost::Gtid::Current();
    ghost::sched_item si;
    ghost_.GetSchedItem(sid, si);
    //printf("thread sid %d get scheditem\n", sid);
    si.sid = sid;
    si.wcid = qos / 16;
    si.gpid = gtid_.id();
    si.flags |= SCHED_ITEM_RUNNABLE;
    si.deadline = 0;
    si.empty_time = 0;
    si.yield_flag = 0;
    ghost_.SetSchedItem(sid, si);
    //printf("thread sid %d set scheditem\n", sid);
    const int ret = ghost::SchedTaskEnterGhost(/*pid=*/0);
    printf("thread sid %d schedTaskEnterGhost\n", sid);
    CHECK_EQ(ret, 0);
}
}