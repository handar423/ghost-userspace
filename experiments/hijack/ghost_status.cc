#include "ghost_status.h"
#include <stdio.h>

namespace ghost_test {

namespace {
// We do not need a different class of service (e.g., different expected
// runtimes, different QoS (Quality-of-Service) classes, etc.) across workers in
// our experiments. Furthermore, all workers are ghOSt one-shots. Thus, put all
// worker sched items in the same work class.
static constexpr uint32_t kWorkClassIdentifier = 0;
}  // namespace

Ghost Ghost_Status::ghost_(WORKER_NUM, 1);

atomic_int Ghost_Status::have_global_init(0);
atomic_int Ghost_Status::thread_num(0);

const absl::Duration Ghost_Status::deadline = absl::Microseconds(100);

void Ghost_Status::global_init(){
    fprintf(stderr, "global_init\n");
    ghost::work_class wc;
    ghost_.GetWorkClass(kWorkClassIdentifier, wc);
    wc.id = kWorkClassIdentifier;
    wc.flags = WORK_CLASS_ONESHOT;
    wc.qos = 16;
    // Write the max unsigned 64-bit integer as the deadline just in case we want
    // to run the experiment with the ghOSt EDF (Earliest-Deadline-First)
    // scheduler.
    wc.exectime = std::numeric_limits<uint64_t>::max();
    // 'period' is irrelevant because all threads scheduled by ghOSt are
    // one-shots.
    wc.period = 0;
    ghost_.SetWorkClass(kWorkClassIdentifier, wc);
    have_global_init = 1;
    fprintf(stderr, "global_init finished\n");
}

void Ghost_Status::thread_init(int sid){
    ghost::GhostThread::SetGlobalEnclaveCtlFdOnce();
    ghost::Gtid gtid_ = ghost::Gtid::Current();
    ghost::sched_item si;
    ghost_.GetSchedItem(sid, si);
    //printf("thread sid %d get scheditem\n", sid);
    si.sid = sid;
    si.wcid = kWorkClassIdentifier;
    si.gpid = gtid_.id();
    si.flags |= SCHED_ITEM_RUNNABLE;
    si.deadline = 0;
    ghost_.SetSchedItem(sid, si);
    //printf("thread sid %d set scheditem\n", sid);
    const int ret = ghost::SchedTaskEnterGhost(/*pid=*/0);
    //printf("thread sid %d schedTaskEnterGhost\n", sid);
    CHECK_EQ(ret, 0);
}
}