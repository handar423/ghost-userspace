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

Ghost Ghost_Status::ghost_(201, 1);

atomic_int Ghost_Status::need_global_init(0);

atomic_int Ghost_Status::have_global_init(0);

const absl::Duration Ghost_Status::deadline = absl::Microseconds(100);

void Ghost_Status::global_init(){
    fprintf(stderr, "global_init\n");
    ghost::work_class wc;
    ghost_.GetWorkClass(kWorkClassIdentifier, wc);
    wc.id = kWorkClassIdentifier;
    wc.flags = WORK_CLASS_ONESHOT;
    wc.qos = 2;
    // Write the max unsigned 64-bit integer as the deadline just in case we want
    // to run the experiment with the ghOSt EDF (Earliest-Deadline-First)
    // scheduler.
    wc.exectime = std::numeric_limits<uint64_t>::max();
    // 'period' is irrelevant because all threads scheduled by ghOSt are
    // one-shots.
    wc.period = 0;
    ghost_.SetWorkClass(kWorkClassIdentifier, wc);
    have_global_init = 1;
}

void Ghost_Status::thread_init(int sid){
    ghost::GhostThread::SetGlobalEnclaveCtlFdOnce();
    ghost::Gtid gtid_ = ghost::Gtid::Current();
    ghost::sched_item si;
    ghost_.GetSchedItem(sid, si);
    si.sid = sid;
    si.wcid = kWorkClassIdentifier;
    si.gpid = gtid_.id();
    si.flags = 0;
    si.deadline = 0;
    ghost_.SetSchedItem(sid, si);
    const int ret = ghost::SchedTaskEnterGhost(/*pid=*/0);
    CHECK_EQ(ret, 0);
}

void Ghost_Status::thread_set_runnable(int sid){
    ghost::sched_item si;
    ghost_.GetSchedItem(sid, si);
    si.deadline = Ghost::ToRawDeadline(ghost::MonotonicNow() + deadline);
    si.flags |= SCHED_ITEM_RUNNABLE;
    // All other flags were set in 'InitGhost' and do not need to be changed.
    ghost_.SetSchedItem(sid, si);
}

void Ghost_Status::thread_wait_runnable(int sid){
    ghost_.MarkIdle(sid);
    ghost_.WaitUntilRunnable(sid);
}

}