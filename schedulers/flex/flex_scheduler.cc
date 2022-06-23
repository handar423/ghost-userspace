// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "schedulers/flex/flex_scheduler.h"

#include "absl/strings/str_format.h"

// 单位纳秒
// #define VRAN_EMPTY_FLAG 10000000
#define FREQUENCY 2200

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#define get_vran_id(task) (task->vran_id)

namespace ghost {

static __inline__ unsigned long long GetCycleCount(void)
{
        unsigned hi,lo;
        __asm__ volatile("rdtsc":"=a"(lo),"=d"(hi));
        return ((unsigned long long)lo)|(((unsigned long long)hi)<<32);
}

void FlexTask::SetRuntime(absl::Duration new_runtime,
                              bool update_elapsed_runtime) {
  CHECK_GE(new_runtime, runtime);
  if (update_elapsed_runtime) {
    elapsed_runtime += new_runtime - runtime;
  }
  runtime = new_runtime;
}

// 这里会有10个微秒的误差，所以不用这个函数，因为系统调用太慢了所以也不用，我们直接在每一个任务返回时更新runtime（此时走syscall），而轮训的粒度由参数决定
void FlexTask::UpdateRuntime() {
  // We read the runtime from the status word rather than make a syscall. The
  // runtime in the status word may be out-of-sync with the true runtime by up
  // to 10 microseconds (which is the timer interrupt period). This is alright
  // for Flex since we only need an accurate measure of the task's runtime
  // to implement the preemption time slice and the global agent uses Abseil's
  // clock functionality to approximate the current elapsed runtime. The syscall
  // will return the true runtime for the task, but it needs to acquire a
  // runqueue lock if the task is currently running which harms tail latency.
  absl::Duration runtime = absl::Nanoseconds(status_word.runtime());
  SetRuntime(runtime, /* update_elapsed_runtime = */ true);
}

FlexScheduler::FlexScheduler(
    Enclave* enclave, CpuList cpus,
    std::shared_ptr<TaskAllocator<FlexTask>> allocator, int32_t global_cpu,
    absl::Duration loop_empty_time_slice,
    absl::Duration preemption_time_slice)
    : BasicDispatchScheduler(enclave, std::move(cpus), std::move(allocator)),
      global_cpu_(global_cpu),
      global_channel_(GHOST_MAX_QUEUE_ELEMS, /*node=*/0),
      loop_empty_time_slice_(loop_empty_time_slice),
      preemption_time_slice_(preemption_time_slice),
      batch_app_assigned_cpu_(MachineTopology()->EmptyCpuList()) {
  if (!cpus.IsSet(global_cpu_)) {
    Cpu c = cpus.Front();
    CHECK(c.valid());
    global_cpu_ = c.id();
  }

  vrans_.resize(MAX_VRAN_NUMBER);

  // 初始化分给batch
  batch_app_assigned_cpu_ += cpus;

  // 把global分出去
  batch_app_assigned_cpu_.Clear(global_cpu_);

  // debug
  yield_time = 0;
}

FlexScheduler::~FlexScheduler() {}

size_t FlexScheduler::RunqueueSize(vRAN_id_t vran_id) {
  if (vran_id) return run_queue_[vran_id].size();
  size_t size = 0;
  for (const auto& [qos, rq] : run_queue_) {
    // size += rq.size();
    size += ((qos & VRAN_ID_MASK) == 0) ? rq.size() : 0;
  }
  return size;
}

void FlexScheduler::EnclaveReady() {
  for (const Cpu& cpu : cpus()) {
    CpuState* cs = cpu_state(cpu);
    cs->agent = enclave()->GetAgent(cpu);
    CHECK_NE(cs->agent, nullptr);
  }
}

bool FlexScheduler::Available(const Cpu& cpu) {
  CpuState* cs = cpu_state(cpu);
  return cs->agent->cpu_avail();
}

void FlexScheduler::DumpAllTasks() {
  fprintf(stderr, "task        state       rq_pos  P\n");
  allocator()->ForEachTask([](Gtid gtid, const FlexTask* task) {
    absl::FPrintF(stderr, "%-12s%-12s%c\n", gtid.describe(),
                  FlexTask::RunStateToString(task->run_state),
                  task->prio_boost ? 'P' : '-');
    return true;
  });

  for (auto const& it : orchs_) it.second->DumpSchedParams();
}

void FlexScheduler::DumpState(const Cpu& agent_cpu, int flags) {
  static unsigned long long scheduling_time_from_last = 0;
  unsigned long long time_temp = GetCycleCount();
  if (flags & kDumpAllTasks) {
    DumpAllTasks();
  }

  uint32_t total_queue_size = 0;
  for(auto& [qos, rq] : run_queue_){
    total_queue_size += rq.size();
  }
  if (!(flags & kDumpStateEmptyRQ) && total_queue_size == 0) {
    return;
  }

  fprintf(stderr, "SchedState: ");
  for (const Cpu& cpu : batch_app_assigned_cpu_) {
    CpuState* cs = cpu_state(cpu);
    fprintf(stderr, "%d_0:", cpu.id());
    if (!cs->current) {
      fprintf(stderr, "none ");
    } else {
      Gtid gtid = cs->current->gtid;
      absl::FPrintF(stderr, "%s ", gtid.describe());
    }
  }
  for(int i = 1; i < MAX_VRAN_NUMBER; ++i){
    VranInfo& vran = vrans_[i];
    if(vran.available == false) continue;
    for (const Cpu& cpu : vran.cpu_assigns_) {
      CpuState* cs = cpu_state(cpu);
      fprintf(stderr, "%d_%u:", cpu.id(), i);
      if (!cs->current) {
        fprintf(stderr, "none ");
      } else {
        Gtid gtid = cs->current->gtid;
        absl::FPrintF(stderr, "%s ", gtid.describe());
      }
    }
  }
  fprintf(stderr, " rq_l_0=%ld", RunqueueSize(0));
  for(int i = 0; i < MAX_VRAN_NUMBER; ++i){
    if (vrans_[i].available)
      fprintf(stderr, " rq_l_%u=%ld", i, RunqueueSize(i << VRAN_INDEX_OFFSET));
  }

  fprintf(stderr, "\n");
  
  // 上次开始的scheduling time
  double time_spend = (time_temp - scheduling_time_from_last) *1.0 / FREQUENCY;
  fprintf(stderr, "single scheduling time in us: %.3lf\navg yield in us:%.3lf \nsum CPU:%.3lf\n", 
          time_spend / scheduling_time, 
          (time_spend / yield_time), 
          vran_sum_number * 100.0 / scheduling_time);

  yield_time = 0;
  scheduling_time = 0;
  vran_sum_number = 0;
  scheduling_time_from_last = GetCycleCount();
}

FlexScheduler::CpuState* FlexScheduler::cpu_state_of(
    const FlexTask* task) {
  CHECK(task->oncpu());
  CpuState* result = &cpu_states_[task->cpu];
  CHECK_EQ(result->current, task);
  return result;
}

// Map the leader's shared memory region if we haven't already done so.
void FlexScheduler::HandleNewGtid(FlexTask* task, pid_t tgid) {
  CHECK_GE(tgid, 0);

  if (orchs_.find(tgid) == orchs_.end()) {
    auto orch = std::make_shared<FlexOrchestrator>();
    if (!orch->Init(tgid)) {
      // If the task's group leader has already exited and closed the PrioTable
      // fd while we are handling TaskNew, it is possible that we cannot find
      // the PrioTable.
      // Just set has_work so that we schedule this task and allow it to exit.
      // We also need to give it an sp; various places call task->sp->qos_.
      static FlexSchedParams dummy_sp;
      task->has_work = true;
      task->sp = &dummy_sp;
      return;
    }
    auto pair = std::make_pair(tgid, std::move(orch));
    orchs_.insert(std::move(pair));
  }
}

void FlexScheduler::UpdateTaskRuntime(FlexTask* task,
                                          absl::Duration new_runtime,
                                          bool update_elapsed_runtime) {
  task->SetRuntime(new_runtime, update_elapsed_runtime);
  if (task->sp) {
    // We only call 'UpdateTaskRuntime' on a task that is on a CPU, not on a
    // task that is queued (note that a queued task cannot accumulate runtime)
    CHECK(task->oncpu());
  } else {
    // The task is not associated with a sched item, so it should not be queued
    CHECK(!task->queued());
  }
}

// 增加CPU分配
void FlexScheduler::TaskNew(FlexTask* task, const Message& msg) {
  const ghost_msg_payload_task_new* payload =
      static_cast<const ghost_msg_payload_task_new*>(msg.payload());

  DCHECK_LE(payload->runtime, task->status_word.runtime());

  UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                    /* update_elapsed_runtime = */ false);
  task->seqnum = msg.seqnum();
  task->run_state = FlexTask::RunState::kBlocked;  // Need this in the
                                                       // runnable case anyway.

  const Gtid gtid(payload->gtid);
  const pid_t tgid = gtid.tgid();
  HandleNewGtid(task, tgid);

  if (payload->runnable) Enqueue(task);
    
  num_tasks_++;

  auto iter = orchs_.find(tgid);
  if (iter != orchs_.end()) {
    task->orch = iter->second;
  } else {
    // It's possible to have no orch if the task died and closed its fds before
    // we found its PrioTable.
    task->orch = nullptr;
  }

  if (!in_discovery_) {
    // Get the task's scheduling parameters (potentially updating its position
    // in the runqueue).
    task->orch->GetSchedParams(task->gtid, kSchedCallbackFunc);
  }

}

void FlexScheduler::TaskRunnable(FlexTask* task, const Message& msg) {
  const ghost_msg_payload_task_wakeup* payload =
      static_cast<const ghost_msg_payload_task_wakeup*>(msg.payload());

  CHECK(task->blocked());

  // A non-deferrable wakeup gets the same preference as a preempted task.
  // This is because it may be holding locks or resources needed by other
  // tasks to make progress.
  task->prio_boost = !payload->deferrable;

  Enqueue(task, /* back = */ false);
}

void FlexScheduler::TaskDeparted(FlexTask* task, const Message& msg) {}

void FlexScheduler::TaskDead(FlexTask* task, const Message& msg) {
  CHECK_EQ(task->run_state,
           FlexTask::RunState::kBlocked);  // Need to schedule to exit.

  uint32_t vran_id = get_vran_id(task);
  // fprintf(stderr, "one vran %d task end\n", vran_id);
  if (vran_id){
    VranInfo& vran = vrans_[vran_id];
    if(vran.max_cpu_number_ == 1){
      // fprintf(stderr, "no vran %d task left\n", vran_id);
      batch_app_assigned_cpu_ += vran.cpu_assigns_;
      vran.cpu_assigns_ = MachineTopology()->EmptyCpuList();
      vran.available = false;
      vran.busy_times = 0;
      vran_cpu_number = 0;
    } else {
      vran.max_cpu_number_ -= 1;
      // fprintf(stderr, "%d vran %d task left\n", vran_max_cpu_number_[vran_id], vran_id);
    }
  }

  allocator()->FreeTask(task);

  num_tasks_--;
}

// 更新CPU时间
void FlexScheduler::TaskBlocked(FlexTask* task, const Message& msg) {
  const ghost_msg_payload_task_blocked* payload =
      reinterpret_cast<const ghost_msg_payload_task_blocked*>(msg.payload());

  DCHECK_LE(payload->runtime, task->status_word.runtime());

  // 两者单位一致，此处不处理
  // uint64_t last_ran = payload->runtime - task->last_runtime;
  vrans_[get_vran_id(task)].empty_times_from_last_schduler_ += 1;
  // task->last_runtime = payload->runtime;
  // printf("vran block one task time\n");

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // blocking (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
  } else if (task->queued()) {
    RemoveFromRunqueue(task);
  } else {
    CHECK(task->paused());
  }
  task->run_state = FlexTask::RunState::kBlocked;
}

void FlexScheduler::TaskPreempted(FlexTask* task, const Message& msg) {
  const ghost_msg_payload_task_preempt* payload =
      reinterpret_cast<const ghost_msg_payload_task_preempt*>(msg.payload());

  DCHECK_LE(payload->runtime, task->status_word.runtime());

  task->preempted = true;
  task->prio_boost = true;

  // States other than the typical kOnCpu are possible here:
  // We could be kQueued from a TASK_NEW that was immediately preempted.
  // We could be kPaused if agent-initiated preemption raced with kernel
  // preemption (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
    Enqueue(task);
  } else if (task->queued()) {
    // The task was preempted, so add it to the front of run queue. We do this
    // because (1) the task could have been holding an important lock when it
    // was preempted so we could improve performance by scheduling the task
    // again as soon as possible and (2) because the Flex algorithm assumes
    // tasks are not preempted by other scheduling classes so getting the task
    // scheduled back onto the CPU as soon as possible is important to
    // faithfully implement the algorithm.
    RemoveFromRunqueue(task);
    Enqueue(task);
  } else {
    CHECK(task->paused());
  }
}

// 更新CPU时间
void FlexScheduler::TaskYield(FlexTask* task, const Message& msg) {
  const ghost_msg_payload_task_yield* payload =
      reinterpret_cast<const ghost_msg_payload_task_yield*>(msg.payload());

  DCHECK_LE(payload->runtime, task->status_word.runtime());
  
  // 两者单位一致，此处不处理
  // uint64_t last_ran = payload->runtime - task->last_runtime;
  // if(vran_id != 0){
  vrans_[get_vran_id(task)].empty_times_from_last_schduler_ = 1;
  // }
  // task->last_runtime = payload->runtime;
  // printf("vran %d one task time %ld\n", vran_id, last_ran);

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // yielding (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    UpdateTaskRuntime(task, absl::Nanoseconds(payload->runtime),
                      /* update_elapsed_runtime= */ true);
    CpuState* cs = cpu_state_of(task);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
    Yield(task);
  } else {
    CHECK(task->queued() || task->paused());
  }
}

void FlexScheduler::DiscoveryStart() { in_discovery_ = true; }

void FlexScheduler::DiscoveryComplete() {
  for (auto& scraper : orchs_) {
    scraper.second->RefreshAllSchedParams(kSchedCallbackFunc);
  }
  in_discovery_ = false;
}

void FlexScheduler::Yield(FlexTask* task) {
  // An oncpu() task can do a sched_yield() and get here via
  // FlexTaskYield(). We may also get here if the scheduler wants to inhibit
  // a task from being picked in the current scheduling round (see
  // GlobalSchedule()).
  CHECK(task->oncpu() || task->queued());
  task->run_state = FlexTask::RunState::kYielding;
  yielding_tasks_.emplace_back(task);
}

void FlexScheduler::Unyield(FlexTask* task) {
  CHECK(task->yielding());

  auto it = std::find(yielding_tasks_.begin(), yielding_tasks_.end(), task);
  CHECK(it != yielding_tasks_.end());
  yielding_tasks_.erase(it);
  Enqueue(task);
}

void FlexScheduler::Enqueue(FlexTask* task, bool back) {
  CHECK_EQ(task->unschedule_level,
           FlexTask::UnscheduleLevel::kNoUnschedule);
  if (!task->has_work) {
    // We'll re-enqueue when this FlexTask has work to do during periodic
    // scraping of PrioTable.
    task->run_state = FlexTask::RunState::kPaused;
    return;
  }

  task->run_state = FlexTask::RunState::kQueued;
  if (back && !task->prio_boost) {
    run_queue_[task->sp->GetQoS()].emplace_back(task);
  } else {
    run_queue_[task->sp->GetQoS()].emplace_front(task);
  }
}

// 等于0时行为等于普通的Dequeue
FlexTask* FlexScheduler::Dequeue(vRAN_id_t vran_id) {
  if (RunqueueEmpty(vran_id)) {
    return nullptr;
  }
  struct FlexTask* task = nullptr;

  if(vran_id == 0){
    std::deque<FlexTask*>& rq = run_queue_[FirstFilledRunqueue()];
    task = rq.front();
    CHECK_NE(task, nullptr);
    CHECK(task->has_work);
    CHECK_EQ(task->unschedule_level,
            FlexTask::UnscheduleLevel::kNoUnschedule);
    rq.pop_front();
  } else {
    std::deque<FlexTask*>& rq = run_queue_[vran_id];
    task = rq.front();
    CHECK_NE(task, nullptr);
    CHECK(task->has_work);
    CHECK_EQ(task->unschedule_level,
            FlexTask::UnscheduleLevel::kNoUnschedule);
    rq.pop_front();
  }

  return task;
}

// 等于0时行为等于普通的Peek
FlexTask* FlexScheduler::Peek(vRAN_id_t vran_id) {
  if (RunqueueEmpty(vran_id)) {
    return nullptr;
  }
  struct FlexTask* task = nullptr;
  
  if(vran_id == 0){
    task = run_queue_[FirstFilledRunqueue()].front();
  } else {
    task = run_queue_[vran_id].front();
  }
  CHECK(task->has_work);
  CHECK_EQ(task->unschedule_level,
           FlexTask::UnscheduleLevel::kNoUnschedule);

  return task;
}

void FlexScheduler::RemoveFromRunqueue(FlexTask* task) {
  CHECK(task->queued());

  for (auto& [qos, rq] : run_queue_) {
    for (int pos = rq.size() - 1; pos >= 0; pos--) {
      // The [] operator for 'std::deque' is constant time
      if (rq[pos] == task) {
        task->run_state = FlexTask::RunState::kPaused;
        rq.erase(rq.cbegin() + pos);
        return;
      }
    }
  }
  // This state is unreachable because the task is queued
  CHECK(false);
}

void FlexScheduler::UnscheduleTask(FlexTask* task) {
  CHECK_NE(task, nullptr);
  CHECK(task->oncpu());

  // Preempt `cpu` while ensuring that the transaction is committed on
  // the uber agent CPU (see `COMMIT_AT_TXN_COMMIT` below).
  Cpu cpu = topology()->cpu(task->cpu);
  RunRequest* req = enclave()->GetRunRequest(cpu);
  req->Open({
      .target = Gtid(0),  // redundant but emphasize that this is a preemption.
      .commit_flags = COMMIT_AT_TXN_COMMIT,
  });
  CHECK(req->Commit());

  CpuState* cs = cpu_state(cpu);
  cs->current = nullptr;
  task->run_state = FlexTask::RunState::kPaused;
  task->unschedule_level = FlexTask::UnscheduleLevel::kNoUnschedule;
}

void FlexScheduler::SchedParamsCallback(FlexOrchestrator& orch,
                                            const FlexSchedParams* sp,
                                            Gtid oldgtid) {
  Gtid gtid = sp->GetGtid();

  // TODO: Get it off cpu if it is running. Assumes that
  // oldgpid wasn't moved around to a later spot in the PrioTable.
  // Get it off the runqueue if it is queued.
  // Implement Scheduler::EraseFromRunqueue(oldgpid);
  CHECK(!oldgtid || (oldgtid == gtid));

  if (!gtid) {  // empty sched_item.
    return;
  }

  // Normally, the agent writes the Runnable bit in the PrioTable for
  // Repeatables (see Case C, below).  However, the old agent may have crashed
  // before it could set the bit, so we must do it.
  if (in_discovery_ && orch.Repeating(sp) && !sp->HasWork()) {
    orch.MakeEngineRunnable(sp);
  }

  FlexTask* task = allocator()->GetTask(gtid);
  if (!task) {
    // We are too early (i.e. haven't seen MSG_TASK_NEW for gtid) in which
    // case ignore the update for now. We'll grab the latest FlexSchedParams
    // from shmem in the MSG_TASK_NEW handler.
    //
    // We are too late (i.e have already seen MSG_TASK_DEAD for gtid) in
    // which case we just ignore the update.
    return;
  }

  // kYielding is an ephemeral state that prevents the task from being
  // picked in this scheduling iteration (the intent is to give other
  // tasks a chance to run). This is an implementation detail and is
  // not required by sched_yield() system call.
  //
  // To simplify state transitions we undo the kYielding behavior if
  // a FlexSchedParams update is also detected in this scheduling iteration.
  if (task->yielding()) Unyield(task);

  // Copy updated params into task->..
  const bool had_work = task->has_work;

  // 为对应的VRAN增加CPU分配上限
  if(unlikely(task->sp == nullptr && (sp->GetQoS() & VRAN_ID_MASK))) {
    uint32_t vran_id = (sp->GetQoS() & VRAN_ID_MASK) >> VRAN_INDEX_OFFSET;
    VranInfo& vran = vrans_[vran_id];
    fprintf(stderr, "find new vran %d task\n", vran_id);
    if(vran.available == false){
      vran.available = true;
      vran.max_cpu_number_ = 1;
      vran.empty_times_from_last_schduler_ = 0;
    } else {
      vran.max_cpu_number_ += 1;
    }

    task->vran_id = vran_id;
    fprintf(stderr, "have find %d vran %d task\n", vran.max_cpu_number_, vran_id);

    // 若有，分配一个新的CPU
    if(batch_app_assigned_cpu_.Size() > 1) {
      Cpu front = batch_app_assigned_cpu_.Front();
      vran.cpu_assigns_.Set(front);
      ++vran_cpu_number;
      batch_app_assigned_cpu_.Clear(front);
    }
  }
  task->sp = sp;
  task->has_work = sp->HasWork();
  task->wcid = sp->GetWorkClass();

  if (had_work) {
    task->UpdateRuntime();
    // The runtime for the ghOSt task needs to be updated as the sched item was
    // repurposed for a different closure. We don't want to bill the new closure
    // for CPU time that was consumed by the old closure.
    task->elapsed_runtime = absl::ZeroDuration();
  }

  // A kBlocked task is not affected by any changes to FlexSchedParams.
  if (task->blocked()) {
    return;
  }

  // Case#  had_work  has_work    run_state change
  //  (a)      0         0        none
  //  (b)      0         1        kPaused -> kQueued
  //  (c)      1         0        kQueued/kOnCpu -> kPaused
  //  (d)      1         1        none
  if (!had_work) {
    CHECK(
        task->paused() ||
        (task->oncpu() && task->unschedule_level ==
                              FlexTask::UnscheduleLevel::kMustUnschedule));
    if (task->has_work) {
      if (task->paused()) {
        // For repeatables, the orchestrator indicates that it is done polling
        // via the had_work -> !has_work edge. The agent exclusively generates
        // the !had_work -> has_work edge when it is time for the orchestrator
        // to poll again. We permit the latter edge here for expediency when
        // handling a new task.
        Enqueue(task);  // case (b).
      } else if (task->oncpu()) {
        // We check this above, but do it again here to make it clear to anyone
        // reading how we get to this branch.
        CHECK_EQ(task->unschedule_level,
                 FlexTask::UnscheduleLevel::kMustUnschedule);

        // The task is currently running on the CPU because it had work. It was
        // then marked as having no work, so the level was set to
        // `kMustUnschedule`. However, the sched item was updated multiple times
        // in the stream and on this successive read, we noticed that the sched
        // item now has work again. As such, this situation is basically the
        // same as case (d) where the sched item had work and, after its update,
        // still has work. Thus, set the level to `kCouldUnschedule` since we no
        // longer need to unschedule this task in `GlobalSchedule` unless there
        // is an already queued task in the runqueue that needs to take the
        // place of `task` on the CPU.
        task->unschedule_level =
            FlexTask::UnscheduleLevel::kCouldUnschedule;
      }
    }
    return;  // case (a).
  }

  if (had_work) {
    CHECK(!task->paused());
    if (task->has_work) {  // case (d).
      if (task->queued()) {
        // Repeatables should not change their closure, so repeatables should
        // not travel the had_work -> has_work edge
        CHECK(!orch.Repeating(task->sp));
      }

      // Allow the task to be unscheduled in 'GlobalSchedule' so that another
      // task enqueued on the runqueue may run. Do not unschedule the task if
      // there is no other queued task to take its place since we would then
      // unnecessarily unschedule then reschedule 'task'.
      //
      // This would add unnecessary overhead:
      // if (task->oncpu()) {
      //   UnscheduleTask(task);
      //   Enqueue(task);
      // }
      // ...
      // ('task' immediately gets rescheduled in the next call to
      // 'GlobalSchedule' because the runqueue is short enough such that no
      // queued tasks need to take the place of 'task'.)
      if (task->oncpu()) {
        task->unschedule_level =
            FlexTask::UnscheduleLevel::kCouldUnschedule;
      }
    } else {  // case (c).
      if (task->oncpu()) {
        // This task must stop running but we defer its unschedule until
        // `GlobalSchedule`. In `GlobalSchedule` we could pair its unschedule
        // with a schedule for another runnable task, saving the overhead of a
        // resched. Furthermore, if this task's sched item was updated more than
        // once in the stream, it is possible that the final read to the sched
        // item would indicate the task has work again, making a resched
        // potentially unnecessary.
        //
        // This would add unnecessary overhead:
        // UnscheduleTask(task);
        // ...
        // (1: Some task `task_new` gets scheduled onto the CPU that `task` was
        // unscheduled from.)
        // (2: Or the sched item for `task` is updated more than once in the
        // stream and the final read to the sched item indicates that `task` has
        // work again and does not need to be unscheduled.)
        task->unschedule_level = FlexTask::UnscheduleLevel::kMustUnschedule;
      } else if (task->queued()) {
        RemoveFromRunqueue(task);
      } else {
        CHECK(0);
      }
      CHECK(task->paused() ||
            (task->oncpu() &&
             task->unschedule_level ==
                 FlexTask::UnscheduleLevel::kMustUnschedule));
      if (orch.Repeating(task->sp)) {
        paused_repeatables_.push_back(task);
      }
    }
  }
}

void FlexScheduler::UpdateSchedParams() {
  for (auto& scraper : orchs_) {
    scraper.second->RefreshSchedParams(kSchedCallbackFunc);
  }
}

bool FlexScheduler::SkipForSchedule(int iteration, const Cpu& cpu) {
  CpuState* cs = cpu_state(cpu);
  // The logic is complex, so we break it into multiple if statements rather
  // than compress it into a single boolean expression that we return
  if (!Available(cpu) || cpu.id() == GetGlobalCPUId()) {
    // Cannot schedule on this CPU.
    return true;
  }
  if (iteration == 0 && cs->current &&
      cs->current->unschedule_level <
          FlexTask::UnscheduleLevel::kMustUnschedule) {
    // Don't preempt the task on this CPU in the first iteration. We first
    // try to see if there is an idle CPU we can run the next task on. The only
    // exception is if the currently running task must be unscheduled... it is
    // fine to preempt the task in the first iteration in that case (especially
    // since we would prefer to preempt a task with a level of `kMustUnschedule`
    // rather than a task on a different CPU with a level of `kCouldUnschedule`.
    return true;
  }
  if (iteration == 1 && cs->next) {
    // We already assigned a task to this CPU in the first iteration.
    return true;
  }
  return false;
}

// 分类处理
// 对于DU，抢占逻辑删除
void FlexScheduler::GlobalSchedule(const StatusWord& agent_sw,
                                       StatusWord::BarrierToken agent_sw_last) {
  // List of CPUs with open transactions.
  ++scheduling_time;
  CpuList open_cpus = MachineTopology()->EmptyCpuList();
  const absl::Time now = absl::Now();

  // 重新分配CPU
  for (int i = 1; i < MAX_VRAN_NUMBER; ++i) {
  // 一般来说，认为没有16个vran那么多，测试中应该只有一到两个
  // batch_app不算（所有vrans_[0]没有意义）
    VranInfo& vran = vrans_[i];
    if(likely(vran.available == false))
      continue;
    yield_time +=vran.empty_times_from_last_schduler_;
    // 若有，分配一个新的CPU
    if(vran.busy_times > 3 && 
       vran.empty_times_from_last_schduler_ == 0 && 
       batch_app_assigned_cpu_.Size() > 1 && 
       vran.cpu_assigns_.Size() < vran.max_cpu_number_){
        if(batch_app_assigned_cpu_.IsSet(vran.last_assign_cpus_)){
          vran.cpu_assigns_.Set(vran.last_assign_cpus_);
          batch_app_assigned_cpu_.Clear(vran.last_assign_cpus_);
        } else {
          Cpu front = batch_app_assigned_cpu_.Front();
          vran.cpu_assigns_.Set(front);
          batch_app_assigned_cpu_.Clear(front);
        }
      ++vran_cpu_number;
    } else if (vran.empty_times_from_last_schduler_ && vran.busy_times < 3 && vran.cpu_assigns_.Size() > 1){
      // fprintf(stderr, "remove cpu %d\n", empty_time);
      Cpu front = vran.cpu_assigns_.Front();
      batch_app_assigned_cpu_.Set(front);
      vran.last_assign_cpus_ = front.id();
      vran.cpu_assigns_.Clear(front);
      --vran_cpu_number;
    }
    vran.busy_times = (!(vran.empty_times_from_last_schduler_)) ? vran.busy_times + 1 : 0;
    vran.empty_times_from_last_schduler_ = 0;

    // TODO: Refactor this loop
    for (int j = 0; j < 2; j++) {
      CpuList updated_cpus = MachineTopology()->EmptyCpuList();
      for (const Cpu& cpu : vran.cpu_assigns_) {
        CpuState* cs = cpu_state(cpu);
        if (SkipForSchedule(j, cpu)) {
          continue;
        }
        vRAN_id_t vran_id = i << VRAN_INDEX_OFFSET;

      again:
        if (cs->current) {
          // Preempt the current task if either:
          // 1. A higher-priority task wants to run.
          // 2. The task's unschedule level is at least `kCouldUnschedule`, making
          // the task eligible for preemption.
          continue;
        }
        FlexTask* to_run = Dequeue(vran_id);
        if (!to_run) {
          // No tasks left to schedule.
          break;
        }

        // The chosen task was preempted earlier but hasn't gotten off the
        // CPU. Make it ineligible for selection in this scheduling round.
        // 不应该出现于vran
        if (to_run->status_word.on_cpu()) {
          Yield(to_run);
          goto again;
        }

        cs->next = to_run;

        updated_cpus.Set(cpu.id());
      }

      for (const Cpu& cpu : vran.cpu_assigns_) {
        CpuState* cs = cpu_state(cpu);
        // Make a copy of the `cs->current` pointer since we need to access the
        // task after it is unscheduled. `UnscheduleTask` sets `cs->current` to
        // `nullptr`.
        FlexTask* task = cs->current;
        if (task) {
          if (!cs->next) {
            if (task->unschedule_level ==
                FlexTask::UnscheduleLevel::kCouldUnschedule) {
              // We set the level to `kNoUnschedule` since no task is being
              // scheduled in place of `task` on `cpu`. We cannot set the level to
              // `kNoUnschedule` when trying to schedule another task on this
              // `cpu` because that schedule may fail, so `task` needs to be
              // directly unscheduled in that case so that `task` does not get
              // preference over tasks waiting in the runqueue.
              //
              // Note that an unschedule is optional for a level of
              // `kCouldUnschedule`, so we do not initiate an unschedule unlike
              // below for `kMustUnschedule`.
              task->unschedule_level =
                  FlexTask::UnscheduleLevel::kNoUnschedule;
            } else if (task->unschedule_level ==
                      FlexTask::UnscheduleLevel::kMustUnschedule) {
              // `task` must be unscheduled and we have no new task schedule to
              // pair the unschedule with, so initiate the unschedule directly.
              UnscheduleTask(task);
            }
          }

          // Four cases:
          //
          // If there is a new task `cs->next` to run next (i.e., `cs->next` !=
          // `nullptr`):
          //   Case 1: If the level is `kCouldUnschedule`, we will attempt the
          //   schedule below and directly initiate an unschedule of `task` if
          //   that schedule fails.
          //
          //   Case 2: If the level is `kMustUnschedule`, we will attempt the
          //   schedule below and directly initiate an unschedule of `task` if
          //   that schedule fails.
          //
          // If there is no new task `cs->current` to run next (i.e., `cs->next`
          // == `nullptr`):
          //   Case 3: If the level of `task` was `kCouldUnschedule`, we set it to
          //   `kNoUnschedule` above.
          //
          //   Case 4: If the level of `task` was `kMustUnschedule`, we directly
          //   initiated an unschedule of `task` above. `UnscheduleTask(task)`
          //   sets the level of `task` to `kNoUnschedule`.
          CHECK(cs->next || task->unschedule_level ==
                                FlexTask::UnscheduleLevel::kNoUnschedule);
        }
      }

      for (const Cpu& cpu : updated_cpus) {
        CpuState* cs = cpu_state(cpu);

        FlexTask* next = cs->next;
        CHECK_NE(next, nullptr);

        if (cs->current == next) continue;

        RunRequest* req = enclave()->GetRunRequest(cpu);
        req->Open({
            .target = next->gtid,
            .target_barrier = next->seqnum,
            .commit_flags = COMMIT_AT_TXN_COMMIT,
        });

        open_cpus.Set(cpu.id());
      }
    }
  }

  vran_sum_number += vran_cpu_number;
  for(const Cpu& cpu : batch_app_assigned_cpu_)
    vran_sum_number += (cpu_state(cpu)->current != nullptr);

  // TODO: Refactor this loop
  for (int i = 0; i < 2; i++) {
    CpuList updated_cpus = MachineTopology()->EmptyCpuList();
    for (const Cpu& cpu : batch_app_assigned_cpu_) {
      CpuState* cs = cpu_state(cpu);
      if (SkipForSchedule(i, cpu)) {
        continue;
      }
      vRAN_id_t vran_id = 0;

    again1:
      if (cs->current) {
        // Approximate the elapsed runtime rather than update the runtime with
        // 'cs->current->UpdateRuntime()' to get the true elapsed runtime from
        // 'cs->current->elapsed_runtime'. Calls to 'UpdateRuntime()' grab the
        // runqueue lock, so calling this for tasks on many CPUs harms tail
        // latency.
        absl::Duration elapsed_runtime = now - cs->current->last_ran;

        // Preempt the current task if either:
        // 1. A higher-priority task wants to run.
        // 2. The next task to run has the same priority as the current task,
        // the current task has used up its time slice, and the current task is
        // not a repeatable.
        // 3. The task's unschedule level is at least `kCouldUnschedule`, making
        // the task eligible for preemption.
        FlexTask* peek = Peek(vran_id);
        bool should_preempt = false;
        if (peek) {
          uint32_t current_qos = cs->current->sp->GetQoS();
          uint32_t peek_qos = peek->sp->GetQoS();

          // 当且仅当batch被vran抢占
          if (current_qos < peek_qos) {
            should_preempt = true;
          } else if (current_qos == peek_qos) {
            if (elapsed_runtime >= preemption_time_slice_ &&
                cs->current->orch &&
                !cs->current->orch->Repeating(cs->current->sp)) {
              should_preempt = true;
            } else if (cs->current->unschedule_level >=
                       FlexTask::UnscheduleLevel::kCouldUnschedule) {
              should_preempt = true;
            }
          }
        }
        if (!should_preempt) {
          continue;
        }
      }
      FlexTask* to_run = Dequeue(vran_id);
      if (!to_run) {
        // No tasks left to schedule.
        break;
      }

      // The chosen task was preempted earlier but hasn't gotten off the
      // CPU. Make it ineligible for selection in this scheduling round.
      // 不应该出现于vran
      if (to_run->status_word.on_cpu()) {
        Yield(to_run);
        goto again1;
      }

      cs->next = to_run;

      updated_cpus.Set(cpu.id());
    }

    for (const Cpu& cpu : batch_app_assigned_cpu_) {
      CpuState* cs = cpu_state(cpu);
      // Make a copy of the `cs->current` pointer since we need to access the
      // task after it is unscheduled. `UnscheduleTask` sets `cs->current` to
      // `nullptr`.
      FlexTask* task = cs->current;
      if (task) {
        if (!cs->next) {
          if (task->unschedule_level ==
              FlexTask::UnscheduleLevel::kCouldUnschedule) {
            // We set the level to `kNoUnschedule` since no task is being
            // scheduled in place of `task` on `cpu`. We cannot set the level to
            // `kNoUnschedule` when trying to schedule another task on this
            // `cpu` because that schedule may fail, so `task` needs to be
            // directly unscheduled in that case so that `task` does not get
            // preference over tasks waiting in the runqueue.
            //
            // Note that an unschedule is optional for a level of
            // `kCouldUnschedule`, so we do not initiate an unschedule unlike
            // below for `kMustUnschedule`.
            task->unschedule_level =
                FlexTask::UnscheduleLevel::kNoUnschedule;
          } else if (task->unschedule_level ==
                     FlexTask::UnscheduleLevel::kMustUnschedule) {
            // `task` must be unscheduled and we have no new task schedule to
            // pair the unschedule with, so initiate the unschedule directly.
            UnscheduleTask(task);
          }
        }

        // Four cases:
        //
        // If there is a new task `cs->next` to run next (i.e., `cs->next` !=
        // `nullptr`):
        //   Case 1: If the level is `kCouldUnschedule`, we will attempt the
        //   schedule below and directly initiate an unschedule of `task` if
        //   that schedule fails.
        //
        //   Case 2: If the level is `kMustUnschedule`, we will attempt the
        //   schedule below and directly initiate an unschedule of `task` if
        //   that schedule fails.
        //
        // If there is no new task `cs->current` to run next (i.e., `cs->next`
        // == `nullptr`):
        //   Case 3: If the level of `task` was `kCouldUnschedule`, we set it to
        //   `kNoUnschedule` above.
        //
        //   Case 4: If the level of `task` was `kMustUnschedule`, we directly
        //   initiated an unschedule of `task` above. `UnscheduleTask(task)`
        //   sets the level of `task` to `kNoUnschedule`.
        CHECK(cs->next || task->unschedule_level ==
                              FlexTask::UnscheduleLevel::kNoUnschedule);
      }
    }

    for (const Cpu& cpu : updated_cpus) {
      CpuState* cs = cpu_state(cpu);

      FlexTask* next = cs->next;
      CHECK_NE(next, nullptr);

      if (cs->current == next) continue;

      RunRequest* req = enclave()->GetRunRequest(cpu);
      req->Open({
          .target = next->gtid,
          .target_barrier = next->seqnum,
          .commit_flags = COMMIT_AT_TXN_COMMIT,
      });

      open_cpus.Set(cpu.id());
    }
  }

  if (!open_cpus.Empty()) {
    enclave()->CommitRunRequests(open_cpus);
  }

  for (const Cpu& cpu : open_cpus) {
    CpuState* cs = cpu_state(cpu);
    FlexTask* next = cs->next;
    cs->next = nullptr;

    RunRequest* req = enclave()->GetRunRequest(cpu);
    DCHECK(req->committed());
    if (req->state() == GHOST_TXN_COMPLETE) {
      if (cs->current) {
        FlexTask* prev = cs->current;
        CHECK(prev->oncpu());

        // The schedule succeeded, so `prev` was unscheduled.
        prev->unschedule_level = FlexTask::UnscheduleLevel::kNoUnschedule;

        // Update runtime of the preempted task.
        prev->UpdateRuntime();

        // Enqueue the preempted task so it is eligible to be picked again.
        Enqueue(prev);
      }

      // FlexTask latched successfully; clear state from an earlier run.
      //
      // Note that 'preempted' influences a task's run_queue position
      // so we clear it only after the transaction commit is successful.
      cs->current = next;
      next->run_state = FlexTask::RunState::kOnCpu;
      next->cpu = cpu.id();
      next->preempted = false;
      next->prio_boost = false;
      // Clear the elapsed runtime so that the preemption timer is reset for
      // this task
      next->elapsed_runtime = absl::ZeroDuration();
      next->last_ran = absl::Now();
    } else {
      // Need to requeue in the stale case.
      Enqueue(next, /* back = */ false);
      if (cs->current && cs->current->unschedule_level >=
                             FlexTask::UnscheduleLevel::kCouldUnschedule) {
        // TODO: Add a commit option that will idle the CPU if a ghOSt
        // task is currently running on the CPU and a transaction to run a new
        // task on that CPU fails. This will allow us to get the desired
        // behavior here without the overhead of calling
        // `UnscheduleTask(cs->current)`.
        UnscheduleTask(cs->current);
      }
    }
  }

  // Yielding tasks are moved back to the runqueue having skipped one round
  // of scheduling decisions.
  if (!yielding_tasks_.empty()) {
    for (FlexTask* t : yielding_tasks_) {
      CHECK_EQ(t->run_state, FlexTask::RunState::kYielding);
      Enqueue(t);
    }
    yielding_tasks_.clear();
  }
  // Check to see if any repeatables are eligible to run
  for (auto it = paused_repeatables_.begin();
       it != paused_repeatables_.end();) {
    FlexTask* task = *it;
    CHECK_NE(task->orch, nullptr);
    absl::Duration wait = absl::Now() - task->last_ran;
    if (wait >= task->orch->GetWorkClassPeriod(task->sp->GetWorkClass())) {
      // The repeatable should run again
      task->orch->MakeEngineRunnable(task->sp);
      task->has_work = true;
      Enqueue(task);
      // 'erase' returns an iterator to the next element in the vector
      it = paused_repeatables_.erase(it);
    } else {
      it++;
    }
  }
}

void FlexScheduler::PickNextGlobalCPU(
    StatusWord::BarrierToken agent_barrier) {
  // TODO: Select CPUs more intelligently.
  for (const Cpu& cpu : cpus()) {
    if (Available(cpu) && cpu.id() != GetGlobalCPUId()) {
      CpuState* cs = cpu_state(cpu);
      FlexTask* prev = cs->current;

      if (prev) {
        if(prev->vran_id) continue;
        CHECK(prev->oncpu());
        // Vacate CPU for running Global agent.
        UnscheduleTask(prev);

        // Set 'prio_boost' to make it reschedule asap in case 'prev' is
        // holding a critical resource (prio_boost also means we can get
        // away with not updating the task's runtime or sched_deadline).
        prev->prio_boost = true;
        Enqueue(prev);
      }

      batch_app_assigned_cpu_.Set(global_cpu_);
      batch_app_assigned_cpu_.Clear(cpu);
      SetGlobalCPU(cpu);
      enclave()->GetAgent(cpu)->Ping();
      break;
    }
  }
}

std::unique_ptr<FlexScheduler> SingleThreadFlexScheduler(
    Enclave* enclave, CpuList cpus, int32_t global_cpu,
    absl::Duration loop_empty_time_slice,
    absl::Duration preemption_time_slice) {
  auto allocator =
      std::make_shared<SingleThreadMallocTaskAllocator<FlexTask>>();
  auto scheduler = absl::make_unique<FlexScheduler>(
      enclave, std::move(cpus), std::move(allocator), global_cpu,
      loop_empty_time_slice, preemption_time_slice);
  return scheduler;
}

void FlexAgent::AgentThread() {
  Channel& global_channel = global_scheduler_->GetDefaultChannel();
  gtid().assign_name("Agent:" + std::to_string(cpu().id()));
  if (verbose() > 1) {
    printf("Agent tid:=%d\n", gtid().tid());
  }
  SignalReady();
  WaitForEnclaveReady();

  PeriodicEdge debug_out(absl::Seconds(1));

  while (!Finished()) {
    StatusWord::BarrierToken agent_barrier = status_word().barrier();
    // Check if we're assigned as the Global agent.
    if (cpu().id() != global_scheduler_->GetGlobalCPUId()) {
      RunRequest* req = enclave()->GetRunRequest(cpu());

      if (verbose() > 1) {
        printf("Agent on cpu: %d Idled.\n", cpu().id());
      }
      req->LocalYield(agent_barrier, /*flags=*/0);
    } else {
      if (boosted_priority()) {
        global_scheduler_->PickNextGlobalCPU(agent_barrier);
        continue;
      }

      Message msg;
      while (!(msg = global_channel.Peek()).empty()) {
        global_scheduler_->DispatchMessage(msg);
        global_channel.Consume(msg);
      }

      // Order matters here: when a worker is PAUSED we defer the
      // preemption until GlobalSchedule() hoping to combine the
      // preemption with a remote_run.
      //
      // To restrict the visibility of this awkward state (PAUSED
      // but on_cpu) we do this immediately before GlobalSchedule().
      global_scheduler_->UpdateSchedParams();

      global_scheduler_->GlobalSchedule(status_word(), agent_barrier);

      if (verbose() && debug_out.Edge()) {
        static const int flags =
            verbose() > 1 ? Scheduler::kDumpStateEmptyRQ : 0;
        if (global_scheduler_->debug_runqueue_) {
          global_scheduler_->debug_runqueue_ = false;
          global_scheduler_->DumpState(cpu(), Scheduler::kDumpAllTasks);
        } else {
          global_scheduler_->DumpState(cpu(), flags);
        }
      }
    }
  }
}

}  //  namespace ghost
