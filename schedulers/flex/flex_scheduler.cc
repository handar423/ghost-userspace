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
    absl::Duration empty_time_slice,
    absl::Duration preemption_time_slice)
    : BasicDispatchScheduler(enclave, std::move(cpus), std::move(allocator)),
      global_cpu_(global_cpu),
      global_channel_(GHOST_MAX_QUEUE_ELEMS, /*node=*/0),
      free_empty_time_slice_(empty_time_slice - absl::Microseconds(3)),
      alloc_empty_time_slice_(empty_time_slice + absl::Microseconds(3)),
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

  // for (auto const& it : orchs_) it.second->DumpSchedParams();
}

void FlexScheduler::DumpState(const Cpu& agent_cpu, int flags) {
  static absl::Time scheduling_time_from_last = absl::Now();
  absl::Time time_temp = absl::Now();
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

  // fprintf(stderr, "SchedState: ");
  // for (const Cpu& cpu : batch_app_assigned_cpu_) {
  //   CpuState* cs = cpu_state(cpu);
  //   fprintf(stderr, "%d_0:", cpu.id());
  //   if (!cs->current) {
  //     fprintf(stderr, "none ");
  //   } else {
  //     Gtid gtid = cs->current->gtid;
  //     absl::FPrintF(stderr, "%s ", gtid.describe());
  //   }
  // }
  // for(int i = 1; i < MAX_VRAN_NUMBER; ++i){
  //   VranInfo& vran = vrans_[i];
  //   if(vran.available == false) continue;
  //   for (const Cpu& cpu : vran.cpu_assigns_) {
  //     CpuState* cs = cpu_state(cpu);
  //     fprintf(stderr, "%d_%u:", cpu.id(), i);
  //     if (!cs->current) {
  //       fprintf(stderr, "none ");
  //     } else {
  //       Gtid gtid = cs->current->gtid;
  //       absl::FPrintF(stderr, "%s ", gtid.describe());
  //     }
  //   }
  // }
  // fprintf(stderr, " rq_l_0=%ld", RunqueueSize(0));
  for(int i = 0; i < MAX_VRAN_NUMBER; ++i){
    if (vrans_[i].available)
      fprintf(stderr, "CPU_%u=%f ", i, vran_sum_number[i] * 100.0 / scheduling_time);
  }

  fprintf(stderr, "\n");
  
  // 上次开始的scheduling time
  int64_t time_spend = (time_temp - scheduling_time_from_last) / absl::Microseconds(1);
  
  fprintf(stderr, "single scheduling time in us: %.3lf\navg yield in us:%.3lf \n", 
          time_spend * 1.0 / scheduling_time, 
          (time_spend * 1.0 / yield_time));

  yield_time = 0;
  scheduling_time = 0;
  std::memset(vran_sum_number, 0, sizeof(vran_sum_number));
  scheduling_time_from_last = absl::Now();
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

  task->seqnum = msg.seqnum();
  task->run_state = FlexTask::RunState::kBlocked;  // Need this in the
                                                       // runnable case anyway.

  const Gtid gtid(payload->gtid);
  const pid_t tgid = gtid.tgid();
  HandleNewGtid(task, tgid);
  task->local_gid = tgid;

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
    } else {
      vran.max_cpu_number_ -= 1;
      // fprintf(stderr, "%d vran %d task left\n", vran_max_cpu_number_[vran_id], vran_id);
    }
  }

  orchs_.erase(task->local_gid);
  allocator()->FreeTask(task);

  num_tasks_--;
}

// 更新CPU时间
void FlexScheduler::TaskBlocked(FlexTask* task, const Message& msg) {
  // const ghost_msg_payload_task_blocked* payload =
  //     reinterpret_cast<const ghost_msg_payload_task_blocked*>(msg.payload());

  // 两者单位一致，此处不处理
  // fprintf(stderr, "vran TaskBlocked one task time\n");

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // blocking (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
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
  // const ghost_msg_payload_task_preempt* payload =
  //     reinterpret_cast<const ghost_msg_payload_task_preempt*>(msg.payload());

  task->preempted = true;
  task->prio_boost = true;

  // States other than the typical kOnCpu are possible here:
  // We could be kQueued from a TASK_NEW that was immediately preempted.
  // We could be kPaused if agent-initiated preemption raced with kernel
  // preemption (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
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
  // const ghost_msg_payload_task_yield* payload =
  //     reinterpret_cast<const ghost_msg_payload_task_yield*>(msg.payload());

  // DCHECK_LE(payload->runtime, task->status_word.runtime());
  
  // 两者单位一致，此处不处理
  // uint64_t last_ran = payload->runtime - task->last_runtime;
  // if(vran_id != 0){
  ++yield_time;
  VranInfo& vran = vrans_[get_vran_id(task)];
  absl::Time now = absl::Now();
  vran.from_last_empty_time = now - vran.last_empty_time;
  vran.last_empty_time = now;
  // vrans_[get_vran_id(task)].empty_times_from_last_schduler_ = 1;
  // }
  // task->last_runtime = payload->runtime;
  // printf("vran %d one task time %ld\n", vran_id, last_ran);

  // States other than the typical kOnCpu are possible here:
  // We could be kPaused if agent-initiated preemption raced with task
  // yielding (then kPaused and kQueued can move between each other via
  // SCHED_ITEM_RUNNABLE edges).
  if (task->oncpu()) {
    CpuState* cs = cpu_state_of(task);
    vran.idle_cpus_.Set(task->cpu);
    CHECK_EQ(cs->current, task);
    cs->current = nullptr;
    Enqueue(task);
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
    rq.pop_front();
  } else {
    std::deque<FlexTask*>& rq = run_queue_[vran_id];
    task = rq.front();
    CHECK_NE(task, nullptr);
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
      vran.last_empty_time = absl::Now();

      // 若有，分配一个新的CPU
      if(!batch_app_assigned_cpu_.Empty()) {
        Cpu front = batch_app_assigned_cpu_.Front();
        vran.cpu_assigns_.Set(front);
        batch_app_assigned_cpu_.Clear(front);
      }
    } else {
      vran.max_cpu_number_ += 1;
    }

    task->vran_id = vran_id;
    fprintf(stderr, "have find %d vran %d task\n", vran.max_cpu_number_, vran_id);

  }
  task->sp = sp;
  task->has_work = sp->HasWork();
  task->wcid = sp->GetWorkClass();

  // A kBlocked task is not affected by any changes to FlexSchedParams.
  if (task->blocked()) {
    return;
  }

  // Case#  had_work  has_work    run_state change
  //  (a)      0         0        none: never
  //  (b)      0         1        kPaused -> kQueued: init
  //  (c)      1         0        kQueued/kOnCpu -> kPaused: never
  //  (d)      1         1        none: normal
  if (unlikely(!had_work)) {
    CHECK(task->paused());
    Enqueue(task);  // case (b).
    return;  // case (a).
  }

  if (likely(had_work)) {
    CHECK(!task->paused());
  }
  return;  // case (d).
}

void FlexScheduler::UpdateSchedParams() {
  for (auto& scraper : orchs_) {
    scraper.second->RefreshSchedParams(kSchedCallbackFunc);
    RefreshSchedEmptyTimes(scraper.second);
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
  if (iteration == 0 && cs->current) {
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
  CpuList updated_cpus = MachineTopology()->EmptyCpuList();
  CpuList open_cpus = MachineTopology()->EmptyCpuList();
  const absl::Time now = absl::Now();

  // 重新分配CPU
  for (int i_outer = 0; i_outer < MAX_VRAN_NUMBER; ++i_outer) {
    int i = (scheduling_time + i_outer) & 0xF;
  // 一般来说，认为没有16个vran那么多，测试中应该只有一到两个
  // batch_app不算（所有vrans_[0]没有意义）
    VranInfo& vran = vrans_[i];
    if(likely(vran.available == false))
      continue;
    // yield_time +=vran.empty_times_from_last_schduler_;
    // 若有，分配新的CPU
    if(likely(vran.from_last_empty_time != absl::Microseconds(1000))){
      absl::Duration yield_time = now - vran.last_empty_time;
      if(yield_time > alloc_empty_time_slice_){
        int target_extra_cpus = std::min(std::min(1, std::max(int(batch_app_assigned_cpu_.Size()) - 1, 0)),
                                vran.max_cpu_number_ - int(vran.cpu_assigns_.Size()));
        for(int local_iter = 0; local_iter < target_extra_cpus; ++local_iter){
          Cpu front = batch_app_assigned_cpu_.Front();
          vran.cpu_assigns_.Set(front);
          batch_app_assigned_cpu_.Clear(front);
        }
      } else if (vran.from_last_empty_time < free_empty_time_slice_ && (!vran.idle_cpus_.Empty())){
        // 假设idle_cpus_为空，则引起yield_time的yield对应CPU已经被释放，此时等待下一次缩容机会
        // fprintf(stderr, "remove cpu %d\n", empty_time);
        Cpu free_ = vran.idle_cpus_.Front();
        batch_app_assigned_cpu_.Set(free_);
        vran.cpu_assigns_.Clear(free_);
      }
      vran.idle_cpus_.Clear();
    }
    vran_sum_number[i] += vran.cpu_assigns_.Size();

    // TODO: Refactor this loop
    for (const Cpu& cpu : vran.cpu_assigns_) {
      CpuState* cs = cpu_state(cpu);
      if (cs->current) {
        continue;
      }

      vRAN_id_t vran_id = i << VRAN_INDEX_OFFSET;
      FlexTask* to_run = Dequeue(vran_id);
      if(!to_run){
        break;
      }
      cs->next = to_run;
      updated_cpus.Set(cpu.id());
    }
  }

  // TODO: Refactor this loop
  // for batch app
  for (const Cpu& cpu : batch_app_assigned_cpu_) {
    CpuState* cs = cpu_state(cpu);
    if (cs->current) {
      continue;
    }

    FlexTask* to_run = Dequeue(0);
    if(!to_run){
      break;
    }
    cs->next = to_run;
    updated_cpus.Set(cpu.id());
  }

  for (const Cpu& cpu : updated_cpus) {
    CpuState* cs = cpu_state(cpu);

    FlexTask* next = cs->next;
    CHECK_NE(next, nullptr);

    if (unlikely(cs->current == next)) continue;

    RunRequest* req = enclave()->GetRunRequest(cpu);
    req->Open({
        .target = next->gtid,
        .target_barrier = next->seqnum,
        .commit_flags = COMMIT_AT_TXN_COMMIT,
    });

    open_cpus.Set(cpu.id());
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
      // FlexTask latched successfully; clear state from an earlier run.
      //
      // Note that 'preempted' influences a task's run_queue position
      // so we clear it only after the transaction commit is successful.
      cs->current = next;
      next->run_state = FlexTask::RunState::kOnCpu;
      next->cpu = cpu.id();
      next->preempted = false;
      next->prio_boost = false;
    } else {
      // Need to requeue in the stale case.
      Enqueue(next, /* back = */ false);
    }
  }

}

bool FlexScheduler::PickNextGlobalCPU(
    StatusWord::BarrierToken agent_barrier) {

  if(batch_app_assigned_cpu_.Empty()) return false;
      
  // TODO: Select CPUs more intelligently.
  Cpu target_cpu = batch_app_assigned_cpu_.Front();
  CpuState* cs = cpu_state(target_cpu);
  FlexTask* prev = cs->current;

  if (prev) {
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
  batch_app_assigned_cpu_.Clear(target_cpu);
  SetGlobalCPU(target_cpu);
  enclave()->GetAgent(target_cpu)->Ping();

  return true;
}

std::unique_ptr<FlexScheduler> SingleThreadFlexScheduler(
    Enclave* enclave, CpuList cpus, int32_t global_cpu,
    absl::Duration empty_time_slice,
    absl::Duration preemption_time_slice) {
  auto allocator =
      std::make_shared<SingleThreadMallocTaskAllocator<FlexTask>>();
  auto scheduler = absl::make_unique<FlexScheduler>(
      enclave, std::move(cpus), std::move(allocator), global_cpu,
      empty_time_slice, preemption_time_slice);
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
      if (boosted_priority() && global_scheduler_->PickNextGlobalCPU(agent_barrier)) {
        // 当且仅当有第三方CPU时允许修改global agent
        // 事实上，也仅仅有vRAN PHY worker进程和第三方应用可能触发这一行为
        fprintf(stderr, "PickNextGlobalCPU!\n");
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
        // static const int flags =
        //     verbose() > 1 ? Scheduler::kDumpStateEmptyRQ : 0;
        if (global_scheduler_->debug_runqueue_) {
          global_scheduler_->debug_runqueue_ = false;
          global_scheduler_->DumpState(cpu(), Scheduler::kDumpAllTasks);
        } else {
          global_scheduler_->DumpState(cpu(), Scheduler::kDumpStateEmptyRQ);
        }
      }
    }
  }
}

void FlexScheduler::RefreshSchedEmptyTimes(std::shared_ptr<ghost::FlexOrchestrator> orch){
  struct sched_item* si = orch->table_.sched_item(0);
  const struct work_class* wc = orch->table_.work_class(si->wcid);
  int vran_id = wc->id;
  if(vran_id){
    FlexScheduler::VranInfo& vran = vrans_[vran_id];
    vran.empty_times_from_last_schduler = 0;
    for (Cpu cpu: vran.cpu_assigns_){
      FlexTask* task = cpu_state(cpu)->current;
      if(task){
        int sid = task->sp->GetSID();
        // printf("sid %d si->wcid %d\n", sid, si->wcid);
        struct sched_item* si_local = orch->table_.sched_item(sid);
        int temp_history_empty_time = vran.history_empty_time[sid];
        if(likely(si_local->empty_time >= temp_history_empty_time)){
          vran.empty_times_from_last_schduler += si_local->empty_time - temp_history_empty_time;
        } else {
          vran.empty_times_from_last_schduler += si_local->empty_time;
        }
        vran.history_empty_time[sid] = si_local->empty_time;
      }
    }
    if(likely(vran.empty_times_from_last_schduler > 0)){
      printf("vran.empty_times_from_last_schduler %d\n", vran.empty_times_from_last_schduler);
      vran.init_flag = 1;
    }
  }
}

}  //  namespace ghost
