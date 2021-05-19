/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GHOST_SCHEDULERS_EDF_EDF_SCHEDULER_H_
#define GHOST_SCHEDULERS_EDF_EDF_SCHEDULER_H_

#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/bind_front.h"
#include "lib/agent.h"
#include "lib/scheduler.h"
#include "schedulers/edf/orchestrator.h"
#include "shared/prio_table.h"

namespace ghost {

class Orchestrator;

struct EdfTask : public Task {
  enum class RunState {
    kBlocked = 0,
    kQueued = 1,
    kOnCpu = 2,
    kYielding = 3,
    kPaused = 4,
  };

  explicit EdfTask(Gtid edf_task_gtid, struct ghost_sw_info sw_info)
      : Task(edf_task_gtid, sw_info) {}
  ~EdfTask() override {}

  inline bool paused() const { return run_state == RunState::kPaused; }
  inline bool blocked() const { return run_state == RunState::kBlocked; }
  inline bool queued() const { return run_state == RunState::kQueued; }
  inline bool oncpu() const { return run_state == RunState::kOnCpu; }
  inline bool yielding() const { return run_state == RunState::kYielding; }

  void SetRuntime(absl::Duration runtime, bool update_elapsed_runtime);
  void UpdateRuntime();

  static std::string_view RunStateToString(EdfTask::RunState run_state) {
    switch (run_state) {
      case EdfTask::RunState::kBlocked:
        return "Blocked";
      case EdfTask::RunState::kQueued:
        return "Queued";
      case EdfTask::RunState::kOnCpu:
        return "OnCpu";
      case EdfTask::RunState::kYielding:
        return "Yielding";
      case EdfTask::RunState::kPaused:
        return "Paused";
        // We will get a compile error if a new member is added to the
        // `EdfTask::RunState` enum and a corresponding case is not added here.
    }
    CHECK(false);
    return "Unknown run state";
  }

  friend inline std::ostream& operator<<(std::ostream& os,
                                         EdfTask::RunState run_state) {
    os << RunStateToString(run_state);
    return os;
  }

  RunState run_state = RunState::kBlocked;
  int cpu = -1;

  // Position in runqueue.
  int rq_pos = -1;

  // Priority boosting for jumping past regular edf ordering in the runqueue.
  //
  // A task's priority is boosted on a kernel preemption or a !deferrable
  // wakeup - basically when it may be holding locks or other resources
  // that prevent other tasks from making progress.
  bool prio_boost = false;

  // Cumulative runtime in ns.
  absl::Duration runtime = absl::ZeroDuration();
  // Accrued CPU time in ns.
  absl::Duration elapsed_runtime = absl::ZeroDuration();

  // Whether the last execution was preempted or not.
  bool preempted = false;
  void CalculateSchedDeadline();

  // Comparator for min-heap runqueue.
  struct SchedDeadlineGreater {
    // Returns true if 'a' should be ordered after 'b' in the min-heap
    // and false otherwise.
    //
    // A task with boosted priority takes precedence over other usual
    // discriminating factors like QoS or sched_deadline.
    bool operator()(EdfTask* a, EdfTask* b) const {
      if (a->prio_boost != b->prio_boost) {
        return b->prio_boost;
      } else if (a->sp->GetQoS() != b->sp->GetQoS()) {
        // A task in a higher QoS class has preference
        return a->sp->GetQoS() < b->sp->GetQoS();
      } else {
        return a->sched_deadline > b->sched_deadline;
      }
    }
  };

  // Estimated runtime in ns.
  // This value is first set to the estimate in the corresponding sched item's
  // work class, but is later set to a weighted average of observed runtimes
  absl::Duration estimated_runtime = absl::ZeroDuration();
  // Absolute deadline by which the sched item must finish.
  absl::Time deadline = absl::InfiniteFuture();
  // Absolute deadline by which the sched item must be scheduled.
  absl::Time sched_deadline = absl::InfiniteFuture();

  const struct SchedParams* sp = nullptr;
  bool has_work = false;
  uint32_t wcid = std::numeric_limits<uint32_t>::max();
};

class EdfScheduler : public BasicDispatchScheduler<EdfTask> {
 public:
  explicit EdfScheduler(Enclave* enclave, CpuList cpus,
                        std::shared_ptr<TaskAllocator<EdfTask>> allocator,
                        int32_t global_cpu);
  ~EdfScheduler() final;

  void EnclaveReady() final;

  void TaskNew(EdfTask* task, const Message& msg) final;
  void TaskRunnable(EdfTask* task, const Message& msg) final;
  void TaskDead(EdfTask* task, const Message& msg) final;
  void TaskDeparted(EdfTask* task, const Message& msg) final;
  void TaskYield(EdfTask* task, const Message& msg) final;
  void TaskBlocked(EdfTask* task, const Message& msg) final;
  void TaskPreempted(EdfTask* task, const Message& msg) final;

  void CpuTick(const Message& msg) final;
  void CpuNotIdle(const Message& msg) final;

  bool Empty() { return num_tasks_ == 0; }
  void ValidatePreExitState();

  void UpdateSchedParams();

  void UpdateRunqueue(EdfTask* task);
  void RemoveFromRunqueue(EdfTask* task);
  void UpdateRunqueuePosition(uint32_t pos);
  void CheckRunQueue();

  void GlobalSchedule(const StatusWord& agent_sw,
                      StatusWord::BarrierToken agent_sw_last);

  int32_t GetGlobalCPUId() {
    return global_cpu_.load(std::memory_order_acquire);
  }
  void SetGlobalCPU(Cpu cpu) {
    global_cpu_.store(cpu.id(), std::memory_order_release);
  }
  void PickNextGlobalCPU();

  void DumpState(Cpu cpu, int flags) final;
  std::atomic<bool> debug_runqueue_ = false;

  static const int kDebugRunqueue = 1;

 private:
  bool PreemptTask(EdfTask* prev, EdfTask* next,
                   StatusWord::BarrierToken agent_barrier);
  void Yield(EdfTask* task);
  void Unyield(EdfTask* task);
  void Enqueue(EdfTask* task);
  EdfTask* Dequeue();
  EdfTask* Peek();
  void DumpAllTasks();

  void UpdateTaskRuntime(EdfTask* task, absl::Duration new_runtime,
                         bool update_elapsed_runtime);
  void SchedParamsCallback(Orchestrator& orch, const SchedParams* sp,
                           Gtid oldgtid);

  void HandleNewGtid(pid_t tgid);

  bool Available(Cpu cpu);

  struct CpuState {
    EdfTask* current = nullptr;
    EdfTask* next = nullptr;
    const Agent* agent = nullptr;
  } ABSL_CACHELINE_ALIGNED;
  CpuState* cpu_state_of(const EdfTask* task);
  inline CpuState* cpu_state(Cpu cpu) { return &cpu_states_[cpu.id()]; }
  CpuState cpu_states_[MAX_CPUS];

  std::atomic<int32_t> global_cpu_;
  int num_tasks_ = 0;
  // Heapified runqueue
  std::vector<EdfTask*> run_queue_;
  std::vector<EdfTask*> yielding_tasks_;
  absl::flat_hash_map<pid_t, std::unique_ptr<Orchestrator>> orchs_;

  const Orchestrator::SchedCallbackFunc kSchedCallbackFunc =
      absl::bind_front(&EdfScheduler::SchedParamsCallback, this);
};

std::unique_ptr<EdfScheduler> SingleThreadEdfScheduler(Enclave* enclave,
                                                       CpuList cpus,
                                                       int32_t global_cpu);

// Operates as the Global or Satellite agent depending on input from the
// global_scheduler->GetGlobalCPU callback.
class GlobalSatAgent : public Agent {
 public:
  GlobalSatAgent(Enclave* enclave, Cpu cpu, Channel* channel,
                 EdfScheduler* global_scheduler)
      : Agent(enclave, cpu),
        channel_(channel),
        global_scheduler_(global_scheduler) {}

  void AgentThread() override;

 private:
  Channel* channel_;
  EdfScheduler* global_scheduler_;
};

// Config for an global agent scheduler.  In addition to the usual AgentConfig,
// it has an global_cpu_, where the global agent prefers to run.
class GlobalConfig : public AgentConfig {
 public:
  GlobalConfig() {}
  GlobalConfig(Topology* topology, CpuList cpus, Cpu global_cpu)
      : AgentConfig(topology, cpus), global_cpu_(global_cpu) {}

  Cpu global_cpu_{Cpu::UninitializedType::kUninitialized};
};

// An global agent scheduler.  It runs a single-threaded EDF scheduler on the
// global_cpu.
template <class ENCLAVE>
class GlobalEdfAgent : public FullAgent<ENCLAVE> {
 public:
  explicit GlobalEdfAgent(GlobalConfig config)
      : FullAgent<ENCLAVE>(config), global_channel_(GHOST_MAX_QUEUE_ELEMS, 0) {
    global_scheduler_ = SingleThreadEdfScheduler(
        &this->enclave_, *this->enclave_.cpus(), config.global_cpu_.id());
    this->StartAgentTasks();
    this->enclave_.Ready();
  }

  ~GlobalEdfAgent() override {
    global_scheduler_->ValidatePreExitState();

    // Terminate global agent before satellites to avoid a false negative error
    // from ghost_run(). e.g. when the global agent tries to schedule on a CPU
    // without an active satellite agent.
    auto global_cpuid = global_scheduler_->GetGlobalCPUId();

    if (this->agents_.front()->cpu().id() != global_cpuid) {
      // Bring the current globalcpu agent to the front.
      for (auto it = this->agents_.begin(); it != this->agents_.end(); it++) {
        if (((*it)->cpu().id() == global_cpuid)) {
          auto d = std::distance(this->agents_.begin(), it);
          std::iter_swap(this->agents_.begin(), this->agents_.begin() + d);
          break;
        }
      }
    }

    CHECK_EQ(this->agents_.front()->cpu().id(), global_cpuid);

    this->TerminateAgentTasks();
  }

  std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) override {
    return absl::make_unique<GlobalSatAgent>(
        &this->enclave_, cpu, &global_channel_, global_scheduler_.get());
  }

  int64_t RpcHandler(int64_t req, const AgentRpcArgs& args) override {
    switch (req) {
      case EdfScheduler::kDebugRunqueue:
        global_scheduler_->debug_runqueue_ = true;
        return 0;
      default:
        return -1;
    }
  }

 private:
  std::unique_ptr<EdfScheduler> global_scheduler_;
  Channel global_channel_;
};

}  // namespace ghost

#endif  // GHOST_SCHEDULERS_EDF_EDF_SCHEDULER_H_
