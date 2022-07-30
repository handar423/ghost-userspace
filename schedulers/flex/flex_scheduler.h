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

#ifndef GHOST_SCHEDULERS_FLEX_FLEX_SCHEDULER_H
#define GHOST_SCHEDULERS_FLEX_FLEX_SCHEDULER_H

#include <cstdint>
#include <map>
#include <cmath>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/bind_front.h"
#include "absl/time/time.h"
#include "lib/agent.h"
#include "lib/scheduler.h"
#include "schedulers/flex/flex_orchestrator.h"
#include "shared/prio_table.h"

namespace ghost {

#define VRAN_ID_MASK 0xF0
#define MAX_VRAN_NUMBER 0x10
#define VRAN_INDEX_OFFSET 4

typedef uint32_t vRAN_id_t;
typedef int32_t cpu_id_t;

// Store information about a scheduled task.
struct FlexTask : public Task {
  enum class RunState {
    kBlocked,
    kQueued,
    kOnCpu,
    kYielding,
    kPaused,
  };

  // Represents a deferred unschedule.
  enum class UnscheduleLevel {
    // No pending deferred unschedule.
    kNoUnschedule,
    // This task was assigned a new closure (noticed via an update to this
    // task's sched item in the PrioTable) and may be preempted so another
    // waiting task may run. However, if there are no waiting tasks, keep this
    // task running to avoid the unschedule-and-reschedule overhead.
    kCouldUnschedule,
    // This task was marked idle via the PrioTable and should stop running.
    kMustUnschedule,
  };

  explicit FlexTask(Gtid shinjuku_task_gtid, struct ghost_sw_info sw_info)
      : Task(shinjuku_task_gtid, sw_info) {}
  ~FlexTask() override {}

  bool paused() const { return run_state == RunState::kPaused; }
  bool blocked() const { return run_state == RunState::kBlocked; }
  bool queued() const { return run_state == RunState::kQueued; }
  bool oncpu() const { return run_state == RunState::kOnCpu; }
  bool yielding() const { return run_state == RunState::kYielding; }

  // Sets the task's runtime to 'runtime' and updates the elapsed runtime if
  // 'update_elapsed_runtime' is true.
  void SetRuntime(absl::Duration runtime, bool update_elapsed_runtime);
  // Updates the task's runtime to match what the kernel last stored in the
  // task's status word. Note that this method does not use the syscall to get
  // the task's current runtime.
  void UpdateRuntime();

  static std::string_view RunStateToString(FlexTask::RunState run_state) {
    switch (run_state) {
      case FlexTask::RunState::kBlocked:
        return "Blocked";
      case FlexTask::RunState::kQueued:
        return "Queued";
      case FlexTask::RunState::kOnCpu:
        return "OnCpu";
      case FlexTask::RunState::kYielding:
        return "Yielding";
      case FlexTask::RunState::kPaused:
        return "Paused";
        // We will get a compile error if a new member is added to the
        // `FlexTask::RunState` enum and a corresponding case is not added
        // here.
    }
    CHECK(false);
    return "Unknown run state";
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  FlexTask::RunState run_state) {
    os << RunStateToString(run_state);
    return os;
  }

  friend std::ostream& operator<<(
      std::ostream& os, FlexTask::UnscheduleLevel unschedule_level) {
    switch (unschedule_level) {
      case FlexTask::UnscheduleLevel::kNoUnschedule:
        os << "No Unschedule";
        break;
      case FlexTask::UnscheduleLevel::kCouldUnschedule:
        os << "Could Unschedule";
        break;
      case FlexTask::UnscheduleLevel::kMustUnschedule:
        os << "Must Unschedule";
        break;
        // We will get a compile error if a new member is added to the
        // `FlexTask::UnscheduleLevel` enum and a corresponding case is not
        // added here.
    }
    return os;
  }

  RunState run_state = RunState::kBlocked;
  int cpu = -1;

  // Priority boosting for jumping past regular shinjuku ordering in the
  // runqueue.
  //
  // A task's priority is boosted on a kernel preemption or a !deferrable
  // wakeup - basically when it may be holding locks or other resources
  // that prevent other tasks from making progress.
  // 对于FlexRAN而言不存在，对于batch而言优先级不超过FlexRAN
  bool prio_boost = false;

  // Cumulative runtime in ns.
  absl::Duration runtime;
  // Accrued CPU time in ns.
  absl::Duration elapsed_runtime;
  // The time that the task was last scheduled.
  absl::Time last_ran = absl::UnixEpoch();

  // Whether the last execution was preempted or not.
  // 对于FLexRAN永远为FALSE
  bool preempted = false;

  std::shared_ptr<FlexOrchestrator> orch;
  const FlexSchedParams* sp = nullptr;

  // 等价于runnable，true
  bool has_work = false;

  uint32_t wcid = std::numeric_limits<uint32_t>::max();
  
  // 为了计算单次运行时长
  uint64_t last_runtime = 0;

  vRAN_id_t vran_id = 0;

  pid_t local_gid = 0;

  // Indicates whether there is a pending deferred unschedule for this task, and
  // if so, whether the unschedule could optionally happen or must happen.
  UnscheduleLevel unschedule_level = UnscheduleLevel::kNoUnschedule;
};

// Implements the global agent policy layer and the Flex scheduling
// algorithm. Can optionally be turned into a centralized queuing algorithm by
// setting the preemption time slice to an infinitely large duration. Can
// optionally be turned into a Shenango algorithm by using QoS classes; assign a
// low-latency app a high QoS class and an antagonist (that consumes CPU cycles)
// a low QoS class.
class FlexScheduler : public BasicDispatchScheduler<FlexTask> {
 public:
  explicit FlexScheduler(
      Enclave* enclave, CpuList cpus,
      std::shared_ptr<TaskAllocator<FlexTask>> allocator,
      int32_t global_cpu, absl::Duration empty_time_slice,
      absl::Duration preemption_time_slice);
  ~FlexScheduler() final;

  void EnclaveReady() final;
  Channel& GetDefaultChannel() final { return global_channel_; };

  // Handles task messages received from the kernel via shared memory queues.
  void TaskNew(FlexTask* task, const Message& msg) final;
  void TaskRunnable(FlexTask* task, const Message& msg) final;
  void TaskDeparted(FlexTask* task, const Message& msg) final;
  void TaskDead(FlexTask* task, const Message& msg) final;
  void TaskYield(FlexTask* task, const Message& msg) final;
  void TaskBlocked(FlexTask* task, const Message& msg) final;
  void TaskPreempted(FlexTask* task, const Message& msg) final;

  void DiscoveryStart() final;
  void DiscoveryComplete() final;

  bool Empty() { return num_tasks_ == 0; }

  // Refreshes updated sched items. Note that all sched items may be refreshed,
  // regardless of whether they have been updated or not, if the stream has
  // overflown.
  void UpdateSchedParams();

  // Removes 'task' from the runqueue.
  void RemoveFromRunqueue(FlexTask* task);

  // Helper function to 'GlobalSchedule' that determines whether it should skip
  // scheduling a CPU right now (returns 'true') or if it can schedule a CPU
  // right now (returns 'false').
  bool SkipForSchedule(int iteration, const Cpu& cpu);

  // Main scheduling function for the global agent.
  void GlobalSchedule(const StatusWord& agent_sw,
                      StatusWord::BarrierToken agent_sw_last);

  int32_t GetGlobalCPUId() {
    return global_cpu_.load(std::memory_order_acquire);
  }

  void SetGlobalCPU(const Cpu& cpu) {
    global_cpu_.store(cpu.id(), std::memory_order_release);
  }

  // When a different scheduling class (e.g., CFS) has a task to run on the
  // global agent's CPU, the global agent calls this function to try to pick a
  // new CPU to move to and, if a new CPU is found, to initiate the handoff
  // process.
  bool PickNextGlobalCPU(StatusWord::BarrierToken agent_barrier);

  // Print debug details about the current tasks managed by the global agent,
  // CPU state, and runqueue stats.
  void DumpState(const Cpu& cpu, int flags) final;
  std::atomic<bool> debug_runqueue_ = false;

  static constexpr int kDebugRunqueue = 1;

 private:
  struct CpuState {
    FlexTask* current = nullptr;
    FlexTask* next = nullptr;
    const Agent* agent = nullptr;
  } ABSL_CACHELINE_ALIGNED;

  class VranInfo {
  public:
    VranInfo():
      cpu_assigns_(MachineTopology()->EmptyCpuList()),
      idle_cpus_(MachineTopology()->EmptyCpuList()){}
    bool available = false;

    // vRAN上一次缩容时的对应的CPU（扩容时优先考虑最新退出的CPU）
    // uint32_t last_assign_cpus_ = 0;

    // 上一次CPU为空的时间
    absl::Time last_empty_time = absl::Now();

    absl::Duration from_last_empty_time = absl::Microseconds(1000);

    // 每一类vRAN上一轮为空次数
    // uint32_t empty_times_from_last_schduler_ = 0;

    // 每一类vRAN分配CPU上限
    int max_cpu_number_ = 0;

    // uint32_t busy_times = 0;

    CpuList cpu_assigns_;

    CpuList idle_cpus_;
  };

  // Stop 'task' from running and schedule nothing in its place. 'task' must be
  // currently running on a CPU.
  void UnscheduleTask(FlexTask* task);

  // Marks a task as yielded.
  void Yield(FlexTask* task);

  // Unmarks a task as yielded.
  void Unyield(FlexTask* task);

  // Adds a task to the FIFO runqueue. By default, the task is added to the back
  // of the FIFO ('back' == true), but will be added to the front of the FIFO if
  // 'back' == false or if 'task->prio_boost' == true. When 'task->prio_boost'
  // == true, the task was unexpectedly preempted (e.g., by CFS) and could be
  // holding a critical lock, so we want to schedule it again as soon as
  // possible so it can release the lock. This could improve performance.
  void Enqueue(FlexTask* task, bool back = true);

  // Removes and returns the task at the front of the runqueue.
  FlexTask* Dequeue(vRAN_id_t vran_id);

  // Returns (but does not remove) the task at the front of the runqueue.
  FlexTask* Peek(vRAN_id_t vran_id);

  // Prints all tasks (includin tasks not running or on the runqueue) managed by
  // the global agent.
  void DumpAllTasks();

  // Updates the task's runtime and performs some consistency checks.
  void UpdateTaskRuntime(FlexTask* task, absl::Duration new_runtime,
                         bool update_elapsed_runtime);

  // Callback when a sched item is updated.
  void SchedParamsCallback(FlexOrchestrator& orch,
                           const FlexSchedParams* sp, Gtid oldgtid);

  // Handles a new process that has at least one of its threads enter the ghOSt
  // scheduling class (e.g., via sched_setscheduler()).
  void HandleNewGtid(FlexTask* task, pid_t tgid);

  // Returns 'true' if a CPU can be scheduled by ghOSt. Returns 'false'
  // otherwise, usually because a higher-priority scheduling class (e.g., CFS)
  // is currently using the CPU.
  bool Available(const Cpu& cpu);

  CpuState* cpu_state_of(const FlexTask* task);

  CpuState* cpu_state(const Cpu& cpu) { return &cpu_states_[cpu.id()]; }

  size_t RunqueueSize(vRAN_id_t vran_id);

  bool RunqueueEmpty(vRAN_id_t vran_id) { return RunqueueSize(vran_id) == 0; }

  // Returns the highest-QoS runqueue that has at least one task enqueued.
  // Must call this on a non-empty runqueue.
  uint32_t FirstFilledRunqueue() const {
    for (auto it = run_queue_.rbegin(); it != run_queue_.rend(); it++) {
      if ((it->first & VRAN_ID_MASK) == 0 && !it->second.empty()) {
        return it->first;
      }
    }
    // A precondition for this method is that the runqueue must be non-empty, so
    // if we get down here, that precondition was not upheld.
    CHECK(false);
    // Return something so that the compiler doesn't complain, but we will never
    // execute this return statement due to the 'CHECK(false)' above.
    return std::numeric_limits<uint32_t>::max();
  }

  CpuState cpu_states_[MAX_CPUS];

  std::atomic<int32_t> global_cpu_;
  Channel global_channel_;
  int num_tasks_ = 0;
  bool in_discovery_ = false;

  // Map from QoS level to runqueue
  // We use an 'std::map' rather than 'absl::flat_hash_map' because we need to
  // iterate on the map in order of QoS level.
  // 这里的Qos实际是VRAN和第三方应用的ID,我们给第三方余4个bit（必须大于0），从第五个bit开始是VRAN的ID(必须大于0)
  std::map<uint32_t, std::deque<FlexTask*>> run_queue_;
  // 这里应该只有batch的
  std::vector<FlexTask*> paused_repeatables_;

  std::vector<FlexTask*> yielding_tasks_;
  // 如果是pid作为key,for ali，可以出现多个pid
  // 现在先按支持算
  absl::flat_hash_map<pid_t, std::shared_ptr<FlexOrchestrator>> orchs_;
  const FlexOrchestrator::SchedCallbackFunc kSchedCallbackFunc =
      absl::bind_front(&FlexScheduler::SchedParamsCallback, this);
  const absl::Duration free_empty_time_slice_;
  const absl::Duration alloc_empty_time_slice_;
  const absl::Duration preemption_time_slice_;

  //CPU和FlexRAN/Batch的对应关系是按数位计的，存在4个空bit，用的时候把空位去除
  
  std::vector<VranInfo> vrans_;

  CpuList batch_app_assigned_cpu_;

  // debug用
  uint32_t scheduling_time = 0;

  uint32_t yield_time = 0;

  uint32_t vran_cpu_number = 0;

  uint32_t vran_sum_number[16] = {0};
};

// Initializes the task allocator and the Flex scheduler.
std::unique_ptr<FlexScheduler> SingleThreadFlexScheduler(
    Enclave* enclave, CpuList cpus, int32_t global_cpu,
    absl::Duration empty_time_slice,
    absl::Duration preemption_time_slice);

// Operates as the Global or Satellite agent depending on input from the
// global_scheduler->GetGlobalCPU callback.
class FlexAgent : public Agent {
 public:
  FlexAgent(Enclave* enclave, Cpu cpu, FlexScheduler* global_scheduler)
      : Agent(enclave, cpu), global_scheduler_(global_scheduler) {}

  void AgentThread() override;
  Scheduler* AgentScheduler() const override { return global_scheduler_; }

 private:
  FlexScheduler* global_scheduler_;
};

class FlexConfig : public AgentConfig {
 public:
  FlexConfig() {}
  FlexConfig(Topology* topology, CpuList cpus, Cpu global_cpu)
      : AgentConfig(topology, cpus), global_cpu_(global_cpu) {}

  Cpu global_cpu_{Cpu::UninitializedType::kUninitialized};

  absl::Duration empty_time_slice_;
  absl::Duration preemption_time_slice_;
};

// An global agent scheduler.  It runs a single-threaded Flex scheduler on
// the global_cpu.
template <class ENCLAVE>
class FullFlexAgent : public FullAgent<ENCLAVE> {
 public:
  explicit FullFlexAgent(FlexConfig config)
      : FullAgent<ENCLAVE>(config) {
    global_scheduler_ = SingleThreadFlexScheduler(
        &this->enclave_, *this->enclave_.cpus(), config.global_cpu_.id(),
        config.empty_time_slice_, config.preemption_time_slice_);
    this->StartAgentTasks();
    this->enclave_.Ready();
  }

  ~FullFlexAgent() override {
    // Terminate global agent before satellites to avoid a false negative error
    // from ghost_run(). e.g. when the global agent tries to schedule on a CPU
    // without an active satellite agent.
    int global_cpuid = global_scheduler_->GetGlobalCPUId();

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
    return absl::make_unique<FlexAgent>(&this->enclave_, cpu,
                                            global_scheduler_.get());
  }

  // for ali，加上调度函数
  // 暂时不需要
  void RpcHandler(int64_t req, const AgentRpcArgs& args,
                  AgentRpcResponse& response) override {
    switch (req) {
      case FlexScheduler::kDebugRunqueue:
        global_scheduler_->debug_runqueue_ = true;
        response.response_code = 0;
        return;
      default:
        response.response_code = -1;
        return;
    }
  }

 private:
  std::unique_ptr<FlexScheduler> global_scheduler_;
};

}  // namespace ghost

#endif  // GHOST_SCHEDULERS_FLEX_FLEX_SCHEDULER_H
