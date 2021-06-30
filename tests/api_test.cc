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

#include <sys/mman.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/random.h"
#include "lib/agent.h"
#include "lib/scheduler.h"
#include "schedulers/fifo/fifo_scheduler.h"

namespace ghost {
namespace {

using ::testing::AnyOf;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::IsFalse;
using ::testing::IsNull;
using ::testing::IsTrue;
using ::testing::Lt;
using ::testing::Ne;
using ::testing::NotNull;

// DeadAgent tries to induce a race in stage_commit() such that task_rq_lock
// returns with a task that is already dead. This validates that the kernel
// properly detects this condition and bails out.
//
// Prior to the fix in go/kcl/353858 the kernel would reliably panic in less
// than 1000 iterations of this test in virtme.
class DeadAgent : public Agent {
 public:
  DeadAgent(Enclave* enclave, Cpu this_cpu, Cpu other_cpu, Channel* channel)
      : Agent(enclave, this_cpu), other_cpu_(other_cpu), channel_(channel) {}

 protected:
  void AgentThread() final {
    // Boilerplate to synchronize startup until both agents are ready.
    SignalReady();
    WaitForEnclaveReady();

    // Satellite agent that simply yields.
    if (!channel_) {
      RunRequest* req = enclave()->GetRunRequest(cpu());
      while (!Finished()) {
        req->LocalYield(status_word().barrier(), 0);
      }
      return;
    }

    // Spinning agent that continuously schedules `task` on `other_cpu_`.
    //
    // The agent is indiscriminate and doesn't track whether a task
    // is blocked or runnable (intentional so we can try to catch
    // the task when it is exiting).
    ASSERT_THAT(channel_, NotNull());

    bool done = false;  // set to true below when MSG_TASK_DEAD is received.
    std::unique_ptr<Task> task(nullptr);  // initialized in TASK_NEW handler.
    while (true) {
      while (true) {
        Message msg = Peek(channel_);
        if (msg.empty()) break;

        // Ignore all types other than task messages (e.g. CPU_TICK).
        if (msg.is_task_msg() && msg.type() != MSG_TASK_NEW) {
          ASSERT_THAT(task, NotNull());
          task->Advance(msg.seqnum());
        }

        switch (msg.type()) {
          case MSG_TASK_NEW: {
            const ghost_msg_payload_task_new* payload =
                static_cast<const ghost_msg_payload_task_new*>(msg.payload());
            ASSERT_THAT(task, IsNull());
            task =
                absl::make_unique<Task>(Gtid(payload->gtid), payload->sw_info);
            task->seqnum = msg.seqnum();
            break;
          }

          case MSG_TASK_DEAD:
            ASSERT_THAT(task, NotNull());
            ASSERT_THAT(done, IsFalse());
            done = true;
            break;

          default:
            break;
        }
        Consume(channel_, msg);
      }

      StatusWord::BarrierToken agent_barrier = status_word().barrier();
      const bool prio_boost = status_word().boosted_priority();

      if (Finished() && done) {
        break;
      }

      if (prio_boost || !task) {
        // Yield if a higher priority sched_class wants to run or
        // we don't have a task to schedule.
        RunRequest* req = enclave()->GetRunRequest(cpu());
        req->LocalYield(agent_barrier, prio_boost ? RTLA_ON_IDLE : 0);
      } else {
        // Schedule `task` on `other_cpu_`.
        RunRequest* req = enclave()->GetRunRequest(other_cpu_);
        req->Open({.target = task->gtid,
                   .target_barrier = task->seqnum,
                   .agent_barrier = agent_barrier,
                   .commit_flags = COMMIT_AT_TXN_COMMIT});

        // We expect the Submit() to fail most of the time since we don't
        // track whether `task` can be legitimately scheduled but it does
        // maximize opportunities to trigger the race in go/kcl/353858.
        //
        // N.B. We use Submit() rather than Commit() because the latter
        // calls CompleteRunRequest() that CHECK-fails for unexpected
        // errors (for e.g. commit can fail with GHOST_TXN_TARGET_ONCPU
        // which is not expected with a well-behaved agent).
        req->Submit();
        while (!req->committed()) {
          asm volatile("pause");
        }
      }
    }
  }

 private:
  Cpu other_cpu_;
  Channel* channel_;
};

template <class ENCLAVE = LocalEnclave>
class FullDeadAgent final : public FullAgent<ENCLAVE> {
 public:
  explicit FullDeadAgent(const AgentConfig& config)
      : FullAgent<ENCLAVE>(config),
        sched_cpu_(config.cpus_.Front()),
        satellite_cpu_(config.cpus_.Back()),
        channel_(GHOST_MAX_QUEUE_ELEMS, sched_cpu_.numa_node(),
                 MachineTopology()->ToCpuList({sched_cpu_})) {
    channel_.SetEnclaveDefault();
    // Start an instance of DeadAgent on each cpu.
    this->StartAgentTasks();

    // Unblock all agents and start scheduling.
    this->enclave_.Ready();
  }

  ~FullDeadAgent() final { this->TerminateAgentTasks(); }

  std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) final {
    Cpu other_cpu = satellite_cpu_;
    Channel* channel_ptr = &channel_;
    if (cpu == satellite_cpu_) {
      other_cpu = sched_cpu_;
      channel_ptr = nullptr;  // sentinel value to indicate a satellite cpu.
    }
    return absl::make_unique<DeadAgent>(&this->enclave_, cpu, other_cpu,
                                        channel_ptr);
  }

  void RpcHandler(int64_t req, const AgentRpcArgs& args,
                  AgentRpcResponse<>& response) final {
    response.response_code = -1;
  }

 private:
  Cpu sched_cpu_;      // CPU running the main scheduling loop.
  Cpu satellite_cpu_;  // Satellite agent CPU.
  Channel channel_;    // Channel configured to wakeup `sched_cpu_`.
};

TEST(ApiTest, RunDeadTask) {
  // Skip test if this is a uni-processor system.
  Topology* topology = MachineTopology();
  CpuList all_cpus = topology->all_cpus();
  if (all_cpus.Size() < 2) {
    GTEST_SKIP() << "must be a multiprocessor system";
    return;
  }

  // Pick two CPUs randomly to run the test. An agent spinning on the first
  // CPU continuously schedules the `GhostThread` below on the second CPU.
  std::vector<Cpu> target_cpu_vector;
  std::vector<Cpu> all_cpus_vector = all_cpus.ToVector();
  std::sample(all_cpus_vector.begin(), all_cpus_vector.end(),
              std::back_inserter(target_cpu_vector), 2, absl::BitGen());
  CpuList target_cpus = MachineTopology()->ToCpuList(target_cpu_vector);

  auto ap = AgentProcess<FullDeadAgent<>, AgentConfig>(
      AgentConfig(topology, target_cpus));

  GhostThread t(GhostThread::KernelScheduler::kGhost, [] {
    // Nothing: exit as soon as possible.
  });
  t.Join();

  // When AgentProcess goes out of scope its destructor will trigger
  // the FullDeadAgent destructor that in turn will Terminate() the
  // agents.

  // Since we were a ghOSt client, we were using an enclave.  Now that the test
  // is over, we need to reset so we can get a fresh enclave later.  Note that
  // we used AgentProcess, so the only user of the gbl_enclave_fd_ is us, the
  // client.
  Ghost::CloseGlobalEnclaveCtlFd();
}

class SyncGroupScheduler final : public BasicDispatchScheduler<FifoTask> {
 public:
  explicit SyncGroupScheduler(
      Enclave* enclave, const CpuList& cpulist,
      std::shared_ptr<TaskAllocator<FifoTask>> allocator)
      : BasicDispatchScheduler(enclave, cpulist, std::move(allocator)),
        sched_cpu_(cpulist.Front()),
        channel_(absl::make_unique<Channel>(
            GHOST_MAX_QUEUE_ELEMS,
            /*node=*/0, MachineTopology()->ToCpuList({sched_cpu_}))) {}

  Channel& GetDefaultChannel() final { return *channel_; };

  bool Empty(const Cpu& cpu) const {
    // Non-scheduling CPUs only look at what's running on the local cpu.
    if (cpu != sched_cpu_) {
      const CpuState* cs = cpu_state(cpu);
      return !cs->current;
    }

    // The scheduling CPU must look at the runqueue as well as what's running
    // on all cpus.
    for (const Cpu& cpu : *enclave()->cpus()) {
      const CpuState* cs = cpu_state(cpu);
      if (cs->current) {
        return false;
      }
    }

    return rq_.Empty();
  }

  void Schedule(const Cpu& this_cpu, StatusWord::BarrierToken agent_barrier,
                bool prio_boost, bool finished) {
    RunRequest* req = enclave()->GetRunRequest(this_cpu);

    if (this_cpu != sched_cpu_) {
      req->LocalYield(agent_barrier, /*flags=*/0);
      return;
    }

    // Dequeue any pending messages.
    Message msg;
    while (!(msg = Peek(channel_.get())).empty()) {
      DispatchMessage(msg);
      Consume(channel_.get(), msg);
    }

    // A non-ghost sched_class is runnable so give up the CPU until it is
    // about to idle.
    if (prio_boost) {
      req->LocalYield(agent_barrier, RTLA_ON_IDLE);
      return;
    }

    if (finished && Empty(this_cpu)) return;

    // Populate 'cs->next' for each cpu in the enclave.
    for (const Cpu& cpu : cpus()) {
      CpuState* cs = cpu_state(cpu);
      ASSERT_THAT(cs->next, IsNull());

      if (cs->current) {
        cs->next = cs->current;  // exercise ALLOW_TASK_ONCPU.
      } else {
        cs->next = rq_.Dequeue();
      }

      const int sync_group_owner = this_cpu.id();
      Gtid target = Gtid(GHOST_IDLE_GTID);
      StatusWord::BarrierToken target_barrier = StatusWord::NullBarrierToken();
      RunRequest* req = enclave()->GetRunRequest(cpu);
      if (cs->next) {
        target = cs->next->gtid;
        target_barrier = cs->next->seqnum;
      }

      req->Open({
          .target = target,
          .target_barrier = target_barrier,
          .agent_barrier = agent_barrier,
          .commit_flags = COMMIT_AT_TXN_COMMIT,
          .run_flags = ALLOW_TASK_ONCPU,
          .sync_group_owner = sync_group_owner,
          .allow_txn_target_on_cpu = true,
      });
      ASSERT_THAT(req->sync_group_owned(), IsTrue());
      ASSERT_THAT(req->sync_group_owner_get(), Eq(sync_group_owner));
    }

    // sync-group commit.
    //
    // N.B. CommitSyncRequests() releases ownership of transactions in
    // the sync_group but that's not an issue for this test since only
    // the agent on 'sched_cpu_' is doing the sync_group commits.
    bool successful = enclave()->CommitSyncRequests(cpus());

    for (const Cpu& cpu : cpus()) {
      CpuState* cs = cpu_state(cpu);

      // Verify all-or-nothing semantics: all commits must have the same
      // disposition (successful or failed).
      const RunRequest* req = enclave()->GetRunRequest(cpu);
      ASSERT_THAT(req->sync_group_owned(), IsFalse());
      ASSERT_THAT(req->committed(), IsTrue());
      ASSERT_THAT(req->succeeded(), Eq(successful));
      if (cs->next != cs->current) {
        ASSERT_THAT(cs->next, NotNull());
        ASSERT_THAT(cs->current, IsNull());
        if (successful) {
          TaskOnCpu(cs->next, cpu);  // task is oncpu.
        } else {
          rq_.Enqueue(cs->next);  // put task back to the runqueue.

          // This is not intuitive but even though the overall sync_group
          // failed to commit it is possible for 'cs->next' to get oncpu
          // briefly (only to reschedule itself promptly due to a poisoned
          // rendezvous). However this reschedule could go down the preempt
          // path (e.g. ghost->cfs) and generate TASK_PREEMPTED which would
          // run afoul of the CHECK(!task->preempted) in DispatchMessage().
          cs->next->preempted = false;
        }
      } else if (cs->next) {
        // Attempted an idempotent commit on `cpu` (i.e. next == current).
        ASSERT_THAT(cs->next->run_state, Eq(FifoTaskState::kOnCpu));
        if (!successful) {
          TaskOffCpu(cs->next, /*blocked=*/false);
          cs->next->prio_boost = true;
          rq_.Enqueue(cs->next);
        }
      }

      cs->next = nullptr;  // reset for next scheduling round.
    }
  }

 protected:
  // Task state change callbacks.
  void TaskNew(FifoTask* task, const Message& msg) final {
    const ghost_msg_payload_task_new* payload =
        static_cast<const ghost_msg_payload_task_new*>(msg.payload());

    task->seqnum = msg.seqnum();
    task->run_state = FifoTaskState::kBlocked;
    if (payload->runnable) {
      task->run_state = FifoTaskState::kRunnable;
      task->cpu = sched_cpu_.id();
      rq_.Enqueue(task);
    }
  }

  void TaskRunnable(FifoTask* task, const Message& msg) final {
    const ghost_msg_payload_task_wakeup* payload =
        static_cast<const ghost_msg_payload_task_wakeup*>(msg.payload());

    EXPECT_NE(task->seqnum, msg.seqnum());

    ASSERT_THAT(task->run_state, Eq(FifoTaskState::kBlocked));
    task->run_state = FifoTaskState::kRunnable;
    task->prio_boost = !payload->deferrable;
    rq_.Enqueue(task);
  }

  void TaskYield(FifoTask* task, const Message& msg) final {
    EXPECT_NE(task->seqnum, msg.seqnum());
    TaskOffCpu(task, /*blocked=*/false);
    rq_.Enqueue(task);
  }

  void TaskBlocked(FifoTask* task, const Message& msg) final {
    EXPECT_NE(task->seqnum, msg.seqnum());
    TaskOffCpu(task, /*blocked=*/true);
  }

  void TaskPreempted(FifoTask* task, const Message& msg) final {
    EXPECT_NE(task->seqnum, msg.seqnum());
    TaskOffCpu(task, /*blocked=*/false);
    task->preempted = true;
    task->prio_boost = true;
    rq_.Enqueue(task);
  }

  void TaskDead(FifoTask* task, const Message& msg) final {
    ASSERT_THAT(task->run_state, Eq(FifoTaskState::kBlocked));
    allocator()->FreeTask(task);
  }

  void TaskDeparted(FifoTask* task, const Message& msg) final {
    allocator()->FreeTask(task);
  }

 private:
  struct CpuState final {
    FifoTask* current = nullptr;
    FifoTask* next = nullptr;
  } ABSL_CACHELINE_ALIGNED;

  CpuState* cpu_state(const Cpu& cpu) {
    CHECK_GE(cpu.id(), 0);
    CHECK_LT(cpu.id(), cpu_states_.size());
    return &cpu_states_[cpu.id()];
  }

  const CpuState* cpu_state(const Cpu& cpu) const {
    CHECK_GE(cpu.id(), 0);
    CHECK_LT(cpu.id(), cpu_states_.size());
    return &cpu_states_[cpu.id()];
  }

  CpuState* cpu_state_of(const FifoTask* task) {
    CHECK_GE(task->cpu, 0);
    CHECK_LT(task->cpu, cpu_states_.size());
    return &cpu_states_[task->cpu];
  }

  void TaskOffCpu(FifoTask* task, bool blocked) {
    CpuState* cs = cpu_state_of(task);
    if (task->oncpu()) {
      ASSERT_THAT(cs->current, Eq(task));
      cs->current = nullptr;
    } else {
      ASSERT_THAT(task->run_state, Eq(FifoTaskState::kQueued));
      ASSERT_THAT(rq_.Erase(task), IsTrue());
    }

    task->run_state =
        blocked ? FifoTaskState::kBlocked : FifoTaskState::kRunnable;
  }

  void TaskOnCpu(FifoTask* task, const Cpu& cpu) {
    ASSERT_THAT(task->run_state, Ne(FifoTaskState::kOnCpu));
    CpuState* cs = cpu_state(cpu);
    cs->current = task;

    task->run_state = FifoTaskState::kOnCpu;
    task->cpu = cpu.id();
    task->preempted = task->prio_boost = false;
  }

  FifoRq rq_;
  Cpu sched_cpu_;  // CPU making the scheduling decisions.
  std::array<CpuState, MAX_CPUS> cpu_states_;
  std::unique_ptr<Channel> channel_;
};

template <typename T>
class TestAgent : public Agent {
 public:
  TestAgent(Enclave* enclave, Cpu cpu, T* scheduler)
      : Agent(enclave, cpu), scheduler_(scheduler) {}

 protected:
  void AgentThread() override {
    // Boilerplate to synchronize startup until all agents are ready.
    SignalReady();
    WaitForEnclaveReady();

    while (true) {
      // Order is important: agent_barrier must be evaluated before Finished().
      // (see cl/339780042 for details).
      StatusWord::BarrierToken agent_barrier = status_word().barrier();
      const bool prio_boost = status_word().boosted_priority();
      const bool finished = Finished();

      if (finished && scheduler_->Empty(cpu())) break;

      scheduler_->Schedule(cpu(), agent_barrier, prio_boost, finished);
    }
  }

 private:
  T* scheduler_;
};

template <class ENCLAVE>
class SyncGroupAgent final : public FullAgent<ENCLAVE> {
 public:
  explicit SyncGroupAgent(AgentConfig config) : FullAgent<ENCLAVE>(config) {
    auto allocator =
        std::make_shared<ThreadSafeMallocTaskAllocator<FifoTask>>();
    scheduler_ = absl::make_unique<SyncGroupScheduler>(
        &this->enclave_, config.cpus_, std::move(allocator));
    scheduler_->GetDefaultChannel().SetEnclaveDefault();
    this->StartAgentTasks();
    this->enclave_.Ready();
  }

  ~SyncGroupAgent() final { this->TerminateAgentTasks(); }

  std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) final {
    return absl::make_unique<TestAgent<SyncGroupScheduler>>(
        &this->enclave_, cpu, scheduler_.get());
  }

  void RpcHandler(int64_t req, const AgentRpcArgs& args,
                  AgentRpcResponse<>& response) final {
    response.response_code = -1;
  }

 private:
  std::unique_ptr<SyncGroupScheduler> scheduler_;
};

void SpinFor(absl::Duration d) {
  while (d > absl::ZeroDuration()) {
    absl::Time a = absl::Now();
    absl::Time b;

    // Try to minimize the contribution of arithmetic/Now() overhead.
    for (int i = 0; i < 150; i++) b = absl::Now();

    absl::Duration t = b - a;

    // Don't count preempted time.
    if (t < absl::Microseconds(100)) d -= t;
  }
}

// std:tuple<int,int> contains the test parameters:
// - first field of tuple is number of cpus.
// - second field of tuple is number of threads.
class SyncGroupTest : public testing::TestWithParam<std::tuple<int, int>> {};

TEST_P(SyncGroupTest, Commit) {
  const auto [num_cpus, num_threads] = GetParam();  // test parameters.

  if (MachineTopology()->num_cpus() < num_cpus) {
    GTEST_SKIP() << "must have at least " << num_cpus << " cpus";
    return;
  }

  // Create a 'cpulist' containing 'num_cpus' CPUs.
  const int first_cpu = 0;
  Topology* topology = MachineTopology();
  std::vector<int> cpuvec(num_cpus);
  std::iota(cpuvec.begin(), cpuvec.end(), first_cpu);
  CpuList cpulist = topology->ToCpuList(std::move(cpuvec));

  auto ap = AgentProcess<SyncGroupAgent<LocalEnclave>, AgentConfig>(
      AgentConfig(topology, cpulist));

  // Create application threads.
  std::vector<std::unique_ptr<GhostThread>> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(
        new GhostThread(GhostThread::KernelScheduler::kGhost, [] {
          SpinFor(absl::Milliseconds(100));
          for (int j = 0; j < 100; j++) absl::SleepFor(absl::Microseconds(100));
          SpinFor(absl::Milliseconds(1));
          sched_yield();
        }));
  }

  // Wait for all threads to finish.
  for (auto& t : threads) t->Join();

  Ghost::CloseGlobalEnclaveCtlFd();
}

INSTANTIATE_TEST_SUITE_P(
    SyncGroupTestConfig, SyncGroupTest,
    testing::Combine(testing::Values(1, 2, 4, 8),   // num_cpus
                     testing::Values(1, 4, 8, 16))  // num_threads
);

class IdlingAgent : public Agent {
 public:
  IdlingAgent(Enclave* enclave, Cpu cpu, bool schedule)
      : Agent(enclave, cpu), schedule_(schedule) {}

 protected:
  void AgentThread() final {
    // Boilerplate to synchronize startup until all agents are ready.
    SignalReady();
    WaitForEnclaveReady();

    // remote_cpus is all cpus in the enclave except this one.
    CpuList remote_cpus = *enclave()->cpus();
    remote_cpus.Clear(cpu());

    while (!Finished()) {
      StatusWord::BarrierToken agent_barrier = status_word().barrier();

      // Non-scheduling agents just yield until they are terminated.
      if (!schedule_) {
        RunRequest* req = enclave()->GetRunRequest(cpu());
        req->LocalYield(agent_barrier, /*flags=*/0);
        continue;
      }

      for (const Cpu& remote_cpu : remote_cpus) {
        ASSERT_THAT(remote_cpu, Ne(cpu()));

        int run_flags = ALLOW_TASK_ONCPU;

        // NEED_CPU_NOT_IDLE enabled on odd numbered cpus.
        if (remote_cpu.id() % 2) {
          run_flags |= NEED_CPU_NOT_IDLE;
        }

        RunRequest* req = enclave()->GetRunRequest(remote_cpu);
        req->Open({
            .target = Gtid(GHOST_IDLE_GTID),
            .agent_barrier = agent_barrier,
            .commit_flags = COMMIT_AT_TXN_COMMIT,
            .run_flags = run_flags,
        });
      }
      enclave()->CommitRunRequests(remote_cpus);
    }
  }

 private:
  bool schedule_;
};

constexpr int kNeedCpuNotIdle = 1;

template <class ENCLAVE>
class FullIdlingAgent final : public FullAgent<ENCLAVE> {
 public:
  explicit FullIdlingAgent(const AgentConfig& config)
      : FullAgent<ENCLAVE>(config), channel_(GHOST_MAX_QUEUE_ELEMS, 0) {
    channel_.SetEnclaveDefault();
    // Start an instance of IdlingAgent on each cpu.
    this->StartAgentTasks();

    // Unblock all agents and start scheduling.
    this->enclave_.Ready();
  }

  ~FullIdlingAgent() final { this->TerminateAgentTasks(); }

  std::unique_ptr<Agent> MakeAgent(const Cpu& cpu) final {
    const bool schedule = (cpu.id() == 0);  // schedule on the first cpu.
    return absl::make_unique<IdlingAgent>(&this->enclave_, cpu, schedule);
  }

  void RpcHandler(int64_t req, const AgentRpcArgs& args,
                  AgentRpcResponse<>& response) final {
    if (req == kNeedCpuNotIdle) {
      const int num_cpus = this->enclave_.cpus()->Size();
      std::vector<int> not_idle_msg_count(num_cpus);

      Message msg;
      while (!(msg = Peek(&channel_)).empty()) {
        if (msg.type() == MSG_CPU_NOT_IDLE) {
          // Count the number of CPU_NOT_IDLE messages seen on each cpu.
          const ghost_msg_payload_cpu_not_idle* payload =
              static_cast<const ghost_msg_payload_cpu_not_idle*>(msg.payload());
          EXPECT_THAT(payload->cpu, Ge(0));
          EXPECT_THAT(payload->cpu, Lt(num_cpus));
          ++not_idle_msg_count[payload->cpu];
        }
        Consume(&channel_, msg);
      }

      int num_failures = 0;
      for (int cpu = 0; cpu < num_cpus; ++cpu) {
        int msg_count = not_idle_msg_count[cpu];
        if (cpu % 2) {
          // Odd numbered CPUs set NEED_CPU_NOT_IDLE in `run_flags` so
          // we expect at least one CPU_NOT_IDLE message on these CPUs.
          EXPECT_THAT(msg_count, Gt(0));
          if (msg_count <= 0) {
            ++num_failures;
          }
        } else {
          // Even numbered CPUs don't set NEED_CPU_NOT_IDLE in `run_flags`
          // so we don't expect any CPU_NOT_IDLE messages on these CPUs.
          EXPECT_THAT(msg_count, Eq(0));
          if (msg_count != 0) {
            ++num_failures;
          }
        }
      }
      response.response_code = num_failures;
      return;
    } else {
      response.response_code = -1;
      return;
    }
  }

 private:
  Channel channel_;
};

TEST(IdleTest, NeedCpuNotIdle) {
  // cpu0   Spinning agent scheduling on cpus 1 and 2.
  // cpu1   Run GHOST_IDLE_GTID with NEED_CPU_NOT_IDLE.
  // cpu2   Run GHOST_IDLE_GTID.
  constexpr int kNumCpus = 3;

  Topology* topology = MachineTopology();
  CpuList all_cpus = topology->all_cpus();
  if (all_cpus.Size() < kNumCpus) {
    GTEST_SKIP();
    return;
  }

  // Create a `cpulist` containing `kNumCpus`.
  const int first_cpu = 0;
  std::vector<int> cpuvec(kNumCpus);
  std::iota(cpuvec.begin(), cpuvec.end(), first_cpu);
  CpuList cpulist = topology->ToCpuList(std::move(cpuvec));

  auto ap = AgentProcess<FullIdlingAgent<LocalEnclave>, AgentConfig>(
      AgentConfig(topology, cpulist));

  // Create application threads.
  std::vector<std::unique_ptr<GhostThread>> threads;
  for (int cpu = first_cpu + 1; cpu < kNumCpus; cpu++) {
    // Start CFS task on each non-scheduling CPU to induce IDLE->CFS
    // scheduling edges thereby triggering CPU_NOT_IDLE messages if
    // the agent sets NEED_CPU_NOT_IDLE in `run_flags`.
    threads.emplace_back(
        new GhostThread(GhostThread::KernelScheduler::kCfs, [cpu] {
          EXPECT_THAT(SchedSetAffinity(/*pid=*/0, cpu), Eq(0));
          SpinFor(absl::Milliseconds(1));
          for (int i = 0; i < 100; i++) absl::SleepFor(absl::Microseconds(10));
          SpinFor(absl::Milliseconds(1));
          sched_yield();
        }));
  }

  // Wait for all threads to finish.
  for (auto& t : threads) t->Join();

  // Verify that the kernel produces MSG_CPU_NOT_IDLE depending on whether
  // NEED_CPU_NOT_IDLE is set in `run_flags`.
  EXPECT_THAT(ap.Rpc(kNeedCpuNotIdle), 0);

  Ghost::CloseGlobalEnclaveCtlFd();
}

struct CoreSchedTask : public Task {
  explicit CoreSchedTask(Gtid task_gtid, struct ghost_sw_info sw_info)
      : Task(task_gtid, sw_info) {}
  ~CoreSchedTask() override {}

  enum class RunState {
    kBlocked,
    kRunnable,
    kOnCpu,
  };

  bool blocked() const { return run_state == RunState::kBlocked; }
  bool runnable() const { return run_state == RunState::kRunnable; }
  bool oncpu() const { return run_state == RunState::kOnCpu; }

  enum RunState run_state = RunState::kBlocked;
  int sibling = -1;
};

class CoreScheduler {
 public:
  explicit CoreScheduler(Enclave* enclave)
      : enclave_(enclave),
        siblings_(*enclave->cpus()),
        next_sibling_(-1),
        task_channel_(GHOST_MAX_QUEUE_ELEMS, /*node=*/0, *enclave->cpus()) {
    // Note that 'task_channel_' is configured such that either of
    // siblings can be woken up when a message is produced into it.
    //
    // We verify below that the kernel wakes up the sibling that
    // the task last ran on.
  }

  bool Empty(const Cpu& cpu) const {
    absl::MutexLock lock(&mu_);
    return task_ == nullptr;
  }

  // Admit a new task into the scheduler.
  void TaskNew(uint64_t gtid, bool runnable, struct ghost_sw_info sw_info,
               uint32_t seqnum) {
    absl::MutexLock lock(&mu_);

    ASSERT_THAT(task_, IsNull());
    ASSERT_THAT(runnable, IsTrue());

    task_ = absl::make_unique<CoreSchedTask>(Gtid(gtid), sw_info);
    task_->run_state = CoreSchedTask::RunState::kRunnable;
    task_->seqnum = seqnum;
    task_->sibling = 0;  // arbitrary.

    ASSERT_THAT(task_channel_.AssociateTask(task_->gtid, task_->seqnum),
                IsTrue());

    const Cpu cpu = siblings_[task_->sibling];
    ASSERT_THAT(enclave_->GetAgent(cpu)->Ping(), IsTrue());
  }

  void Schedule(const Cpu& agent_cpu, uint32_t agent_barrier, bool prio_boost,
                bool finished) {
    // MutexLock cannot be used because we must drop the lock before
    // calling scheduling functions like LocalYield() that block in
    // the kernel.
    mu_.Lock();

    // At the end of this loop there are no inflight transactions and
    // the mutex serializes with the sibling's scheduling loop.
    for (const Cpu& cpu : siblings_) {
      CpuState* cs = cpu_state(cpu);
      if (!cs->next) continue;

      EXPECT_THAT(cs->current, IsNull());
      EXPECT_THAT(cs->next, Eq(task_.get()));
      EXPECT_THAT(next_sibling_, AnyOf(Eq(0), Eq(1)));

      RunRequest* req = enclave_->GetRunRequest(cpu);
      if (enclave_->CompleteRunRequest(req)) {
        // Transaction committed successfully: promote 'cs->next' to 'current'.
        cs->current = cs->next;
        task_->sibling = next_sibling_;
        task_->run_state = CoreSchedTask::RunState::kOnCpu;
      }

      cs->next = nullptr;
    }

    // We are testing agent wakeups so this test depends on controlling when
    // each agent wakes up. The one wakeup we cannot control is when the main
    // thread pings the agent via Agent::Terminate(). Thus we consume messages
    // only if the agent isn't being terminated or if it is the rightful agent
    // to handle the final TASK_BLOCKED/TASK_DEAD messages (see cl/334728088
    // for a detailed description of the race between pthread_join() and
    // task_dead_ghost()).
    while (!finished || (task_ && agent_cpu == siblings_[task_->sibling])) {
      Message msg = Peek(&task_channel_);
      if (msg.empty()) break;

      // 'task_channel_' is exclusively for task state change msgs.
      EXPECT_THAT(msg.is_task_msg(), IsTrue());
      EXPECT_THAT(msg.type(), Ne(MSG_TASK_NEW));

      task_->Advance(msg.seqnum());

      Cpu task_cpu = siblings_[task_->sibling];
      CpuState* cs = cpu_state(task_cpu);

      // Scheduling is simple: we track whether the task is runnable
      // or blocked. If the task is runnable the agent schedules it
      // on the sibling cpu.
      //
      // We verify that 'agent_cpu' and 'task_cpu' match in all cases.
      switch (msg.type()) {
        case MSG_TASK_WAKEUP:
          EXPECT_THAT(task_->blocked(), IsTrue());
          EXPECT_THAT(task_cpu, Eq(agent_cpu));
          EXPECT_THAT(cs->current, IsNull());
          task_->run_state = CoreSchedTask::RunState::kRunnable;
          break;

        case MSG_TASK_BLOCKED:
          EXPECT_THAT(task_->oncpu(), IsTrue());
          EXPECT_THAT(task_cpu, Eq(agent_cpu));
          EXPECT_THAT(cs->current, Eq(task_.get()));
          task_->run_state = CoreSchedTask::RunState::kBlocked;
          cs->current = nullptr;
          break;

        case MSG_TASK_DEAD:
          EXPECT_THAT(task_->blocked(), IsTrue());
          EXPECT_THAT(task_cpu, Eq(agent_cpu));
          EXPECT_THAT(cs->current, IsNull());
          task_.reset();
          break;

        case MSG_TASK_YIELD:
        case MSG_TASK_PREEMPT:
          EXPECT_THAT(task_->oncpu(), IsTrue());
          EXPECT_THAT(task_cpu, Eq(agent_cpu));
          EXPECT_THAT(cs->current, Eq(task_.get()));
          task_->run_state = CoreSchedTask::RunState::kRunnable;
          cs->current = nullptr;
          break;

        default:
          break;
      }

      Consume(&task_channel_, msg);
    }

    if (finished && !task_) {
      mu_.Unlock();
      return;
    }

    // Either we don't have a runnable task or a task in another sched_class
    // wants to run on this cpu.
    if (prio_boost || !task_ || !task_->runnable()) {
      // Don't yield CPU indefinitely if we have useful work to do:
      // e.g. schedule a runnable task or terminate.
      const int run_flags =
          (task_ && task_->runnable()) || finished ? RTLA_ON_IDLE : 0;
      mu_.Unlock();
      RunRequest* req = enclave_->GetRunRequest(agent_cpu);
      req->LocalYield(agent_barrier, run_flags);
      return;
    }

    EXPECT_THAT(task_->runnable(), IsTrue());

    // Idle on 'agent_cpu' and schedule task on 'other_cpu'.
    int other_sibling = agent_cpu == siblings_[0] ? (siblings_.Size() - 1) : 0;
    Cpu other_cpu = siblings_[other_sibling];
    CpuState* cs = cpu_state(other_cpu);

    EXPECT_THAT(cs->current, IsNull());

    const int sync_group_owner = agent_cpu.id();
    RunRequest* this_req = enclave_->GetRunRequest(agent_cpu);
    this_req->Open({
        .target = Gtid(GHOST_IDLE_GTID),
        .agent_barrier = agent_barrier,
        .commit_flags = COMMIT_AT_TXN_COMMIT,
        .sync_group_owner = sync_group_owner,
    });

    EXPECT_THAT(this_req->sync_group_owned(), IsTrue());
    EXPECT_THAT(this_req->sync_group_owner_get(), Eq(sync_group_owner));

    cs->next = task_.get();
    next_sibling_ = other_sibling;
    RunRequest* other_req = enclave_->GetRunRequest(other_cpu);
    other_req->Open({
        .target = cs->next->gtid,
        .target_barrier = cs->next->seqnum,
        .commit_flags = COMMIT_AT_TXN_COMMIT,
        .sync_group_owner = sync_group_owner,
    });
    EXPECT_THAT(other_req->sync_group_owned(), IsTrue());
    EXPECT_THAT(other_req->sync_group_owner_get(), Eq(sync_group_owner));

    mu_.Unlock();

    if (enclave_->CommitSyncRequests(siblings_)) {
      // Commit succeeded and the kernel has already released all transactions
      // in the sync_group on our behalf. Here's why: if this cpu is part of
      // the sync group then the local agent doesn't get an opportunity to
      // release ownership before it schedules.
    } else {
      // The sync_group commit failed and txn ownership was released by the
      // CommitSyncRequests() API after validating the reason for failure.
    }
  }

 private:
  struct CpuState {
    CoreSchedTask* current = nullptr;
    CoreSchedTask* next = nullptr;
  } ABSL_CACHELINE_ALIGNED;

  CpuState* cpu_state(const Cpu& cpu) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    CHECK_GE(cpu.id(), 0);
    CHECK_LT(cpu.id(), cpu_states_.size());
    return &cpu_states_[cpu.id()];
  }

  Enclave* enclave_;
  const CpuList& siblings_;

  mutable absl::Mutex mu_;
  int next_sibling_ ABSL_GUARDED_BY(mu_);
  Channel task_channel_ ABSL_GUARDED_BY(mu_);
  std::unique_ptr<CoreSchedTask> task_ ABSL_GUARDED_BY(mu_);
  std::array<CpuState, MAX_CPUS> cpu_states_ ABSL_GUARDED_BY(mu_);
};

// This test ensures the version check functionality works properly.
// 'Ghost::GetVersion' should return a version that matches 'GHOST_VERSION'.
TEST(ApiTest, CheckVersion) {
  uint64_t kernel_abi_version;
  ASSERT_THAT(Ghost::GetVersion(kernel_abi_version), Eq(0));
  ASSERT_THAT(kernel_abi_version, testing::Eq(GHOST_VERSION));
  // Use 'GHOST_VERSION + 1' rather than a specific number, such as 0, so that
  // this test doesn't fail if the number we choose happens to be the current
  // kernel ABI version.
  ASSERT_THAT(kernel_abi_version, testing::Ne(GHOST_VERSION + 1));
}

// Bare-bones agent implementation that can schedule exactly one task.
// TODO: Put agent and test thread into separate address spaces to
// avoid potential deadlock.
class TimeAgent : public Agent {
 public:
  TimeAgent(Enclave* enclave, Cpu cpu)
      : Agent(enclave, cpu),
        channel_(GHOST_MAX_QUEUE_ELEMS, kNumaNode,
                 MachineTopology()->ToCpuList({cpu})) {
    channel_.SetEnclaveDefault();
  }

  // Wait for agent to idle.
  void WaitForIdle() { idle_.WaitForNotification(); }

 protected:
  void AgentThread() override {
    // Boilerplate to synchronize startup until all agents are ready
    // (mostly redundant since we only have a single agent in the test).
    SignalReady();
    WaitForEnclaveReady();

    std::unique_ptr<Task> task(nullptr);
    bool runnable = false;
    while (true) {
      while (true) {
        Message msg = Peek(&channel_);
        if (msg.empty()) break;

        // Ignore all types other than task messages (e.g. CPU_TICK).
        if (msg.is_task_msg() && msg.type() != MSG_TASK_NEW) {
          ASSERT_THAT(task, NotNull());
          task->Advance(msg.seqnum());
        }

        // Scheduling is simple: we track whether the task is runnable
        // or blocked. If the task is runnable the agent switches to it
        // and idles otherwise.
        switch (msg.type()) {
          case MSG_TASK_NEW: {
            const ghost_msg_payload_task_new* payload =
                static_cast<const ghost_msg_payload_task_new*>(msg.payload());

            ASSERT_THAT(task, IsNull());
            task =
                absl::make_unique<Task>(Gtid(payload->gtid), payload->sw_info);
            task->seqnum = msg.seqnum();
            runnable = payload->runnable;
            break;
          }

          case MSG_TASK_WAKEUP:
            ASSERT_THAT(task, NotNull());
            ASSERT_FALSE(runnable);
            runnable = true;
            break;

          case MSG_TASK_BLOCKED:
            ASSERT_THAT(task, NotNull());
            ASSERT_TRUE(runnable);
            runnable = false;
            break;

          case MSG_TASK_YIELD: {
            ASSERT_THAT(task, NotNull());
            ASSERT_TRUE(runnable);
            runnable = true;
            ASSERT_THAT(commit_time_, Ne(absl::UnixEpoch()));
            absl::Duration switch_delay =
                task->status_word.switch_time() - commit_time_;
            EXPECT_THAT(switch_delay, Gt(absl::ZeroDuration()));
            EXPECT_THAT(switch_delay, Lt(absl::Microseconds(100)));
            break;
          }

          case MSG_TASK_DEAD:
            ASSERT_THAT(task, NotNull());
            ASSERT_FALSE(runnable);
            task = nullptr;
            break;

          default:
            // This includes task messages like TASK_PREEMPTED and cpu messages
            // like CPU_TICK that don't influence runnability. We do handle
            // MSG_TASK_YIELD as a special case above since we want to check the
            // context switch time.
            break;
        }
        Consume(&channel_, msg);
      }

      StatusWord::BarrierToken agent_barrier = status_word().barrier();
      const bool prio_boost = status_word().boosted_priority();

      if (Finished() && !task) break;

      RunRequest* req = enclave()->GetRunRequest(cpu());
      if (!task || !runnable || prio_boost) {
        NotifyIdle();
        req->LocalYield(agent_barrier, prio_boost ? RTLA_ON_IDLE : 0);
      } else {
        absl::Time now = MonotonicNow();
        req->Open({
            .target = task->gtid,
            .target_barrier = task->seqnum,
            .agent_barrier = agent_barrier,
            .commit_flags = COMMIT_AT_TXN_COMMIT,
        });
        req->Commit();

        commit_time_ = req->commit_time();
        absl::Duration commit_delay = commit_time_ - now;
        EXPECT_THAT(commit_delay, Gt(absl::ZeroDuration()));
        EXPECT_THAT(commit_delay, Lt(absl::Microseconds(100)));
      }
    }
  }

 private:
  // The NUMA node that the channel is on.
  static constexpr int kNumaNode = 0;

  // Notify the main thread when agent idles for the first time.
  void NotifyIdle() {
    if (first_idle_) {
      first_idle_ = false;
      idle_.Notify();
    }
  }

  Channel channel_;

  Notification idle_;
  absl::Time commit_time_;
  bool first_idle_ = true;
};

// Tests that the kernel writes plausible commit times to transactions and
// plausible context switch times to task status words.
TEST(ApiTest, KernelTimes) {
  Ghost::InitCore();

  // Arbitrary but safe because there must be at least one CPU.
  constexpr int kCpuNum = 0;
  Topology* topology = MachineTopology();
  auto enclave = absl::make_unique<LocalEnclave>(
      AgentConfig(topology, topology->ToCpuList(std::vector<int>{kCpuNum})));
  const Cpu kAgentCpu = topology->cpu(kCpuNum);

  TimeAgent agent(enclave.get(), kAgentCpu);
  agent.Start();
  enclave->Ready();

  agent.WaitForIdle();

  GhostThread t(GhostThread::KernelScheduler::kGhost, [] {
    sched_yield();  // MSG_TASK_YIELD.
  });
  t.Join();

  agent.Terminate();
}

// Tests that `Ghost::SchedGetAffinity()` returns the correct affinity mask for
// a thread.
TEST(ApiTest, SchedGetAffinity) {
  // This test requires at least 4 CPUs.
  if (MachineTopology()->num_cpus() < 4) {
    GTEST_SKIP() << "must have at least 4 cpus";
    return;
  }

  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(0, &set);
  CPU_SET(2, &set);
  CPU_SET(3, &set);
  ASSERT_THAT(sched_setaffinity(0, sizeof(set), &set), Eq(0));

  CpuList cpus = MachineTopology()->EmptyCpuList();
  ASSERT_THAT(Ghost::SchedGetAffinity(Gtid::Current(), cpus), IsTrue());
  EXPECT_THAT(cpus, Eq(MachineTopology()->ToCpuList(set)));
}

}  // namespace
}  // namespace ghost