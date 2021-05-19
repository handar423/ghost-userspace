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

#ifndef GHOST_EXPERIMENTS_ROCKSDB_CFS_ORCHESTRATOR_H_
#define GHOST_EXPERIMENTS_ROCKSDB_CFS_ORCHESTRATOR_H_

#include "absl/synchronization/barrier.h"
#include "experiments/rocksdb/latency.h"
#include "experiments/rocksdb/orchestrator.h"
#include "experiments/rocksdb/request.h"
#include "experiments/shared/cfs.h"

namespace ghost_test {

// This is the orchestrator for the CFS (Linux Completely Fair Scheduler)
// experiments. All threads are scheduled by CFS. The worker threads may either
// (1) spin when waiting for more work to be assigned to them or (2) sleep on a
// futex until more work is assigned to them.
//
// Example:
// Orchestrator::Options options;
// ... Fill in the options.
// CfsOrchestrator orchestrator_(options);
// (Constructs orchestrator with options.)
// ...
// orchestrator_.Terminate();
// (Tells orchestrator to stop the experiment and print the results.)
class CfsOrchestrator final : public Orchestrator {
 public:
  explicit CfsOrchestrator(Orchestrator::Options opts);
  ~CfsOrchestrator() final {}

  void Terminate() final;

 protected:
  // For CFS, the load generator passes requests to the dispatcher.
  void LoadGenerator(uint32_t sid) final;

  void Dispatcher(uint32_t sid) final;

  void Worker(uint32_t sid) final;

 private:
  // Initializes the thread pool.
  void InitThreadPool();

  // The dispatcher calls this method to receive requests sent to it by the load
  // generator.
  void HandleLoadGenerator();

  // The dispatcher calls this method to populate 'idle_sids_' with a list of
  // the SIDs of idle workers. Note that this method clears 'idle_sids_' before
  // filling it in.
  void GetIdleWorkerSIDs();

  // The total number of threads, including the load generator thread, the
  // dispatcher thread, and the worker threads.
  const size_t total_threads_ = 0;

  // Allows runnable threads to run and keeps idle threads either spinning or
  // sleeping on a futex until they are marked runnable again.
  CompletelyFairScheduler cfs_;

  // Each thread (the load generator, the dispatcher, and the workers)
  // decrements this once they have initialized themselves. This barrier is used
  // to block the load generator until all threads have been initialized so that
  // it does not generate load while the system is initializing. If it generated
  // load while the system is initializing, the experiment results would be bad
  // solely due to initialization costs rather than any deficiency in the
  // system. The initialization costs are irrelevant to the experiment.
  absl::Barrier threads_ready_;

  // The max number of requests that the load generator will send at a time to
  // the dispatcher.
  static constexpr size_t kLoadGeneratorBatchSize = 100;

  // The dispatcher's queue on waiting requests to assign to workers.
  std::deque<Request> dispatcher_queue_;

  // The dispatcher uses this to store idle SIDs. We make this a class member
  // rather than a local variable in the 'Dispatcher' method to avoid repeatedly
  // allocating memory for the list backing in the dispatcher common case, which
  // is expensive.
  std::list<uint32_t> idle_sids_;
};

}  // namespace ghost_test

#endif  // GHOST_EXPERIMENTS_ROCKSDB_CFS_ORCHESTRATOR_H_
