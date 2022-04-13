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

#ifndef GHOST_EXPERIMENTS_ROCKSDB_ORCHESTRATOR_H_
#define GHOST_EXPERIMENTS_ROCKSDB_ORCHESTRATOR_H_

#include <filesystem>
#include <mutex>

#include "experiments/rocksdb_origin/database.h"
#include "experiments/rocksdb_origin/ingress.h"
#include "experiments/rocksdb_origin/latency.h"
#include "experiments/rocksdb_origin/request.h"
#include "experiments/rocksdb_origin/thread_pool.h"

namespace ghost_test {

// This class is the central orchestrator of the RocksDB experiment. It manages
// the load generator, the dispatcher (if one exists), and the workers. It
// prints out the results when the experiment is finished.
//
// Note that this is an abstract class so it cannot be constructed.
class Orchestrator {
 public:
  // Orchestrator configuration options.
  struct Options {
    // Parses all command line flags and returns them as an 'Options' instance.
    static Options GetOptions();

    // This pass a string representation of 'options' to 'os'. The options are
    // printed in alphabetical order by name.
    friend std::ostream& operator<<(std::ostream& os, const Options& options);

    latency::PrintOptions print_options;

    // The orchestrator prints the overall results for all request types
    // combined, no matter what.

    // If true, the orchestrator will also print a section with the results for
    // just Get requests.
    bool print_get;

    // If true, the orchestrator will also print a section with the results for
    // just Range queries.
    bool print_range;

    // The path to the RocksDB database.
    std::filesystem::path rocksdb_db_path;

    // The throughput of the generated synthetic load.
    double throughput;

    // The share of requests that are Range queries. This value must be greater
    // than or equal to 0 and less than or equal to 1. The share of requests
    // that are Get requests is '1 - range_query_ratio'.
    double range_query_ratio;

    // The CPU that the load generator thread runs on.
    int load_generator_cpu;

    // For CFS (Linux Completely Fair Scheduler) experiments, the CPU that the
    // dispatcher runs on.
    int cfs_dispatcher_cpu;

    // The number of workers. Each worker has one thread.
    size_t num_workers;

    // The CPUs that worker threads run on for CFS (Linux Completely Fair
    // Scheduler) experiments. Each worker thread is pinned to its own CPU.
    // Thus, `worker_cpus.size()` must be equal to `num_workers`.
    //
    // For ghOSt experiments, ghOSt assigns workers to CPUs. Thus, this vector
    // must be empty when ghOSt is used.
    std::vector<int> worker_cpus;

    // The total amount of time spent processing a Get request in RocksDB and
    // doing synthetic work.
    absl::Duration get_duration;

    // The total amount of time spent processing a Range query in RocksDB and
    // doing synthetic work.
    absl::Duration range_duration;

    // The Get request service time distribution can be converted from a fixed
    // distribution to an exponential distribution by adding a sample from the
    // exponential distribution with a mean of 'get_exponential_mean' to
    // 'get_duration' to get the total service time for a Get request.
    //
    // Distribution: 'get_duration' + Exp(1 / 'get_exponential_mean')
    //
    // To keep the Get request service time distribution as a fixed
    // distribution, set 'get_exponential_mean' to 'absl::ZeroDuration()'.
    absl::Duration get_exponential_mean;

    // The maximum number of requests to assign at a time to a worker. Generally
    // this should be set to 1 (otherwise centralized queuing and Shinjuku
    // cannot be faithfully implemented) but we support larger batches for
    // future experiments that may want them.
    size_t batch;

    // The experiment duration.
    absl::Duration experiment_duration;

    // Discards all results from when the experiment starts to when the
    // 'discard' duration has elapsed. We do not want the results to include
    // initialization costs, such as page faults.
    absl::Duration discard_duration;

    // The scheduler that schedules the experiment. This is either CFS (Linux
    // Completely Fair Scheduler) or ghOSt.
    ghost::GhostThread::KernelScheduler scheduler;

    // For the ghOSt experiments, this is the QoS (Quality-of-Service) class
    // for the PrioTable work class that all worker sched items are added to.
    uint32_t ghost_qos;
  };

  // Threads use this type to pass requests to each other. In the CFS (Linux
  // Completely Fair Scheduler) experiments, the load generator uses this to
  // pass requests to the dispatcher and the dispatcher uses this to pass
  // requests to workers. In the ghOSt experiments, the load generator uses this
  // to pass requests to workers.
  //
  // When 'num_requests' is greater than zero, there are pending requests for
  // the worker. When 'num_requests' is 0, there are no pending requests for the
  // worker, so the dispatcher should add requests.
  struct WorkerWork {
    // The number of requests in 'requests'. We use this atomic rather than just
    // look at 'requests.size()' since the dispatcher and the worker need an
    // atomic to sync on. This number should never be greater than
    // 'options_.batch'.
    std::atomic<size_t> num_requests;
    // The requests.
    std::vector<Request> requests;
  } ABSL_CACHELINE_ALIGNED;

  // Constructs the orchestrator. 'options' is the experiment settings.
  // 'total_threads' is the total number of threads managed by the orchestrator,
  // including the load generator thread and the worker threads.
  Orchestrator(Options options, size_t total_threads);

  // Affine all background threads to this CPU.
  static constexpr int kBackgroundThreadCpu = 0;

  // Stops the experiment, joins all threads (i.e., the load generator, the
  // dispatcher (if one exists), and the workers) and prints the results
  // specified in 'options_'.
  void Terminate();

  // Returns the CPU runtime for the calling thread.
  absl::Duration GetThreadCpuTime() const;

 protected:
  // The SID (sched item identifier) of the load generator.
  static constexpr uint32_t kLoadGeneratorSid = 0;

  // This method is executed in a loop by the load generator thread. This method
  // checks the ingress queue for pending requests. 'sid' is the sched item
  // identifier for the load generator thread.
  void LoadGenerator(uint32_t sid);

  // This method is executed in a loop by worker threads. This method handles
  // the request assigned to it by accessing the RocksDB database and doing
  // synthetic work. 'sid' is the sched item identifier for the worker thread.
  void Worker(uint32_t sid);

  // Handles 'request' which is either a Get request or a Range query. 'gen' is
  // a random bit generator used for Get requests that have an exponential
  // service time. 'gen' is used to generate a sample from the exponential
  // distribution.
  void HandleRequest(Request& request, absl::BitGen& gen);

  // Prints all results (total numbers of requests, throughput, and latency
  // percentiles). 'experiment_duration' is the duration of the experiment.
  void PrintResults(absl::Duration experiment_duration) const;

  const Options& options() const { return options_; }

  size_t total_threads() const { return total_threads_; }

  SyntheticNetwork& network() { return network_; }

  ExperimentThreadPool& thread_pool() { return thread_pool_; }

  absl::Time start() const { return start_; }
  void set_start(absl::Time start) { start_ = start; }

  std::vector<std::unique_ptr<WorkerWork>>& worker_work() {
    return worker_work_;
  }

  std::vector<std::vector<Request>>& requests() { return requests_; }

  std::vector<absl::BitGen>& gen() { return gen_; }

  ThreadTrigger& first_run() { return first_run_; }

 private:
  // Processes 'request', which must be a Get request (a CHECK will fail if
  // not).
  void HandleGet(Request& request, absl::BitGen& gen);

  // Processes 'request', which must be a Range query (a CHECK will fail if
  // not).
  void HandleRange(Request& request, absl::BitGen& gen);

  // Prints the results for 'requests' (total number of requests in 'requests',
  // throughput, and latency percentiles). 'results_name' is a name printed with
  // the results (e.g., "Get" for Get requests) and 'experiment_duration' is the
  // duration of the experiment.
  void PrintResultsHelper(const std::string& results_name,
                          absl::Duration experiment_duration,
                          const std::vector<Request>& requests) const;

  // Squashes the two-dimensional 'requests' vector into a one-dimensional
  // vector and returns the one-dimensional vector. Each request is only added
  // into the one-dimensional vector if 'should_include' returns true when the
  // request is passed as a parameter to it.
  std::vector<Request> FilterRequests(
      const std::vector<std::vector<Request>>& requests,
      std::function<bool(const Request&)> should_include) const;

  // Returns true if 'request' was generated during the discard period should
  // not be included in the results. Returns false if 'request' was generated
  // after the discard and should be included in the results.
  bool ShouldDiscard(const Request& request) const;

  // Spins for 'duration'. 'start_duration' is the CPU time consumed by the
  // thread when calling this method. There is overhead to calling this method,
  // such as creating the stack frame, so passing 'start_duration' allows the
  // method to count this overhead toward its synthetic work.
  void Spin(absl::Duration duration, absl::Duration start_duration) const;

  // Initializes the thread pool.
  void InitThreadPool();

  // Initializes the ghOSt PrioTable.
  void InitGhost();

  // The load generator calls this method to populate 'idle_sids_' with a list
  // of the SIDs of idle workers. Note that this method clears 'idle_sids_'
  // before filling it in.
  void GetIdleWorkerSIDs();

  // We do not need a different class of service (e.g., different expected
  // runtimes, different QoS (Quality-of-Service) classes, etc.) across workers
  // in our experiments. Furthermore, all workers are ghOSt one-shots and the
  // only candidate for a repeatable -- the load generator -- runs in CFS. Thus,
  // put all worker sched items in the same work class.
  static constexpr uint32_t kWorkClassIdentifier = 0;

  // 'threads_ready_' is notified once all threads have been spawned and the
  // ghOSt PrioTable has been initialized with the work class and all worker
  // thread sched items.
  ghost::Notification threads_ready_;

  // The load generator uses this to store idle SIDs. We make this a class
  // member rather than a local variable in the 'LoadGenerator' method to avoid
  // repeatedly allocating memory for the list backing in the load generator
  // common case, which is expensive.
  std::list<uint32_t> idle_sids_;

  // Orchestrator options.
  const Options options_;

  // The total number of threads managed by the orchestrator, including the load
  // generator thread, the worker threads, and if relevant, the dispatcher
  // thread.
  const size_t total_threads_;

  // The RocksDB database.
  Database database_;

  // The synthetic network that the load generator uses to generate synthetic
  // requests.
  SyntheticNetwork network_;

  // The time that the experiment started at (after initialization).
  absl::Time start_;

  // Shared memory used by the dispatcher to pass requests to workers. Worker
  // 'i' accesses index 'i' in this vector. We wrap each 'WorkerWork' struct in
  // a unique pointer since the struct contains an atomic and therefore does not
  // have a copy constructor, so it cannot be stored directly into a vector.
  std::vector<std::unique_ptr<WorkerWork>> worker_work_;

  // The requests processed to completion by workers.
  std::vector<std::vector<Request>> requests_;

  // Random bit generators. Each thread has its own bit generator since the bit
  // generators are not thread safe.
  std::vector<absl::BitGen> gen_;

  // A thread triggers itself the first time it runs. Each thread does work on
  // its first iteration, so each thread uses the trigger to know if it is in
  // its first iteration or not.
  ThreadTrigger first_run_;

  // The thread pool.
  ExperimentThreadPool thread_pool_;

  // Mutex for single task queue
  std::mutex queue_mutex;

};

}  // namespace ghost_test

#endif  // GHOST_EXPERIMENTS_ROCKSDB_ORCHESTRATOR_H_