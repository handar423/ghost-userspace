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

// Runs the RocksDB test for ghOSt or CFS.
#include <csignal>

#include "lib/topology.h"
#include "absl/flags/parse.h"
#include "experiments/rocksdb_origin/orchestrator.h"

ABSL_FLAG(std::string, print_format, "pretty",
          "Results print format (\"pretty\" or \"csv\", default: \"pretty\")");
ABSL_FLAG(bool, print_distribution, false,
          "Prints every request's results (default: false)");
ABSL_FLAG(bool, print_ns, false,
          "Prints the results in nanoseconds if true. Prints the results in "
          "microseconds if false (default: false).");
ABSL_FLAG(bool, print_get, false,
          "Prints an additional section that shows the results for Get "
          "requests, if true (default: false).");
ABSL_FLAG(bool, print_range, false,
          "Prints an additional section that shows the results for Range "
          "queries, if true (default: false).");
ABSL_FLAG(std::string, rocksdb_db_path, "",
          "The path to the RocksDB database. Creates the database if it does "
          "not exist.");
ABSL_FLAG(double, throughput, 20000.0,
          "The synthetic throughput generated in units of requests per second "
          "(default: 20,000 requests per second).");
ABSL_FLAG(
    double, range_query_ratio, 0.0,
    "The share of requests that are range queries. This value must be greater "
    "than or equal to 0.0 and less than or equal to 1.0. The share of requests "
    "that are Get requests is '1 - range_query_ratio'. (default: 0.0).");
ABSL_FLAG(int, load_generator_cpu, 10,
          "The CPU that the load generator thread runs on (default: 10).");
ABSL_FLAG(int, cfs_dispatcher_cpu, 11,
          "For CFS (Linux Completely Fair Scheduler) experiments, the CPU that "
          "the dispatcher runs on (default: 11).");
ABSL_FLAG(size_t, num_workers, 6,
          "The number of workers. Each worker has one thread. (default: 6).");
// It is preferred that the 'worker_cpus' flag be an 'std::vector<int>', but the
// only vector type that Abseil supports is 'std::vector<std::string>'.
ABSL_FLAG(std::vector<std::string>, worker_cpus,
          std::vector<std::string>({"12", "13", "14", "15", "16", "17"}),
          "The CPUs that worker threads run on for CFS (Linux Completely Fair "
          "Scheduler) experiments. Each worker thread is pinned to its own "
          "CPU. This, the number of CPUs must be equal to the 'num_workers' "
          "flag value. For ghOSt experiments, ghOSt assigns workers to CPUs. "
          "Thus, no CPUs should be specified with this flag when ghOSt is "
          "used. (default: 12,13,14,15,16,17).");
ABSL_FLAG(std::string, cfs_wait_type, "spin",
          "For CFS experiments, the way that worker threads wait until they "
          "are assigned more work by the dispatcher (\"spin\" or \"futex\", "
          "default: \"spin\").");
ABSL_FLAG(
    absl::Duration, get_duration, absl::Microseconds(10),
    "The duration of Get requests. This includes both accessing the RocksDB "
    "database and doing synthetic work. (default: 10 microseconds)");
ABSL_FLAG(
    absl::Duration, range_duration, absl::Microseconds(10000),
    "The duration of Range queries. This includes both accessing the RocksDB "
    "database and doing synthetic work. (default: 10,000 microseconds)");
ABSL_FLAG(absl::Duration, get_exponential_mean, absl::Microseconds(0),
          "If nonzero, a sample from the exponential distribution with this "
          "mean is generated and added to each Get request service time. This "
          "total service time for Get requests is 'get_duration' + Exp(1 / "
          "'get_exponential_mean') micrseconds. (default: 0)");
ABSL_FLAG(size_t, batch, 1,
          "The maximum number of requests assigned to a worker at once. This "
          "number must be greater than 0. (default: 1 request)");
ABSL_FLAG(absl::Duration, experiment_duration, absl::InfiniteDuration(),
          "The experiment duration (default: infinity).");
ABSL_FLAG(absl::Duration, discard_duration, absl::Seconds(2),
          "All results from when the experiment starts to when the discard "
          "duration has elapsed are discarded. We do not want the results to "
          "include initialization costs, such as page faults. (default: 2s).");
ABSL_FLAG(std::string, scheduler, "cfs",
          "The scheduler to use (\"cfs\" for Linux Completely Fair Scheduler "
          "or \"ghost\" for ghOSt, default: \"cfs\")");
ABSL_FLAG(
    uint32_t, ghost_qos, 2,
    "For the ghOSt experiments, this is the QoS (Quality-of-Service) class for "
    "the PrioTable work class that all worker sched items are added to.");

namespace {
// Parses all command line flags and returns them as a
// 'ghost_test::Orchestrator::Options' instance.
ghost_test::Orchestrator::Options GetOptions() {
  ghost_test::Orchestrator::Options options;

  std::string print_format = absl::GetFlag(FLAGS_print_format);
  CHECK(print_format == "pretty" || print_format == "csv");
  options.print_options.pretty = (print_format == "pretty");

  options.print_options.distribution = absl::GetFlag(FLAGS_print_distribution);
  options.print_options.ns = absl::GetFlag(FLAGS_print_ns);
  options.print_options.os = &std::cout;
  options.print_get = absl::GetFlag(FLAGS_print_get);
  options.print_range = absl::GetFlag(FLAGS_print_range);
  options.rocksdb_db_path = absl::GetFlag(FLAGS_rocksdb_db_path);
  options.throughput = absl::GetFlag(FLAGS_throughput);
  options.range_query_ratio = absl::GetFlag(FLAGS_range_query_ratio);
  options.load_generator_cpu = absl::GetFlag(FLAGS_load_generator_cpu);
  options.cfs_dispatcher_cpu = absl::GetFlag(FLAGS_cfs_dispatcher_cpu);
  options.num_workers = absl::GetFlag(FLAGS_num_workers);

  const std::vector<std::string> worker_cpus = absl::GetFlag(FLAGS_worker_cpus);
  for (const std::string& cpu : worker_cpus) {
    options.worker_cpus.push_back(std::stoi(cpu));
  }

  std::string cfs_wait_type = absl::GetFlag(FLAGS_cfs_wait_type);
  CHECK(cfs_wait_type == "spin" || cfs_wait_type == "futex");

  options.get_duration = absl::GetFlag(FLAGS_get_duration);
  CHECK_GE(options.get_duration, absl::ZeroDuration());

  options.range_duration = absl::GetFlag(FLAGS_range_duration);
  CHECK_GE(options.range_duration, absl::ZeroDuration());

  options.get_exponential_mean = absl::GetFlag(FLAGS_get_exponential_mean);
  CHECK_GE(options.get_exponential_mean, absl::ZeroDuration());

  options.batch = absl::GetFlag(FLAGS_batch);

  options.experiment_duration = absl::GetFlag(FLAGS_experiment_duration);
  CHECK_GE(options.experiment_duration, absl::ZeroDuration());

  options.discard_duration = absl::GetFlag(FLAGS_discard_duration);
  CHECK_GE(options.discard_duration, absl::ZeroDuration());

  std::string scheduler = absl::GetFlag(FLAGS_scheduler);
  CHECK(scheduler == "cfs" || scheduler == "ghost");
  options.scheduler = ghost::GhostThread::KernelScheduler::kGhost;

  options.ghost_qos = absl::GetFlag(FLAGS_ghost_qos);

  return options;
}

// Sets a timer signal to fire after 'duration'.
void SetTimer(absl::Duration duration) {
  CHECK_GE(duration, absl::ZeroDuration());
  int64_t remainder = absl::ToInt64Microseconds(duration) %
                      absl::ToInt64Microseconds(absl::Seconds(1));
  absl::Duration seconds = duration - absl::Microseconds(remainder);
  itimerval itimer = {.it_interval = {.tv_sec = 0, .tv_usec = 0},
                      .it_value = {.tv_sec = absl::ToInt64Seconds(seconds),
                                   .tv_usec = remainder}};
  CHECK_EQ(setitimer(ITIMER_REAL, &itimer, nullptr), 0);
}

}  // namespace

// Registers signal handlers for SIGINT and SIGALRM (for the timer). When
// receiving either of the signals for the first time, the application stops the
// experiment, prints the results, and exits. After receiving two SIGINTs, the
// program will force exit.
ghost::Notification* RegisterSignalHandlers() {
  // 'exit' must be static so that it can be implicitly captured by the signal
  // handler lambdas. We allocate its memory on the heap as the
  // 'ghost::Notification' type is not trivially destructible.
  static ghost::Notification* exit = new ghost::Notification;

  std::signal(SIGALRM, [](int signum) {
    CHECK_EQ(signum, SIGALRM);
    std::cout << "Timer fired..." << std::endl;
    // 'exit' may have already been notified by SIGINT.
    if (!exit->HasBeenNotified()) {
      exit->Notify();
    }
  });
  return exit;
}

// Initializes the application, runs the experiment, and exits.
int main(int argc, char* argv[]) {
  ghost::Notification* exit = RegisterSignalHandlers();

  // Affine the main thread to CPU
  cpu_set_t cpuset = ghost::Topology::ToCpuSet(ghost::MachineTopology()->ToCpuList(std::vector<int>{
                     ghost_test::Orchestrator::kBackgroundThreadCpu}));
  sched_setaffinity(0, sizeof(cpuset), &cpuset);

  absl::ParseCommandLine(argc, argv);

  ghost_test::Orchestrator::Options options = GetOptions();
  std::cout << options << std::endl;
  std::cout << std::endl;

  std::cout << "Initializing..." << std::endl;
  std::unique_ptr<ghost_test::Orchestrator> orch;
  orch = std::make_unique<ghost_test::Orchestrator>(options, options.num_workers + 1);
  printf("Initialization complete.\n");
  // When `stdout` is directed to a terminal, it is newline-buffered. When
  // `stdout` is directed to a non-interactive device (e.g, a Python subprocess
  // pipe), it is fully buffered. Thus, in order for the Python script to read
  // the initialization message as soon as it is passed to `printf`, we need to
  // manually flush `stdout`.
  fflush(stdout);

  SetTimer(options.experiment_duration);

  exit->WaitForNotification();
  orch->Terminate();

  return 0;
}
