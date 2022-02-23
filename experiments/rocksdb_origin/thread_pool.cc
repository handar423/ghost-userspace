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

#include "experiments/rocksdb_origin/thread_pool.h"

namespace ghost_test {

ExperimentThreadPool::~ExperimentThreadPool() {
  // Check that all threads have been joined.
  CHECK(absl::c_all_of(threads_,
                       [](const std::unique_ptr<std::thread>& thread) {
                         return !thread->joinable();
                       }));
}

void ExperimentThreadPool::Init(
    const std::vector<ghost::GhostThread::KernelScheduler>& ksched,
    const std::vector<std::function<void(uint32_t)>>& thread_work) {
  CHECK_EQ(ksched.size(), num_threads_);
  CHECK_EQ(ksched.size(), thread_work.size());

  threads_.reserve(num_threads_);
  for (uint32_t i = 0; i < num_threads_; i++) {
    threads_.push_back(std::make_unique<std::thread>(
      std::bind(&ExperimentThreadPool::ThreadMain, this, i, thread_work[i])));
  }
}

void ExperimentThreadPool::MarkExit(uint32_t sid) {
  thread_triggers_.Trigger(sid);
}

void ExperimentThreadPool::ThreadMain(
    uint32_t i, std::function<void(uint32_t)> thread_work) {
  // 模拟一些可入侵的函数
  usleep(i + 1);
  if(i == 0)
    sleep(1);
  while (!ShouldExit(i)) {
    thread_work(i);
  }
  // fprintf(stderr, "thread %d exit!\n", i);
  num_exited_.fetch_add(1, std::memory_order_release);
}

void ExperimentThreadPool::Join() {
  // Check that all threads have already been notified to exit. If not, the call
  // to `Join` below will hang on one the threads because that thread will not
  // exit.
  for (uint32_t i = 0; i < num_threads_; i++) {
    CHECK(thread_triggers_.Triggered(/*sid=*/i));
  }
  for (std::unique_ptr<std::thread>& thread : threads_) {
    // Check that `thread` is joinable. `thread` will not be joinable if it has
    // already been joined.
    CHECK(thread->joinable());
    thread->join();
  }
}

}  // namespace ghost_test
