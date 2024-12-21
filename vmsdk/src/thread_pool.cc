// Copyright 2024 Google LLC
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

#include "vmsdk/src/thread_pool.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
namespace {

void *RunWorkerThread(void *arg) {
  vmsdk::ThreadPool *pool = static_cast<vmsdk::ThreadPool *>(arg);
  pool->WorkerThread();
  return nullptr;
}
}  // namespace

namespace vmsdk {

ThreadPool::ThreadPool(const std::string &name_prefix, size_t num_threads)
    : threads_(num_threads),
      priority_tasks_(static_cast<int>(ThreadPool::Priority::kMax) + 1),
      name_prefix_(name_prefix) {}

void ThreadPool::StartWorkers() {
  CHECK(!started_);
  started_ = true;
  for (size_t i = 0; i < threads_.size(); ++i) {
    pthread_create(&threads_[i], nullptr, RunWorkerThread, this);
    pthread_setname_np(threads_[i], (name_prefix_ + std::to_string(i)).c_str());
  }
}

bool ThreadPool::Schedule(absl::AnyInvocable<void()> task, Priority priority) {
  absl::MutexLock lock(&queue_mutex_);
  if (stop_mode_.has_value()) {
    return false;
  }
  auto &tasks_queue = GetProrityTasksQueue(priority);
  tasks_queue.emplace(std::move(task));
  condition_.Signal();
  return true;
}

absl::Status ThreadPool::MarkForStop(StopMode stop_mode) {
  absl::MutexLock lock(&queue_mutex_);
  if (stop_mode_ == stop_mode) {
    return absl::OkStatus();
  }
  if (stop_mode_ == StopMode::kAbrupt && stop_mode == StopMode::kGraceful) {
    return absl::InvalidArgumentError(
        "Cannot set stop mode to kGraceful after kAbrupt mode was set");
  }
  stop_mode_ = stop_mode;
  suspend_workers_ = false;
  condition_.SignalAll();
  return absl::OkStatus();
}

ThreadPool::~ThreadPool() { JoinWorkers(); }

void ThreadPool::JoinWorkers() {
  {
    absl::MutexLock lock(&queue_mutex_);
    if (!stop_mode_.has_value()) {
      stop_mode_ = StopMode::kGraceful;
      condition_.SignalAll();
    }
    suspend_workers_ = false;
  }
  if (!started_) {
    return;
  }
  for (size_t i = 0; i < threads_.size(); ++i) {
    pthread_join(threads_[i], nullptr);
  }
  started_ = false;
}

absl::Status ThreadPool::SuspendWorkers() {
  absl::MutexLock lock(&suspend_resume_mutex_);
  DCHECK(!blocking_refcount_);
  {
    absl::MutexLock lock(&queue_mutex_);
    if (!started_) {
      return absl::InvalidArgumentError("Thread pool is not started");
    }
    if (stop_mode_.has_value()) {
      return absl::InvalidArgumentError(
          "Cannot suspend workers after the thread pool is marked for stop");
    }
    if (suspend_workers_) {
      return absl::InvalidArgumentError("Thread pool is already suspended");
    }
    suspend_workers_ = true;
    blocking_refcount_ =
        std::make_unique<absl::BlockingCounter>(threads_.size());
    condition_.SignalAll();
  }
  blocking_refcount_->Wait();
  blocking_refcount_ = nullptr;
  return absl::OkStatus();
}

absl::Status ThreadPool::ResumeWorkers() {
  absl::MutexLock lock(&suspend_resume_mutex_);
  DCHECK(!blocking_refcount_);
  {
    absl::MutexLock lock(&queue_mutex_);
    if (stop_mode_.has_value()) {
      return absl::InvalidArgumentError(
          "Cannot resume workers after the thread pool is marked for stop");
    }
    if (!suspend_workers_) {
      return absl::InvalidArgumentError("Thread pool is not suspended");
    }
    suspend_workers_ = false;
    blocking_refcount_ =
        std::make_unique<absl::BlockingCounter>(threads_.size());
  }

  blocking_refcount_->Wait();
  blocking_refcount_ = nullptr;
  return absl::OkStatus();
}

bool SuspendResumeReady(bool *suspend_workers) { return !(*suspend_workers); }

void ThreadPool::AwaitSuspensionCleared()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_) {
  if (!suspend_workers_) {
    return;
  }
  blocking_refcount_->DecrementCount();
  queue_mutex_.Await(absl::Condition(SuspendResumeReady, &suspend_workers_));
  if (blocking_refcount_) {
    blocking_refcount_->DecrementCount();
  }
}

void ThreadPool::WorkerThread() {
  while (true) {
    absl::AnyInvocable<void()> task;
    {
      absl::MutexLock lock(&queue_mutex_);
      AwaitSuspensionCleared();
      auto condition = absl::Condition(this, &ThreadPool::QueueReady);
      while (!condition.Eval()) {
        condition_.Wait(&queue_mutex_);
      }
      if (stop_mode_.has_value() &&
          (stop_mode_.value() == StopMode::kAbrupt ||
           std::all_of(priority_tasks_.begin(), priority_tasks_.end(),
                       [](const auto &tasks) { return tasks.empty(); }))) {
        return;
      }
      if (suspend_workers_) {
        continue;
      }
      for (auto it = priority_tasks_.rbegin(); it != priority_tasks_.rend();
           ++it) {
        if (!it->empty()) {
          task = std::move(it->front());
          it->pop();
          break;
        }
      }
    }
    task();
  }
}

size_t ThreadPool::QueueSize() const {
  absl::MutexLock lock(&queue_mutex_);
  return std::accumulate(
      priority_tasks_.begin(), priority_tasks_.end(), 0,
      [](size_t sum, const auto &tasks) { return sum + tasks.size(); });
}

}  // namespace vmsdk
