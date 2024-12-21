/*
 * Copyright 2024 Google LLC
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

#ifndef VMSDK_SRC_THREAD_POOL_H_
#define VMSDK_SRC_THREAD_POOL_H_

#include <pthread.h>  // NOLINT(build/c++11)

#include <cstddef>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"

namespace vmsdk {
// Note google3/thread can't be used as it's not open source
class ThreadPool {
 public:
  ThreadPool(const std::string& name, size_t num_threads);
  // This type is neither copyable nor movable.
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  enum class StopMode { kGraceful, kAbrupt };

  void StartWorkers();
  void JoinWorkers();
  absl::Status MarkForStop(StopMode stop_mode);
  absl::Status SuspendWorkers();
  bool IsSuspended() const {
    absl::MutexLock lock(&queue_mutex_);
    return suspend_workers_;
  }
  absl::Status ResumeWorkers();
  virtual ~ThreadPool();

  size_t Size() const { return threads_.size(); }
  size_t QueueSize() const ABSL_LOCKS_EXCLUDED(queue_mutex_);
  enum class Priority { kLow = 0, kHigh = 1, kMax = 2 };
  virtual bool Schedule(absl::AnyInvocable<void()> task, Priority priority)
      ABSL_LOCKS_EXCLUDED(queue_mutex_);
  void WorkerThread() ABSL_LOCKS_EXCLUDED(queue_mutex_);

 private:
  inline void AwaitSuspensionCleared()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_);
  inline bool QueueReady() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_) {
    for (const auto& queue : priority_tasks_) {
      if (!queue.empty()) {
        return true;
      }
    }
    return stop_mode_.has_value() || suspend_workers_;
  }
  inline std::queue<absl::AnyInvocable<void()>>& GetProrityTasksQueue(
      Priority priority) ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_) {
    return priority_tasks_[static_cast<int>(priority)];
  }
  std::vector<pthread_t> threads_;
  mutable absl::Mutex queue_mutex_;
  absl::CondVar condition_ ABSL_GUARDED_BY(queue_mutex_);
  std::vector<std::queue<absl::AnyInvocable<void()>>> priority_tasks_
      ABSL_GUARDED_BY(queue_mutex_);
  std::string name_prefix_;
  std::optional<StopMode> stop_mode_ ABSL_GUARDED_BY(queue_mutex_);
  bool started_{false};
  std::unique_ptr<absl::BlockingCounter> blocking_refcount_;
  bool suspend_workers_ ABSL_GUARDED_BY(queue_mutex_){false};

  // Suspend and resume are mutually exclusive.
  mutable absl::Mutex suspend_resume_mutex_;
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_THREAD_POOL_H_
