/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
#include "gtest/gtest_prod.h"
#include "vmsdk/src/thread_safe_vector.h"
#include "vmsdk/src/thread_monitoring.h"

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

  /// Notify all active workers to terminate and join them. In addition, this
  /// method will internally call `JoinTerminatedWorkers`
  void JoinWorkers();

  /// Cleanup after threads that self terminated after pool resize operation and
  /// were placed in the `pending_join_threads_` queue.
  void JoinTerminatedWorkers();

  absl::Status MarkForStop(StopMode stop_mode);
  absl::Status SuspendWorkers();
  bool IsSuspended() const {
    absl::MutexLock lock(&queue_mutex_);
    return suspend_workers_;
  }
  absl::Status ResumeWorkers();
  virtual ~ThreadPool();

  size_t Size() const { return threads_.Size(); }
  size_t QueueSize() const ABSL_LOCKS_EXCLUDED(queue_mutex_);
  enum class Priority { kLow = 0, kHigh = 1, kMax = 2 };
  virtual bool Schedule(absl::AnyInvocable<void()> task, Priority priority)
      ABSL_LOCKS_EXCLUDED(queue_mutex_);

  /// Resize the pool size to `count` threads. If `wait_for_resize` is `true`,
  /// this method waits for resize operation to complete; otherwise, the resize
  /// operation is done asynchronously.
  void Resize(size_t count, bool wait_for_resize = false);

  /// A struct representing a worker thread
  struct Thread {
    bool IsShutdown() const { return shutdown_flag.load(); }
    void Shutdown(absl::AnyInvocable<void()> callback = nullptr) {
      if (callback != nullptr) {
        shutdown_callback = std::move(callback);
      }
      shutdown_flag.store(true);
    }

    /// If `shutdown_callback is` not null, call it
    void InvokeShutdownCallback() {
      if (shutdown_callback.has_value()) {
        (*shutdown_callback)();
      }
    }

    void InitThreadMonitor() {
      thread_monitor_ = std::make_unique<ThreadMonitor>(thread_id);
    }

    pthread_t thread_id = 0;
    std::atomic_bool shutdown_flag = false;
    /// If not null, the thread will call this callback when it exits via the
    /// shutdown_flag
    std::optional<absl::AnyInvocable<void()>> shutdown_callback = std::nullopt;
    std::unique_ptr<vmsdk::ThreadMonitor> thread_monitor_;
  };

  absl::StatusOr<double> GetAvgCPUPercentage();

  void WorkerThread(std::shared_ptr<Thread> thread)
      ABSL_LOCKS_EXCLUDED(queue_mutex_);

 private:
  void IncrThreadCountBy(size_t count);
  void DecrThreadCountBy(size_t count, bool sync);

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
  inline std::queue<absl::AnyInvocable<void()>>& GetPriorityTasksQueue(
      Priority priority) ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_) {
    return priority_tasks_[static_cast<int>(priority)];
  }
  size_t initial_thread_count_ = 0;
  ThreadSafeVector<std::shared_ptr<Thread>> threads_;
  ThreadSafeVector<std::shared_ptr<Thread>> pending_join_threads_;
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
  FRIEND_TEST(ThreadPoolTest, DynamicSizing);
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_THREAD_POOL_H_
