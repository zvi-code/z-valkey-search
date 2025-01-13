
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
#ifndef VMSDK_SRC_UTILS_H_
#define VMSDK_SRC_UTILS_H_
#include <cassert>
#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
namespace vmsdk {

class StopWatch {
 public:
  StopWatch() { Reset(); }
  ~StopWatch() = default;
  void Reset() { start_time_ = absl::Now(); }
  absl::Duration Duration() const { return absl::Now() - start_time_; }

 private:
  absl::Time start_time_;
};
// Timer creation from background threads is not safe. The event loop of Redis/
// Valkey releases the GIL, and during this period also checks the timer data
// structure for the next pending timer, meaning there is no way to safely
// create a timer from a background thread.
//
// This function creates a timer from a background thread by creating a task
// that is added to the event loop, which then creates the timer.
int StartTimerFromBackgroundThread(RedisModuleCtx *ctx, mstime_t period,
                                   RedisModuleTimerProc callback, void *data);
struct TimerDeletionTask {
  RedisModuleCtx *ctx;
  RedisModuleTimerID timer_id;
  absl::AnyInvocable<void(void *)> user_data_deleter;
};
int StopTimerFromBackgroundThread(
    RedisModuleCtx *ctx, RedisModuleTimerID timer_id,
    absl::AnyInvocable<void(void *)> user_data_deleter);

void TrackCurrentAsMainThread();
bool IsMainThread();
inline void VerifyMainThread() { CHECK(IsMainThread()); }

// MainThreadAccessGuard ensures that all access to the underlying data
// structure is done on the main thread.
template <typename T>
class MainThreadAccessGuard {
 public:
  MainThreadAccessGuard() = default;
  MainThreadAccessGuard(const T &var) : var_(var) {}
  MainThreadAccessGuard(T&& var) noexcept : var_(std::move(var)) {}
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> const &other) {
    VerifyMainThread();
    var_ = other.var_;
    return *this;
  }
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> &&other) noexcept {
    VerifyMainThread();
    var_ = std::move(other.var_);
    return *this;
  }
  T &Get() {
    VerifyMainThread();
    return var_;
  }
  const T &Get() const {
    VerifyMainThread();
    return var_;
  }

 private:
  T var_;
};

int RunByMain(absl::AnyInvocable<void()> fn, bool force_async = false);

std::string WrongArity(absl::string_view cmd);

}  // namespace vmsdk
#endif  // VMSDK_SRC_UTILS_H_
