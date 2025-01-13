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

#ifndef VMSDK_SRC_LOG_H_
#define VMSDK_SRC_LOG_H_

#include <optional>
#include <string>

#include "absl/log/log.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/status/status.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

enum class LogLevel : int {
  kWarning = 0,
  kNotice = 1,
  kVerbose = 2,
  kDebug = 3,
};
// NOLINTNEXTLINE
constexpr LogLevel WARNING = static_cast<LogLevel>(LogLevel::kWarning);
// NOLINTNEXTLINE
constexpr LogLevel NOTICE = static_cast<LogLevel>(LogLevel::kNotice);
// NOLINTNEXTLINE
constexpr LogLevel VERBOSE = static_cast<LogLevel>(LogLevel::kVerbose);
// NOLINTNEXTLINE
constexpr LogLevel DEBUG = static_cast<LogLevel>(LogLevel::kDebug);

namespace vmsdk {

const char* ToStrLogLevel(int log_level);

using LogFormatterFunc = std::string (*)(const absl::LogEntry&);

LogFormatterFunc GetSinkFormatter();
void SetSinkFormatter(LogFormatterFunc formatter);

class ValkeyLogSink : public absl::LogSink {
 public:
  void Send(const absl::LogEntry& entry) override;
  void SetContext(RedisModuleCtx* ctx) { ctx_ = ctx; }

 private:
  RedisModuleCtx* ctx_{nullptr};
};

class ValkeyIOLogSink : public absl::LogSink {
 public:
  void Send(const absl::LogEntry& entry) override;
  void SetModuleIO(RedisModuleIO* io) { io_ = io; }

 private:
  RedisModuleIO* io_{nullptr};
};

absl::Status InitLogging(
    RedisModuleCtx* ctx,
    std::optional<std::string> log_level_str = std::nullopt);

static thread_local ValkeyLogSink log_sink;
static thread_local ValkeyIOLogSink io_log_sink;

}  // namespace vmsdk

#define VMSDK_LOG(log_level, ctx)  \
  vmsdk::log_sink.SetContext(ctx); \
  VLOG(static_cast<int>(log_level)).ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_LOG_EVERY_N(log_level, ctx, n)   \
  vmsdk::log_sink.SetContext(ctx);             \
  VLOG_EVERY_N(static_cast<int>(log_level), n) \
      .ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_LOG_EVERY_N_SEC(log_level, ctx, n)   \
  vmsdk::log_sink.SetContext(ctx);                 \
  VLOG_EVERY_N_SEC(static_cast<int>(log_level), n) \
      .ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_IO_LOG(log_level, module_io)   \
  vmsdk::io_log_sink.SetModuleIO(module_io); \
  VLOG(static_cast<int>(log_level)).ToSinkOnly(&vmsdk::io_log_sink)

#define VMSDK_IO_LOG_EVERY_N(log_level, module_io, n) \
  vmsdk::io_log_sink.SetModuleIO(module_io);          \
  VLOG_EVERY_N(static_cast<int>(log_level), n)        \
      .ToSinkOnly(&vmsdk::io_log_sink)

#define VMSDK_IO_LOG_EVERY_N_SEC(log_level, module_io, n) \
  vmsdk::io_log_sink.SetModuleIO(module_io);              \
  VLOG_EVERY_N_SEC(static_cast<int>(log_level), n)        \
      .ToSinkOnly(&vmsdk::io_log_sink)

#endif  // VMSDK_SRC_LOG_H_
