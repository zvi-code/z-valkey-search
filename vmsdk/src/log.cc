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

#include "vmsdk/src/log.h"

#include <cerrno>
#include <cstddef>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/globals.h"
#include "absl/log/log_entry.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/status/status_macros.h"

namespace vmsdk {

const char* ToStrLogLevel(int log_level) {
  switch (log_level) {
    case 0:
      return REDISMODULE_LOGLEVEL_WARNING;
    case 1:
      return REDISMODULE_LOGLEVEL_NOTICE;
    case 2:
      return REDISMODULE_LOGLEVEL_VERBOSE;
    case 3:
      return REDISMODULE_LOGLEVEL_DEBUG;
  }
  CHECK(false);
}

static inline std::string DefaultSinkFormater(const absl::LogEntry& entry) {
  pthread_t thread_id = pthread_self();
  return absl::StrFormat(
      "[%s], tid: %lu, %s:%d: %s", ToStrLogLevel(entry.verbosity()),
      static_cast<unsigned long>(thread_id),
      entry.source_filename(), entry.source_line(), entry.text_message());
}

struct SinkOptions {
  LogFormatterFunc formatter{DefaultSinkFormater};
  bool log_level_specified{false};
};

static SinkOptions sink_options;

LogFormatterFunc GetSinkFormatter() { return sink_options.formatter; }
void SetSinkFormatter(LogFormatterFunc formatter) {
  if (formatter) {
    sink_options.formatter = formatter;
  } else {
    sink_options.formatter = DefaultSinkFormater;
  }
}

const absl::flat_hash_map<std::string, LogLevel> kLogLevelMap = {
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_WARNING), LogLevel::kWarning},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_NOTICE), LogLevel::kNotice},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_VERBOSE), LogLevel::kVerbose},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_DEBUG), LogLevel::kDebug},
};

absl::StatusOr<std::string> FetchEngineLogLevel(RedisModuleCtx* ctx) {
  auto reply = vmsdk::UniquePtrRedisCallReply(
      RedisModule_Call(ctx, "CONFIG", "cc", "GET", "loglevel"));
  if (reply == nullptr) {
    if (errno == EINVAL) {
      return absl::InvalidArgumentError(
          "Error fetch Valkey Engine log level: EINVAL (command "
          "name is invalid, the format specifier uses characters "
          "that are not recognized, or the command is called with "
          "the wrong number of arguments)");
    } else {
      return absl::InternalError(
          absl::StrCat("Error fetch Valkey Engine log level: errno=", errno));
    }
  }

  RedisModuleCallReply* loglevel_reply =
      RedisModule_CallReplyArrayElement(reply.get(), 1);

  if (loglevel_reply == NULL ||
      RedisModule_CallReplyType(loglevel_reply) != REDISMODULE_REPLY_STRING) {
    return absl::NotFoundError(
        absl::StrCat("Log level value is missing or not a string."));
  }

  size_t len;
  const char* loglevel_str =
      RedisModule_CallReplyStringPtr(loglevel_reply, &len);
  return std::string(loglevel_str, len);
}

absl::Status InitLogging(RedisModuleCtx* ctx,
                         std::optional<std::string> log_level_str) {
  if (!log_level_str.has_value()) {
    auto engine_log_level = FetchEngineLogLevel(ctx);
    if (!engine_log_level.ok()) {
      // It is possible we can't get it, e.g. if the CONFIG command is renamed.
      // In such a case, we log a warning and default to NOTICE.
      VMSDK_LOG(WARNING, ctx)
          << "Failed to fetch Valkey Engine log level, "
          << engine_log_level.status() << ", using default log level: "
          << ToStrLogLevel(static_cast<int>(LogLevel::kNotice));
      log_level_str = ToStrLogLevel(static_cast<int>(LogLevel::kNotice));
    } else {
      log_level_str = engine_log_level.value();
    }
    sink_options.log_level_specified = false;
  } else {
    sink_options.log_level_specified = true;
  }
  auto itr = kLogLevelMap.find(absl::AsciiStrToLower(log_level_str.value()));
  if (itr == kLogLevelMap.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unknown sevirity `", log_level_str.value(), "`"));
  }
  absl::SetGlobalVLogLevel(static_cast<int>(itr->second));

  return absl::OkStatus();
}

const char* ReportedLogLevel(int log_level) {
  if (sink_options.log_level_specified) {
    return REDISMODULE_LOGLEVEL_WARNING;
  }
  return ToStrLogLevel(log_level);
}

void ValkeyLogSink::Send(const absl::LogEntry& entry) {
  RedisModule_Log(ctx_, ReportedLogLevel(entry.verbosity()), "%s",
                  GetSinkFormatter()(entry).c_str());
}

void ValkeyIOLogSink::Send(const absl::LogEntry& entry) {
  RedisModule_LogIOError(io_, ReportedLogLevel(entry.verbosity()), "%s",
                         GetSinkFormatter()(entry).c_str());
}

}  // namespace vmsdk
