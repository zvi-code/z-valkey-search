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

#include "vmsdk/src/module.h"

#include <list>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/log.h"

namespace vmsdk {
namespace module {

int LogOnLoad(absl::Status status, RedisModuleCtx *ctx,
              const Options &options) {
  if (status.ok()) {
    RedisModule_Log(
        ctx, REDISMODULE_LOGLEVEL_NOTICE, "%s",
        absl::StrCat(options.name, " module was successfully loaded!").c_str());
    return REDISMODULE_OK;
  }
  RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                  status.message().data());
  return REDISMODULE_ERR;
}

absl::Status RegisterInfo(RedisModuleCtx *ctx, RedisModuleInfoFunc info) {
  if (info == nullptr) {
    return absl::OkStatus();
  }
  if (RedisModule_RegisterInfoFunc(ctx, info) == REDISMODULE_ERR) {
    return absl::InternalError("Failed to register info");
  }
  return absl::OkStatus();
}

absl::Status RegisterCommands(RedisModuleCtx *ctx,
                              const std::list<CommandOptions> &commands) {
  for (auto &command : commands) {
    if (RedisModule_CreateCommand(ctx, command.cmd_name.data(),
                                  command.cmd_func, command.permissions.data(),
                                  command.first_key, command.last_key,
                                  command.key_step) == REDISMODULE_ERR) {
      return absl::InternalError(
          absl::StrCat("Failed to create command: ", command.cmd_name.data()));
    }
  }
  return absl::OkStatus();
}

int OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           const Options &options) {
  if (RedisModule_Init(ctx, options.name.c_str(), 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Failed to init module");
    return REDISMODULE_ERR;
  }
  UseValkeyAlloc();
  auto status = vmsdk::InitLogging(ctx);
  if (!status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING,
                    "Failed to init logging, %s", status.message().data());
    return REDISMODULE_ERR;
  }
  if (auto status = RegisterCommands(ctx, options.commands); !status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                    status.message().data());
    return REDISMODULE_ERR;
  }
  // Initialize counters for request count metric
  if (auto status = RegisterInfo(ctx, options.info); !status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                    status.message().data());
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
}  // namespace module
}  // namespace vmsdk
