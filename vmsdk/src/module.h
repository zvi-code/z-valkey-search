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

#ifndef VMSDK_SRC_MODULE_H_
#define VMSDK_SRC_MODULE_H_

#include <list>
#include <optional>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/utils.h"  // IWYU pragma: keep

#define VALKEY_MODULE(options)                                              \
  namespace {                                                               \
  extern "C" {                                                              \
  int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,     \
                         int argc) {                                        \
    vmsdk::TrackCurrentAsMainThread();                                      \
    if (auto status = vmsdk::module::OnLoad(ctx, argv, argc, options);      \
        status != REDISMODULE_OK) {                                         \
      return status;                                                        \
    }                                                                       \
                                                                            \
    if (options.on_load.has_value()) {                                      \
      return vmsdk::module::LogOnLoad(                                      \
          options.on_load.value()(ctx, argv, argc, options), ctx, options); \
    }                                                                       \
    return vmsdk::module::LogOnLoad(absl::OkStatus(), ctx, options);        \
  }                                                                         \
  int RedisModule_OnUnload(RedisModuleCtx *ctx) {                           \
    if (options.on_unload.has_value()) {                                    \
      options.on_unload.value()(ctx, options);                              \
    }                                                                       \
    return REDISMODULE_OK;                                                  \
  }                                                                         \
  }                                                                         \
  }

namespace vmsdk {
namespace module {

struct CommandOptions {
  std::string cmd_name;
  std::string permissions;
  RedisModuleCmdFunc cmd_func{nullptr};
  // By default - assume no keys.
  int first_key{0};
  int last_key{0};
  int key_step{0};
};

struct Options {
  std::string name;
  RedisModuleInfoFunc info{nullptr};
  std::list<CommandOptions> commands;
  using OnLoad = std::optional<absl::AnyInvocable<absl::Status(
      RedisModuleCtx *, RedisModuleString **, int, const Options &)>>;

  using OnUnload = std::optional<
      absl::AnyInvocable<void(RedisModuleCtx *, const Options &)>>;
  OnLoad on_load;
  OnUnload on_unload;
};

int LogOnLoad(absl::Status status, RedisModuleCtx *ctx, const Options &options);

int OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           const Options &options);

absl::Status RegisterInfo(RedisModuleCtx *ctx, RedisModuleInfoFunc info);

}  // namespace module
using ModuleCommandFunc = absl::Status (*)(RedisModuleCtx *,
                                           RedisModuleString **, int);
template <ModuleCommandFunc func>
int CreateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  auto status = func(ctx, argv, argc);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, status.message().data());
  }
  return REDISMODULE_OK;
}
}  // namespace vmsdk

#endif  // VMSDK_SRC_MODULE_H_
