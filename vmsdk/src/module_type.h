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

#ifndef VMSDK_SRC_MODULETYPE_H_
#define VMSDK_SRC_MODULETYPE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class ModuleType {
 public:
  ModuleType(RedisModuleCtx *ctx, absl::string_view key,
             RedisModuleType *module_type);
  virtual ~ModuleType() = default;
  absl::Status Register(RedisModuleCtx *ctx);
  absl::Status Deregister(RedisModuleCtx *ctx);

  static absl::Status Register(RedisModuleCtx *ctx, absl::string_view key,
                               void *ptr, RedisModuleType *module_type);
  static absl::Status Deregister(RedisModuleCtx *ctx, absl::string_view key);

 protected:
  RedisModuleType *module_type_{nullptr};
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  std::string key_;
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_MODULETYPE_H_
