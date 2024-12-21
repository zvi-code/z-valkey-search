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

#include "vmsdk/src/module_type.h"

#include <string>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"

namespace vmsdk {

void DoDeregister(RedisModuleCtx *ctx, RedisModuleKey *module_key,
                  absl::string_view key) {
  if (RedisModule_DeleteKey(module_key) != REDISMODULE_OK) {
    VMSDK_LOG(WARNING, ctx) << "failed to delete redis key " << key;
    DCHECK(false);
  }
}

ModuleType::ModuleType(RedisModuleCtx *ctx, absl::string_view key,
                       RedisModuleType *module_type)
    : module_type_(module_type),
      detached_ctx_(vmsdk::MakeUniqueRedisDetachedThreadSafeContext(ctx)),
      key_(key) {
  DCHECK(module_type);
}

absl::Status ModuleType::Register(RedisModuleCtx *ctx, absl::string_view key,
                                  void *ptr, RedisModuleType *module_type) {
  auto redis_str = MakeUniqueRedisString(key);
  auto module_key =
      MakeUniqueRedisOpenKey(ctx, redis_str.get(), REDISMODULE_WRITE);
  if (!module_key) {
    return absl::InternalError(
        absl::StrCat("failed to open Redis module key: ", key));
  }
  if (RedisModule_KeyType(module_key.get()) != REDISMODULE_KEYTYPE_EMPTY) {
    return absl::AlreadyExistsError(
        absl::StrCat("Redis module key ", key, " already exists"));
  }
  if (RedisModule_ModuleTypeSetValue(module_key.get(), module_type, ptr) !=
      REDISMODULE_OK) {
    DoDeregister(ctx, module_key.get(), key);
    return absl::InternalError(
        absl::StrCat("failed to set module type value for key: ", key));
  }
  return absl::OkStatus();
}

absl::Status ModuleType::Deregister(RedisModuleCtx *ctx,
                                    absl::string_view key) {
  auto redis_str = MakeUniqueRedisString(key);

  if (!RedisModule_KeyExists(ctx, redis_str.get())) return absl::OkStatus();

  auto module_key =
      MakeUniqueRedisOpenKey(ctx, redis_str.get(), REDISMODULE_WRITE);
  if (!module_key) {
    DCHECK(false);
    return absl::InternalError(
        absl::StrCat("failed to open redis key: ", key.data()));
  }
  DoDeregister(ctx, module_key.get(), key);
  return absl::OkStatus();
}

absl::Status ModuleType::Register(RedisModuleCtx *ctx) {
  return Register(ctx, key_, this, module_type_);
}

absl::Status ModuleType::Deregister(RedisModuleCtx *ctx) {
  return Deregister(ctx, key_);
}

}  // namespace vmsdk
