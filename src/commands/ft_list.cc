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

#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/redismodule.h"

namespace valkey_search {

absl::Status FTListCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if (argc > 1) {
    return absl::InvalidArgumentError(
        vmsdk::WrongArity(kListCommand).c_str());
  }
  absl::flat_hash_set<std::string> names =
      SchemaManager::Instance().GetIndexSchemasInDB(
          RedisModule_GetSelectedDb(ctx));
  RedisModule_ReplyWithArray(ctx, names.size());
  for (const auto &name : names) {
    RedisModule_ReplyWithSimpleString(ctx, name.c_str());
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
