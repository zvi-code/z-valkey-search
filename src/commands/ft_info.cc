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

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

absl::Status FTInfoCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc) {
  if (argc < 2) {
    return absl::InvalidArgumentError(
        vmsdk::WrongArity(kInfoCommand).c_str());
  }

  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();
  VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
  auto index_schema_name = vmsdk::ToStringView(itr_arg);

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(RedisModule_GetSelectedDb(ctx),
                                               index_schema_name));

  index_schema->RespondWithInfo(ctx);

  return absl::OkStatus();
}

}  // namespace valkey_search
