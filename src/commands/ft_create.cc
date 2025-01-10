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
#include "src/commands/ft_create_parser.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search {

absl::Status FTCreateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  VMSDK_ASSIGN_OR_RETURN(auto index_schema_proto,
                         ParseFTCreateArgs(ctx, argv + 1, argc - 1));
  VMSDK_RETURN_IF_ERROR(
      SchemaManager::Instance().CreateIndexSchema(ctx, index_schema_proto));

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  RedisModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
