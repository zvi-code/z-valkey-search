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

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_

#include <deque>
#include <memory>

#include "absl/status/statusor.h"
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
class ValkeySearch;
// Declared here to support testing
void SendReply(RedisModuleCtx *ctx, std::deque<indexes::Neighbor> &neighbors,
               const query::VectorSearchParameters &parameters);
namespace async {

struct Result {
  absl::StatusOr<std::deque<indexes::Neighbor>> neighbors;
  std::unique_ptr<query::VectorSearchParameters> parameters;
};

int Reply(RedisModuleCtx *ctx, [[maybe_unused]] RedisModuleString **argv,
          [[maybe_unused]] int argc);

void Free(RedisModuleCtx * /*ctx*/, void *privdata);

}  // namespace async

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_SEARCH_H_
