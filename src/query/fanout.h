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

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_H_

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::fanout {

struct FanoutSearchTarget {
  enum Type {
    kLocal,
    kRemote,
  };
  Type type;
  // Empty string if type is kLocal.
  std::string address;

  bool operator==(const FanoutSearchTarget& other) const {
    return type == other.type && address == other.address;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const FanoutSearchTarget& target) {
    os << "FanoutSearchTarget{type: " << target.type
       << ", address: " << target.address << "}";
    return os;
  }
};

absl::Status PerformSearchFanoutAsync(
    RedisModuleCtx* ctx, std::vector<FanoutSearchTarget>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::VectorSearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool, query::SearchResponseCallback callback);

std::vector<FanoutSearchTarget> GetSearchTargetsForFanout(RedisModuleCtx* ctx);

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
