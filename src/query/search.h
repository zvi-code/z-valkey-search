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

#ifndef VALKEYSEARCH_SRC_QUERY_SEARCH_H_
#define VALKEYSEARCH_SRC_QUERY_SEARCH_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/hnswlib/hnswlib.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query {

constexpr int64_t kTimeoutMS{50000};
const size_t kMaxTimeoutMs{60000};

struct LimitParameter {
  uint64_t first_index{0};
  uint64_t number{10};
};

struct ReturnAttribute {
  vmsdk::UniqueRedisString identifier;
  vmsdk::UniqueRedisString alias;
};

struct VectorSearchParameters {
  std::shared_ptr<IndexSchema> index_schema;
  std::string index_schema_name;
  std::string attribute_alias;
  vmsdk::UniqueRedisString score_as;
  std::string query;
  uint32_t dialect{2};
  bool local_only{false};
  std::optional<int> k;
  std::optional<uint64_t> ef;
  LimitParameter limit;
  uint64_t timeout_ms{kTimeoutMS};
  bool no_content{false};
  FilterParseResults filter_parse_results;
  std::vector<ReturnAttribute> return_attributes;
};

// Callback to be called when the search is done.
using SearchResponseCallback =
    absl::AnyInvocable<void(absl::StatusOr<std::deque<indexes::Neighbor>>&,
                            std::unique_ptr<VectorSearchParameters>)>;

absl::StatusOr<std::deque<indexes::Neighbor>> Search(
    const VectorSearchParameters& parameters, bool is_local_search);

absl::Status SearchAsync(std::unique_ptr<VectorSearchParameters> parameters,
                         vmsdk::ThreadPool* thread_pool,
                         SearchResponseCallback callback, bool is_local_search);

absl::StatusOr<std::deque<indexes::Neighbor>> MaybeAddIndexedContent(
    absl::StatusOr<std::deque<indexes::Neighbor>> results,
    const VectorSearchParameters& parameters);

class Predicate;
// Defined in the header to support testing
size_t EvaluateFilterAsPrimary(
    const Predicate* predicate,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& enteries_fetchers,
    bool negate);

// Defined in the header to support testing
absl::StatusOr<std::deque<indexes::Neighbor>> PerformVectorSearch(
    indexes::VectorBase* vector_index,
    const VectorSearchParameters& parameters);

std::priority_queue<std::pair<float, hnswlib::labeltype>>
CalcBestMatchingPrefiltereddKeys(
    const VectorSearchParameters& parameters,
    std::queue<std::unique_ptr<indexes::EntriesFetcherBase>>& enteries_fetchers,
    indexes::VectorBase* vector_index);

}  // namespace valkey_search::query
#endif  // VALKEYSEARCH_SRC_QUERY_SEARCH_H_
