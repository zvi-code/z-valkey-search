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

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

struct FTCreateTagParameters {
  absl::string_view separator{","};
  bool case_sensitive{false};
};

constexpr int kDefaultInitialCap{10 * 1024};

struct FTCreateVectorParameters {
  std::optional<int> dimensions;
  data_model::DistanceMetric distance_metric{
      data_model::DistanceMetric::DISTANCE_METRIC_UNSPECIFIED};
  data_model::VectorDataType vector_data_type{
      data_model::VectorDataType::VECTOR_DATA_TYPE_UNSPECIFIED};
  int initial_cap{kDefaultInitialCap};
  absl::Status Verify() const;
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

constexpr int kDefaultBlockSize{1024};
constexpr int kDefaultM{16};
constexpr int kDefaultEFConstruction{200};
constexpr int kDefaultEFRuntime{10};

struct HNSWParameters : public FTCreateVectorParameters {
  // Tightly connected with internal dimensionality of the data
  // strongly affects the memory consumption
  int m{kDefaultM};
  int ef_construction{kDefaultEFConstruction};
  size_t ef_runtime{kDefaultEFRuntime};
  absl::Status Verify() const;
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

struct FlatParameters : public FTCreateVectorParameters {
  // Block size holds the amount of vectors in a contiguous array. This is
  // useful when the index is dynamic with respect to addition and deletion.
  uint32_t block_size{kDefaultBlockSize};
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

absl::StatusOr<data_model::IndexSchema> ParseFTCreateArgs(
    RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
