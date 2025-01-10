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

#ifndef VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#define VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

bool HashHasRecord(RedisModuleKey *key, absl::string_view identifier);

class RecordsMapValue {
 public:
  RecordsMapValue(vmsdk::UniqueRedisString identifier,
                  vmsdk::UniqueRedisString value)
      : value(std::move(value)), identifier_(std::move(identifier)) {}
  RecordsMapValue(RedisModuleString *identifier, vmsdk::UniqueRedisString value)
      : value(std::move(value)), identifier_(identifier) {}
  vmsdk::UniqueRedisString value;
  RedisModuleString *GetIdentifier() const {
    if (absl::holds_alternative<vmsdk::UniqueRedisString>(identifier_)) {
      return absl::get<vmsdk::UniqueRedisString>(identifier_).get();
    }
    return absl::get<RedisModuleString *>(identifier_);
  }

 private:
  absl::variant<RedisModuleString *, vmsdk::UniqueRedisString> identifier_;
};

using RecordsMap = absl::flat_hash_map<absl::string_view, RecordsMapValue>;

class AttributeDataType {
 public:
  virtual ~AttributeDataType() = default;
  virtual absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const = 0;
  virtual int GetRedisEventTypes() const {
    return REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_EXPIRED |
           REDISMODULE_NOTIFY_EVICTED;
  };
  virtual absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const = 0;
  virtual data_model::AttributeDataType ToProto() const = 0;
  virtual std::string ToString() const = 0;
  virtual bool IsProperType(RedisModuleKey *key) const = 0;
  // This provides indication wether the fetched content need special
  // normalization.
  virtual bool RecordsProvidedAsString() const = 0;
};

class HashAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetRedisEventTypes() const override {
    return REDISMODULE_NOTIFY_HASH | AttributeDataType::GetRedisEventTypes();
  }

  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
  }
  inline std::string ToString() const override { return "HASH"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(RedisModuleKey *key) const override {
    return RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_HASH;
  }
  bool RecordsProvidedAsString() const override { return false; }
};

inline constexpr absl::string_view kJsonCmd = "JSON.GET";
inline constexpr absl::string_view kJsonRootElementQuery = "$";

class JsonAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetRedisEventTypes() const override {
    return REDISMODULE_NOTIFY_MODULE | AttributeDataType::GetRedisEventTypes();
  }
  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON;
  }
  inline std::string ToString() const override { return "JSON"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(RedisModuleKey *key) const override {
    return RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_MODULE;
  }
  bool RecordsProvidedAsString() const override { return true; }
};

bool IsJsonModuleLoaded(RedisModuleCtx *ctx);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
