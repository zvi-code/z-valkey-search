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

#include "src/attribute_data_type.h"

#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
absl::StatusOr<vmsdk::UniqueRedisString> HashAttributeDataType::GetRecord(
    [[maybe_unused]] RedisModuleCtx *ctx, RedisModuleKey *open_key,
    [[maybe_unused]] absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::VerifyMainThread();
  RedisModuleString *record{nullptr};
  RedisModule_HashGet(open_key, REDISMODULE_HASH_CFIELDS, identifier.data(),
                      &record, nullptr);
  if (!record) {
    return absl::NotFoundError("No such record with identifier");
  }
  return vmsdk::UniqueRedisString(record);
}

struct HashScanCallbackData {
  const absl::flat_hash_set<absl::string_view> &identifiers;
  RecordsMap key_value_content;
};

void HashScanCallback(RedisModuleKey *key, RedisModuleString *field,
                      RedisModuleString *value, void *privdata) {
  vmsdk::VerifyMainThread();
  if (!field || !value) {
    return;
  }
  HashScanCallbackData *callback_data = (HashScanCallbackData *)(privdata);
  auto field_str = vmsdk::ToStringView(field);
  if (field_str.empty()) {
    return;
  }
  if (callback_data->identifiers.empty() ||
      callback_data->identifiers.contains(field_str)) {
    callback_data->key_value_content.emplace(
        field_str, RecordsMapValue(vmsdk::RetainUniqueRedisString(field),
                                   vmsdk::RetainUniqueRedisString(value)));
  }
}

bool HashHasRecord(RedisModuleKey *key, absl::string_view identifier) {
  int exists;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS | REDISMODULE_HASH_EXISTS,
                      identifier.data(), &exists, nullptr);
  return exists;
}

absl::StatusOr<RecordsMap> HashAttributeDataType::FetchAllRecords(
    RedisModuleCtx *ctx, const std::string &vector_identifier,
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueRedisString(key);
  auto key_obj =
      vmsdk::MakeUniqueRedisOpenKey(ctx, key_str.get(), REDISMODULE_READ);
  if (!key_obj) {
    return absl::NotFoundError(
        absl::StrCat("No such record with key: `", vector_identifier, "`"));
  }
  if (!HashHasRecord(key_obj.get(), vector_identifier)) {
    return absl::NotFoundError(absl::StrCat("No such record with identifier: `",
                                            vector_identifier, "`"));
  }
  vmsdk::UniqueRedisScanCursor cursor = vmsdk::MakeUniqueRedisScanCursor();
  HashScanCallbackData callback_data{identifiers};
  while (RedisModule_ScanKey(key_obj.get(), cursor.get(), HashScanCallback,
                             &callback_data)) {
  }
  return std::move(callback_data.key_value_content);
}

absl::StatusOr<vmsdk::UniqueRedisString> JsonAttributeDataType::GetRecord(
    RedisModuleCtx *ctx, [[maybe_unused]] RedisModuleKey *open_key,
    absl::string_view key, absl::string_view identifier) const {
  vmsdk::VerifyMainThread();

  auto reply = vmsdk::UniquePtrRedisCallReply(RedisModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), identifier.data()));
  if (reply == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("No such record with identifier: `", identifier, "`"));
  }
  auto reply_type = RedisModule_CallReplyType(reply.get());
  if (reply_type == REDISMODULE_REPLY_STRING) {
    return vmsdk::UniqueRedisString(
        RedisModule_CreateStringFromCallReply(reply.get()));
  }
  return absl::NotFoundError("Json.get returned a non string value");
}

absl::StatusOr<RecordsMap> JsonAttributeDataType::FetchAllRecords(
    RedisModuleCtx *ctx, const std::string &vector_identifier,
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  // Validating that the a JSON object with the key exists with the vector
  // identifier
  auto reply = vmsdk::UniquePtrRedisCallReply(RedisModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), vector_identifier.c_str()));
  if (reply == nullptr ||
      RedisModule_CallReplyType(reply.get()) != REDISMODULE_REPLY_STRING) {
    return absl::NotFoundError(absl::StrCat("No such record with identifier: `",
                                            vector_identifier, "`"));
  }
  RecordsMap key_value_content;
  for (const auto &identifier : identifiers) {
    auto str = GetRecord(ctx, nullptr, key, identifier);
    if (!str.ok()) {
      continue;
    }
    key_value_content.emplace(
        identifier, RecordsMapValue(vmsdk::MakeUniqueRedisString(identifier),
                                    std::move(str.value())));
  }
  return key_value_content;
}

bool IsJsonModuleLoaded(RedisModuleCtx *ctx) {
  static std::optional<bool> json_module_loaded;
  if (!json_module_loaded.has_value() ||
      (!json_module_loaded.value() && vmsdk::IsMainThread())) {
    vmsdk::VerifyMainThread();
    auto reply = vmsdk::UniquePtrRedisCallReply(
        RedisModule_Call(ctx, kJsonCmd.data(), "cc", "nonexistentkey", "."));
    json_module_loaded =
        (reply != nullptr &&
         RedisModule_CallReplyType(reply.get()) != REDISMODULE_REPLY_ERROR);
  }
  return json_module_loaded.value();
}
}  // namespace valkey_search
