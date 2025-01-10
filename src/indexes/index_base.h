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

#ifndef VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
#define VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/rdb_io_stream.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {
enum class IndexerType { kHNSW, kFlat, kNumeric, kTag, kVector, kNone };

enum class DeletionType {
  kRecord,      // The record was deleted from the index.
  kIdentifier,  // One or more fields of the record were deleted.
  kNone         // No deletion occurred.
};

const absl::NoDestructor<absl::flat_hash_map<absl::string_view, IndexerType>>
    kIndexerTypeByStr({{"VECTOR", IndexerType::kVector},
                       {"TAG", IndexerType::kTag},
                       {"NUMERIC", IndexerType::kNumeric}});

class IndexBase {
 public:
  explicit IndexBase(IndexerType indexer_type) : indexer_type_(indexer_type) {}
  virtual ~IndexBase() = default;

  // Add/Remove/Modify will return true if the operation was successful, false
  // if it was skipped.  Returns an error status if there is an unexpected
  // failure.
  virtual absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) = 0;
  virtual absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) = 0;
  virtual absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                            absl::string_view data) = 0;
  virtual int RespondWithInfo(RedisModuleCtx* ctx) const = 0;
  virtual bool IsTracked(const InternedStringPtr& key) const = 0;
  IndexerType GetIndexerType() const { return indexer_type_; }
  virtual absl::Status SaveIndex(RDBOutputStream& rdb_stream) const = 0;

  virtual std::unique_ptr<data_model::Index> ToProto() const = 0;
  virtual void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const {}

  virtual vmsdk::UniqueRedisString NormalizeStringRecord(
      vmsdk::UniqueRedisString input) const {
    return input;
  }
  virtual uint64_t GetRecordCount() const = 0;

 private:
  IndexerType indexer_type_{IndexerType::kNone};
};

class EntriesFetcherIteratorBase {
 public:
  virtual bool Done() const = 0;
  virtual void Next() = 0;
  virtual const InternedStringPtr& operator*() const = 0;
  virtual ~EntriesFetcherIteratorBase() = default;
};

class EntriesFetcherBase {
 public:
  virtual size_t Size() const = 0;
  virtual ~EntriesFetcherBase() = default;
  virtual std::unique_ptr<EntriesFetcherIteratorBase> Begin() = 0;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
