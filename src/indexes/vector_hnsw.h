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

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_io_stream.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

template <typename T>
class VectorHNSW : public VectorBase {
 public:
  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> Create(
      const data_model::VectorIndex& vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> LoadFromRDB(
      RedisModuleCtx* ctx, const AttributeDataType* attribute_data_type,
      const data_model::VectorIndex& vector_index_proto,
      RDBInputStream& rdb_stream,
      absl::string_view attribute_identifier) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorHNSW() override = default;
  size_t GetDataTypeSize() const override { return sizeof(T); }

  const hnswlib::SpaceInterface<float>* GetSpace() const {
    return space_.get();
  }

  int GetDimensions() const { return dimensions_; }
  size_t GetCapacity() const override
      ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->max_elements_;
  }
  int GetM() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->M_;
  }
  int GetEfConstruction() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->ef_construction_;
  }
  size_t GetEfRuntime() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->ef_;
  }
  // Used just for testing
  void SetBlockSize(uint32_t block_size) { block_size_ = block_size; }
  absl::StatusOr<std::deque<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt)
      ABSL_LOCKS_EXCLUDED(resize_mutex_);

 protected:
  absl::Status ResizeIfFull() ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status AddRecordImpl(uint64_t internal_id,
                             absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);

  absl::Status RemoveRecordImpl(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::StatusOr<bool> ModifyRecordImpl(uint64_t internal_id,
                                        absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  void ToProtoImpl(data_model::VectorIndex* vector_index_proto) const override;
  int RespondWithInfoImpl(RedisModuleCtx* ctx) const override;
  absl::Status SaveIndexImpl(RDBOutputStream& rdb_stream) const override;
  absl::StatusOr<std::pair<float, hnswlib::labeltype>>
  ComputeDistanceFromRecordImpl(uint64_t internal_id, absl::string_view query)
      const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  char* GetValueImpl(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->getPoint(internal_id);
  }
  char* TrackVector(uint64_t internal_id,
                    const InternedStringPtr& vector) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);
  void UnTrackVector(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);

 private:
  VectorHNSW(int dimensions, absl::string_view attribute_identifier,
             data_model::AttributeDataType attribute_data_type);
  std::unique_ptr<hnswlib::HierarchicalNSW<T>> algo_
      ABSL_GUARDED_BY(resize_mutex_);
  std::unique_ptr<hnswlib::SpaceInterface<T>> space_;
  uint32_t block_size_{kHNSWBlockSize};
  mutable absl::Mutex resize_mutex_;
  mutable absl::Mutex tracked_vectors_mutex_;
  std::deque<InternedStringPtr> tracked_vectors_
      ABSL_GUARDED_BY(tracked_vectors_mutex_);
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
