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

#ifndef VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
#define VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

using AuxSaveCallback = void (*)(RedisModuleIO *rdb, int when);
using AuxLoadCallback = int (*)(RedisModuleIO *rdb, int encver, int when);
constexpr absl::string_view kSchemaManagerModuleTypeName{"SchMgr-VS"};
constexpr absl::string_view kSchemaManagerMetadataTypeName{"vs_index_schema"};
constexpr uint32_t kMetadataEncodingVersion = 1;

class SchemaManager {
 public:
  SchemaManager(RedisModuleCtx *ctx,
                absl::AnyInvocable<void()> server_events_subscriber_callback,
                vmsdk::ThreadPool *mutations_thread_pool,
                bool coordinator_enabled);
  ~SchemaManager() = default;
  SchemaManager(const SchemaManager &) = delete;
  SchemaManager &operator=(const SchemaManager &) = delete;

  absl::Status CreateIndexSchema(
      RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status ImportIndexSchema(std::shared_ptr<IndexSchema> index_schema)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status RemoveIndexSchema(uint32_t db_num, absl::string_view name)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> GetIndexSchema(
      uint32_t db_num, absl::string_view name) const
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::flat_hash_set<std::string> GetIndexSchemasInDB(uint32_t db_num) const;
  // TODO(b/350755299) Investigate storing aggregated counters to optimize stats
  // generation.
  uint64_t GetNumberOfIndexSchemas() const;
  uint64_t GetNumberOfAttributes() const;
  uint64_t GetTotalIndexedHashKeys() const;
  bool IsIndexingInProgress() const;
  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;

  void OnFlushDBEnded(RedisModuleCtx *ctx);
  void OnSwapDB(RedisModuleSwapDbInfo *swap_db_info);
  void AuxSave(RedisModuleIO *rdb, int when);
  absl::Status AuxLoad(RedisModuleIO *rdb, int encver, int when);
  absl::Status RegisterModuleType(RedisModuleCtx *ctx);

  // These functions provide cross-compatibility with prior versions
  // TODO(b/349436336) Remove after rolled out.
  void OnSavingStarted(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnSavingEnded(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnLoadingEnded(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void OnReplicationLoadStart(RedisModuleCtx *ctx);

  void PerformBackfill(RedisModuleCtx *ctx, uint32_t batch_size)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnFlushDBCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                         uint64_t subevent, void *data)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void OnPersistenceCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                             uint64_t subevent, void *data);

  void OnLoadingCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                         uint64_t subevent, void *data);

  void OnServerCronCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                            uint64_t subevent, void *data);

  int OnAuxLoadCallback(RedisModuleIO *rdb, int encver, int when);
  absl::StatusOr<void *> IndexSchemaRDBLoad(RedisModuleIO *rdb,
                                            int encoding_version);

  static void InitInstance(std::unique_ptr<SchemaManager> instance);
  static SchemaManager &Instance();

 protected:
  RedisModuleType *schema_manager_module_type_;
  RedisModuleType *index_schema_module_type_;

 private:
  absl::Status RemoveAll()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::AnyInvocable<void()> server_events_subscriber_callback_;
  bool is_subscribed_to_server_events_ = false;
  vmsdk::ThreadPool *mutations_thread_pool_;
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;

  static absl::StatusOr<uint64_t> ComputeFingerprint(
      const google::protobuf::Any &metadata);
  absl::Status OnMetadataCallback(absl::string_view id,
                                  const google::protobuf::Any *metadata)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  absl::Status CreateIndexSchemaInternal(
      RedisModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  absl::StatusOr<std::shared_ptr<IndexSchema>> RemoveIndexSchemaInternal(
      uint32_t db_num, absl::string_view name)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  absl::StatusOr<IndexSchema *> StageIndexFromRDB(RedisModuleCtx *ctx,
                                                  RedisModuleIO *rdb,
                                                  int encver);
  absl::Status StageIndicesFromAux(RedisModuleCtx *ctx,
                                   int aux_index_schema_count,
                                   RedisModuleIO *rdb, int encver);

  absl::StatusOr<IndexSchema *> LoadIndexFromRDB(RedisModuleCtx *ctx,
                                                 RedisModuleIO *rdb, int encver)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  absl::Status LoadIndicesFromAux(RedisModuleCtx *ctx,
                                  int aux_index_schema_count,
                                  RedisModuleIO *rdb, int encver)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);

  void SubscribeToServerEventsIfNeeded();

  // GetIndexSchemasInDBInternal returns a set of strings representing the names
  // of the index schemas in the given DB. Note that the set of strings is
  // created through copying out the state at the time of the call. Due to this
  // copy - this should not be used in performance critical paths like
  // FT.SEARCH.
  absl::flat_hash_set<std::string> GetIndexSchemasInDBInternal(uint32_t db_num)
      const ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  void AddSchemasToKeyspace(RedisModuleCtx *ctx)
      ABSL_LOCKS_EXCLUDED(db_to_index_schemas_mutex_);
  void DeleteSchemasFromKeyspace(RedisModuleCtx *ctx)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);

  mutable absl::Mutex db_to_index_schemas_mutex_;
  absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>
      db_to_index_schemas_ ABSL_GUARDED_BY(db_to_index_schemas_mutex_);

  // Staged changes to index schemas, to be applied on loading ended.
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<
      uint32_t, absl::flat_hash_map<std::string, std::shared_ptr<IndexSchema>>>>
      staged_db_to_index_schemas_;
  absl::StatusOr<std::shared_ptr<IndexSchema>> LookupInternal(
      uint32_t db_num, absl::string_view name) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(db_to_index_schemas_mutex_);
  vmsdk::MainThreadAccessGuard<bool> staging_indices_due_to_repl_load_ = false;

  bool coordinator_enabled_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_SCHEMA_MANAGER_H_
