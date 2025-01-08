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

#ifndef VALKEYSEARCH_SRC_VALKEY_SEARCH_H_
#define VALKEYSEARCH_SRC_VALKEY_SEARCH_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/server.h"
#include "src/index_schema.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

namespace server_events {
  extern void SubscribeToServerEvents();
}

class ValkeySearch {
 public:
  ValkeySearch() = default;
  virtual ~ValkeySearch() = default;

  bool SupportParralelQueries() const {
    return reader_thread_pool_ && reader_thread_pool_->Size() > 0;
  }

  vmsdk::ThreadPool *GetReaderThreadPool() const {
    return reader_thread_pool_.get();
  }
  vmsdk::ThreadPool *GetWriterThreadPool() const {
    return writer_thread_pool_.get();
  }
  void Info(RedisModuleInfoCtx *ctx) const;

  IndexSchema::Stats::ResultCnt<uint64_t> AccumulateIndexSchemaResults(
      absl::AnyInvocable<const IndexSchema::Stats::ResultCnt<
          std::atomic<uint64_t>> &(const IndexSchema::Stats &) const>
          get_result_cnt_func) const;
  void OnServerCronCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                            uint64_t subevent, void *data);
  void OnForkChildCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                           uint64_t subevent, void *data);
  void OnClusterMessageCallback(RedisModuleCtx *ctx, const char *sender_id,
                                uint8_t type, const unsigned char *payload,
                                uint32_t len);
  void SendMetadataBroadcast(RedisModuleCtx *ctx, void *data);
  void AtForkPrepare();
  void AfterForkParent();
  static ValkeySearch &Instance();
  static void InitInstance(std::unique_ptr<ValkeySearch> instance);

  absl::Status OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
  void OnUnload(RedisModuleCtx *ctx);
  friend absl::NoDestructor<ValkeySearch>;

  bool UsingCoordinator() const { return coordinator_ != nullptr; }
  bool IsCluster() const {
    return RedisModule_GetContextFlags(GetBackgroundCtx()) &
           REDISMODULE_CTX_FLAGS_CLUSTER;
  }

  coordinator::ClientPool *GetCoordinatorClientPool() const {
    return client_pool_.get();
  }
  void SetCoordinatorClientPool(
      std::unique_ptr<coordinator::ClientPool> client_pool) {
    client_pool_ = std::move(client_pool);
  }
  coordinator::Server *GetCoordinatorServer() const {
    return coordinator_.get();
  }
  void SetCoordinatorServer(std::unique_ptr<coordinator::Server> server) {
    coordinator_ = std::move(server);
  }
  // GetBackgroundCtx returns a RedisModuleCtx that is valid for the scope of
  // the module lifetime. Getting this context should be safe for the duration
  // of the program.
  RedisModuleCtx *GetBackgroundCtx() const { return ctx_; }

 protected:
  std::unique_ptr<vmsdk::ThreadPool> reader_thread_pool_;
  std::unique_ptr<vmsdk::ThreadPool> writer_thread_pool_;
  virtual size_t GetMaxWorkerThreadPoolSuspensionSec() const;

 private:
  absl::Status LoadOptions(RedisModuleCtx *ctx, RedisModuleString **argv,
                           int argc);
  static void *RDBLoad(RedisModuleIO *rdb, int encoding_version);
  static absl::StatusOr<void *> _RDBLoad(RedisModuleIO *rdb,
                                         int encoding_version);
  static void FreeIndexSchema(void *value);
  static bool IsChildProcess();
  void ProcessIndexSchemaBackfill(RedisModuleCtx *ctx, uint32_t batch_size);

  void ResumeWriterThreadPool(RedisModuleCtx *ctx, bool is_expired);

  uint64_t inc_id_{0};
  RedisModuleCtx *ctx_{nullptr};
  std::optional<vmsdk::StopWatch> writer_thread_pool_suspend_watch_;

  std::unique_ptr<coordinator::Server> coordinator_;
  std::unique_ptr<coordinator::ClientPool> client_pool_;
};
void ModuleInfo(RedisModuleInfoCtx *ctx, int for_crash_report);
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_VALKEY_SEARCH_H_
