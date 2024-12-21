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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_

#include <cstdint>
#include <memory>
#include <utility>

#include "grpcpp/server.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/server_callback.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::coordinator {

class Service final : public Coordinator::CallbackService {
 public:
  Service(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
          vmsdk::ThreadPool* reader_thread_pool,
          vmsdk::ThreadPool* writer_thread_pool)
      : detached_ctx_(std::move(detached_ctx)),
        reader_thread_pool_(reader_thread_pool),
        writer_thread_pool_(writer_thread_pool) {}
  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  ~Service() = default;

  grpc::ServerUnaryReactor* GetGlobalMetadata(
      grpc::CallbackServerContext* context,
      const GetGlobalMetadataRequest* request,
      GetGlobalMetadataResponse* response) override;

  grpc::ServerUnaryReactor* SearchIndexPartition(
      grpc::CallbackServerContext* context,
      const SearchIndexPartitionRequest* request,
      SearchIndexPartitionResponse* response) override;

 private:
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  vmsdk::ThreadPool* reader_thread_pool_;
  vmsdk::ThreadPool* writer_thread_pool_;
};

class Server {
 public:
  virtual uint16_t GetPort() const = 0;
  virtual ~Server() = default;
};

class ServerImpl final : public Server {
 public:
  static std::unique_ptr<Server> Create(RedisModuleCtx* ctx,
                                        vmsdk::ThreadPool* reader_thread_pool,
                                        vmsdk::ThreadPool* writer_thread_pool,
                                        uint16_t port);
  ServerImpl(const ServerImpl&) = delete;
  ServerImpl& operator=(const ServerImpl&) = delete;
  uint16_t GetPort() const override { return port_; }

 private:
  ServerImpl(std::unique_ptr<Service> coordinator_service,
             std::unique_ptr<grpc::Server> server, uint16_t port);

  std::unique_ptr<Service> coordinator_service_{nullptr};
  std::unique_ptr<grpc::Server> server_{nullptr};
  uint16_t port_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_
