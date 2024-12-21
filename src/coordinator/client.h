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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_
#define VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_

#include <memory>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

typedef absl::AnyInvocable<void(grpc::Status, GetGlobalMetadataResponse&)>
    GetGlobalMetadataCallback;
typedef absl::AnyInvocable<void(grpc::Status, SearchIndexPartitionResponse&)>
    SearchIndexPartitionCallback;

class Client {
 public:
  virtual ~Client() = default;
  virtual void GetGlobalMetadata(GetGlobalMetadataCallback done) = 0;
  virtual void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done) = 0;
};

class ClientImpl : public Client {
 public:
  ClientImpl(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
             absl::string_view address,
             std::unique_ptr<Coordinator::Stub> stub);
  static std::shared_ptr<Client> MakeInsecureClient(
      vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
      absl::string_view address);

  ClientImpl(const ClientImpl&) = delete;
  ClientImpl& operator=(const ClientImpl&) = delete;

  void GetGlobalMetadata(GetGlobalMetadataCallback done);
  void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done);

 private:
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  std::string address_;
  std::unique_ptr<Coordinator::Stub> stub_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_
