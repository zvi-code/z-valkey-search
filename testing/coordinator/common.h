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

#ifndef VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_
#define VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_

#include <cstdint>
#include <memory>

#include "gmock/gmock.h"
#include "absl/strings/string_view.h"
#include "src/coordinator/client.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/server.h"

namespace valkey_search::coordinator {

class MockClientPool : public ClientPool {
 public:
  MockClientPool() : ClientPool(nullptr) {}
  MOCK_METHOD(std::shared_ptr<Client>, GetClient, (absl::string_view address),
              (override));
};

class MockClient : public Client {
 public:
  MOCK_METHOD(void, GetGlobalMetadata, (GetGlobalMetadataCallback done),
              (override));
  MOCK_METHOD(void, SearchIndexPartition,
              (std::unique_ptr<SearchIndexPartitionRequest> request,
               SearchIndexPartitionCallback done),
              (override));
};

class MockServer : public Server {
 public:
  MOCK_METHOD(uint16_t, GetPort, (), (const, override));
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_
