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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
#define VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/client.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

class ClientPool {
 public:
  ClientPool(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx)
      : detached_ctx_(std::move(detached_ctx)) {}
  virtual ~ClientPool() = default;

  virtual std::shared_ptr<Client> GetClient(absl::string_view address) {
    auto mutex = absl::MutexLock(&client_pool_mutex_);
    auto itr = client_pool_.find(address);
    if (itr == client_pool_.end()) {
      auto client = ClientImpl::MakeInsecureClient(
          vmsdk::MakeUniqueRedisDetachedThreadSafeContext(detached_ctx_.get()),
          address);
      client_pool_[address] = std::move(client);
    }
    return client_pool_[address];
  }

 private:
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  absl::Mutex client_pool_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<Client>> client_pool_
      ABSL_GUARDED_BY(client_pool_mutex_);
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
