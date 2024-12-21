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

#ifndef VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_
#define VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "src/attribute_data_type.h"
#include "src/utils/patricia_tree.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

typedef std::function<absl::Status(RedisModuleCtx *ctx, int types)>
    StartSubscriptionFunction;

// KeyspaceEventSubscription is an interface for classes that want to subscribe
// to keyspace events.
class KeyspaceEventSubscription {
 public:
  virtual ~KeyspaceEventSubscription() = default;
  virtual const AttributeDataType &GetAttributeDataType() const = 0;

  // GetKeyPrefixes should return a vector containing >= 1 prefixes to subscribe
  // to. Note that this should not be empty. If no prefixes are needed, this
  // should still return a vector with the single value "".
  //
  // The value "*" will match the character "*". If matching all prefixes is
  // desired, "" should be used.
  //
  // The returned vector should contain no two strings A and B, such that A is
  // completely prefixed by B. Otherwise, duplicate events may fire.
  virtual const std::vector<std::string> &GetKeyPrefixes() const = 0;

  virtual void OnKeyspaceNotification(RedisModuleCtx *ctx, int type,
                                      const char *event,
                                      RedisModuleString *key) = 0;
};

class KeyspaceEventManager {
 public:
  KeyspaceEventManager()
      : subscriptions_(),
        subscription_trie_(/*case_sensitive=*/true),
        subscribed_types_bit_mask_(0) {};
  void NotifySubscribers(RedisModuleCtx *ctx, int type, const char *event,
                         RedisModuleString *key);

  absl::Status InsertSubscription(RedisModuleCtx *ctx,
                                  KeyspaceEventSubscription *subscription);

  absl::Status RemoveSubscription(KeyspaceEventSubscription *subscription);

  inline bool HasSubscription(KeyspaceEventSubscription *subscription) const {
    return subscriptions_.Get().contains(subscription);
  }
  static void InitInstance(std::unique_ptr<KeyspaceEventManager> instance);
  static KeyspaceEventManager &Instance();

 private:
  absl::Status StartRedisSubscribtionIfNeeded(RedisModuleCtx *ctx, int types);

  static inline int OnRedisKeyspaceNotification(RedisModuleCtx *ctx, int type,
                                                const char *event,
                                                RedisModuleString *key) {
    Instance().NotifySubscribers(ctx, type, event, key);
    return REDISMODULE_OK;
  }

  vmsdk::MainThreadAccessGuard<absl::flat_hash_set<KeyspaceEventSubscription *>>
      subscriptions_;
  // TODO: b/355561165 - Migrate to PatriciaTreeSet
  vmsdk::MainThreadAccessGuard<PatriciaTree<KeyspaceEventSubscription *>>
      subscription_trie_;
  int subscribed_types_bit_mask_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_
