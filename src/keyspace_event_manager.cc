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

#include "src/keyspace_event_manager.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
static absl::NoDestructor<std::unique_ptr<KeyspaceEventManager>>
    keyspace_event_manager_instance;

KeyspaceEventManager &KeyspaceEventManager::Instance() {
  return **keyspace_event_manager_instance;
}
void KeyspaceEventManager::InitInstance(
    std::unique_ptr<KeyspaceEventManager> instance) {
  *keyspace_event_manager_instance = std::move(instance);
}

void KeyspaceEventManager::NotifySubscribers(RedisModuleCtx *ctx, int type,
                                             const char *event,
                                             RedisModuleString *key) {
  if (ctx == VectorExternalizer::Instance().GetCtx()) {
    return;
  }
  std::vector<KeyspaceEventSubscription *> subscriptions_to_notify;
  {
    auto key_view = vmsdk::ToStringView(key);
    for (auto match_itr = subscription_trie_.Get().PathIterator(key_view);
         !match_itr.Done(); match_itr.Next()) {
      for (const auto &subscription : *match_itr.Value().value) {
        if (subscription->GetAttributeDataType().GetRedisEventTypes() & type) {
          subscriptions_to_notify.push_back(subscription);
        }
      }
    }
  }
  for (const auto &subscription : subscriptions_to_notify) {
    subscription->OnKeyspaceNotification(ctx, type, event, key);
  }
  VectorExternalizer::Instance().ProcessEngineUpdateQueue();
}

absl::Status KeyspaceEventManager::RemoveSubscription(
    KeyspaceEventSubscription *subscription) {
  auto &subscriptions = subscriptions_.Get();
  if (!subscriptions.contains(subscription)) {
    return absl::NotFoundError("Subscription not found");
  }

  auto key_prefixes = subscription->GetKeyPrefixes();
  DCHECK(!key_prefixes.empty());
  auto &subscription_trie = subscription_trie_.Get();
  for (const auto &prefix : key_prefixes) {
    subscription_trie.Remove(prefix, subscription);
    // TODO[@jkmurphy] - we need to support unsubscribe to keyspace events
  }

  subscriptions.erase(subscription);
  return absl::OkStatus();
}

absl::Status KeyspaceEventManager::InsertSubscription(
    RedisModuleCtx *ctx, KeyspaceEventSubscription *subscription) {
  VMSDK_RETURN_IF_ERROR(StartRedisSubscribtionIfNeeded(
      ctx, subscription->GetAttributeDataType().GetRedisEventTypes()));

  auto key_prefixes = subscription->GetKeyPrefixes();
  CHECK(!key_prefixes.empty());
  auto &subscription_trie = subscription_trie_.Get();
  for (const auto &prefix : key_prefixes) {
    subscription_trie.AddKeyValue(prefix, subscription);
  }

  subscriptions_.Get().insert(subscription);
  return absl::OkStatus();
}

absl::Status KeyspaceEventManager::StartRedisSubscribtionIfNeeded(
    RedisModuleCtx *ctx, int types) {
  int to_subscribe = types & ~subscribed_types_bit_mask_;
  if (!to_subscribe) return absl::OkStatus();
  if (RedisModule_SubscribeToKeyspaceEvents(
          ctx, to_subscribe, OnRedisKeyspaceNotification) != REDISMODULE_OK) {
    return absl::InternalError("failed to subscribe to keyspace events");
  }
  subscribed_types_bit_mask_ |= types;

  return absl::OkStatus();
}

}  // namespace valkey_search
