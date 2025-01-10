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

#include "src/server_events.h"

#include <cstdint>
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "src/coordinator/metadata_manager.h"

namespace valkey_search::server_events {

void OnForkChildCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                      uint64_t subevent, void *data) {
  ValkeySearch::Instance().OnForkChildCallback(ctx, eid, subevent, data);
}

void OnFlushDBCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                    uint64_t subevent, void *data) {
  SchemaManager::Instance().OnFlushDBCallback(ctx, eid, subevent, data);
}

void OnPersistenceCallback(RedisModuleCtx *ctx,
                                        RedisModuleEvent eid, uint64_t subevent,
                                        void *data) {
  SchemaManager::Instance().OnPersistenceCallback(ctx, eid, subevent, data);
}

void OnLoadingCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                    uint64_t subevent, void *data) {
  SchemaManager::Instance().OnLoadingCallback(ctx, eid, subevent, data);
  if (coordinator::MetadataManager::IsInitialized()) {
    coordinator::MetadataManager::Instance().OnLoadingCallback(
        ctx, eid, subevent, data);
  }
}

void OnSwapDBCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                                   uint64_t subevent, void *data) {
  SchemaManager::Instance().OnSwapDB((RedisModuleSwapDbInfo *)data);
}

void OnServerCronCallback(RedisModuleCtx *ctx,
                                       RedisModuleEvent eid, uint64_t subevent,
                                       void *data) {
  ValkeySearch::Instance().OnServerCronCallback(ctx, eid, subevent, data);
  SchemaManager::Instance().OnServerCronCallback(ctx, eid, subevent, data);
  if (coordinator::MetadataManager::IsInitialized()) {
    coordinator::MetadataManager::Instance().OnServerCronCallback(
        ctx, eid, subevent, data);
  }
}

void AtForkPrepare() { ValkeySearch::Instance().AtForkPrepare(); }

void AfterForkParent() {
  ValkeySearch::Instance().AfterForkParent();
}

void SubscribeToServerEvents() {
  // Note: all the events are subscribed to here. The engine only supports
  // a single subscriber per each event, so we centralize it to this file to
  // prevent unexpected behavior. If multiple events are subscribed to in
  // different places, the engine will only call the callback for the first
  // subscriber and the other will silently be ignored.
  RedisModuleCtx *ctx = ValkeySearch::Instance().GetBackgroundCtx();
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_CronLoop,
                                     &OnServerCronCallback);
  pthread_atfork(AtForkPrepare, AfterForkParent, nullptr);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ForkChild,
                                     &OnForkChildCallback);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence,
                                     &OnPersistenceCallback);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading,
                                     &OnLoadingCallback);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB,
                                     &OnSwapDBCallback);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB,
                                     &OnFlushDBCallback);
}

}  // namespace valkey_search::server_events
