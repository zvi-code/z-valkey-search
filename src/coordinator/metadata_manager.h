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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "google/protobuf/any.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "highwayhash/hh_types.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::coordinator {

using FingerprintCallback = absl::AnyInvocable<absl::StatusOr<uint64_t>(
    const google::protobuf::Any &metadata)>;
using MetadataUpdateCallback = absl::AnyInvocable<absl::Status(
    absl::string_view, const google::protobuf::Any *metadata)>;
using AuxSaveCallback = void (*)(RedisModuleIO *rdb, int when);
using AuxLoadCallback = int (*)(RedisModuleIO *rdb, int encver, int when);
static constexpr int kEncodingVersion = 0;
static constexpr absl::string_view kMetadataManagerModuleTypeName{"MtdMgr-VS"};
static constexpr uint8_t kMetadataBroadcastClusterMessageReceiverId = 0x00;

// Randomly generated 32 bit key for fingerprinting the metadata.
static constexpr highwayhash::HHKey kHashKey{
    0x9736bad976c904ea, 0x08f963a1a52eece9, 0x1ea3f3f773f3b510,
    0x9290a6b4e4db3d51};

class MetadataManager {
 public:
  MetadataManager(RedisModuleCtx *ctx, ClientPool &client_pool)
      : client_pool_(client_pool),
        detached_ctx_(vmsdk::MakeUniqueRedisDetachedThreadSafeContext(ctx)) {}

  static uint64_t ComputeTopLevelFingerprint(
      const google::protobuf::Map<std::string, GlobalMetadataEntryMap>
          &type_namespace_map);

  absl::Status TriggerCallbacks(absl::string_view type_name,
                                absl::string_view id,
                                const GlobalMetadataEntry &entry);

  absl::StatusOr<google::protobuf::Any> GetEntry(absl::string_view type_name,
                                                 absl::string_view id);

  absl::Status CreateEntry(absl::string_view type_name, absl::string_view id,
                           std::unique_ptr<google::protobuf::Any> contents);

  absl::Status DeleteEntry(absl::string_view type_name, absl::string_view id);

  std::unique_ptr<GlobalMetadata> GetGlobalMetadata();

  // RegisterType is used to register a new metadata type in the metadata
  // manager. After registering a type, the metadata manager will be able to
  // accept updates to that type both locally and over the cluster bus.
  //
  // * type_name should be a unique string identifying the type.
  // * encoding_version should be bumped any time the underlying metadata format
  // is changed.
  // * fingerprint_callback should be a function for computing the fingerprint
  // of the metadata for the given encoding version. This function can only
  // change when the encoding version is bumped.
  // * update_callback will be called whenever the metadata is updated.
  void RegisterType(absl::string_view type_name, uint32_t encoding_version,
                    FingerprintCallback fingerprint_callback,
                    MetadataUpdateCallback callback);

  void BroadcastMetadata(RedisModuleCtx *ctx);

  void BroadcastMetadata(RedisModuleCtx *ctx,
                         const GlobalMetadataVersionHeader &version_header);

  void HandleClusterMessage(RedisModuleCtx *ctx, const char *sender_id,
                            uint8_t type, const unsigned char *payload,
                            uint32_t len);

  void HandleBroadcastedMetadata(
      RedisModuleCtx *ctx, const char *sender_id,
      std::unique_ptr<GlobalMetadataVersionHeader> header);

  absl::Status ReconcileMetadata(const GlobalMetadata &proposed,
                                 bool trigger_callbacks = true,
                                 bool prefer_incoming = false);

  void OnServerCronCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                            uint64_t subevent, void *data);

  void OnLoadingEnded(RedisModuleCtx *ctx);
  void OnLoadingStarted(RedisModuleCtx *ctx);
  void OnReplicationLoadStart(RedisModuleCtx *ctx);
  void OnLoadingCallback(RedisModuleCtx *ctx, RedisModuleEvent eid,
                         uint64_t subevent, void *data);

  void AuxSave(RedisModuleIO *rdb, int when);
  absl::Status AuxLoad(RedisModuleIO *rdb, int encver, int when);
  absl::Status RegisterModuleType(RedisModuleCtx *ctx);
  void RegisterForClusterMessages(RedisModuleCtx *ctx);

  static bool IsInitialized();
  static void InitInstance(std::unique_ptr<MetadataManager> instance);
  static MetadataManager &Instance();

 protected:
  RedisModuleType *module_type_;

 private:
  struct RegisteredType {
    uint32_t encoding_version;
    FingerprintCallback fingerprint_callback;
    MetadataUpdateCallback update_callback;
  };
  absl::StatusOr<uint64_t> ComputeFingerprint(
      absl::string_view type_name, const google::protobuf::Any &contents,
      absl::flat_hash_map<std::string, RegisteredType> &registered_types);
  vmsdk::MainThreadAccessGuard<GlobalMetadata> metadata_;
  vmsdk::MainThreadAccessGuard<GlobalMetadata> staged_metadata_;
  vmsdk::MainThreadAccessGuard<bool> staging_metadata_due_to_repl_load_ = false;
  vmsdk::MainThreadAccessGuard<bool> is_loading_ = false;
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<std::string, RegisteredType>>
      registered_types_;
  coordinator::ClientPool &client_pool_;
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
};
}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_METADATA_MANAGER_H_
