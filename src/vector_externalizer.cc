/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_externalizer.h"

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/utils/lru.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
VectorExternalizer::VectorExternalizer()
    : lru_cache_(std::make_unique<LRU<LRUCacheEntry>>(kLRUCapacity)) {}

std::vector<char> DenormalizeVector(absl::string_view record, size_t type_size,
                                    float magnitude) {
  std::vector<char> ret(record.size());
  if (type_size == sizeof(float)) {
    CopyAndDenormalizeEmbedding((float*)ret.data(), (float*)record.data(),
                                ret.size() / sizeof(float), magnitude);
    return ret;
  }
  CHECK(false) << "unsupported type size";
}

char* ExternalizeCB(void* cb_data, size_t* len) {
  vmsdk::VerifyMainThread();
  VectorExternalizer::Instance().ExternalizeCBCalled();
  auto vector_externalizer_entry =
      static_cast<VectorExternalizer::VectorExternalizerEntry*>(cb_data);
  if (vector_externalizer_entry->cache_normalized_) {
    *len =
        vector_externalizer_entry->cache_normalized_->normalized_vector.size();
    auto ptr =
        &vector_externalizer_entry->cache_normalized_->normalized_vector[0];
    VectorExternalizer::Instance().LRUPromote(
        vector_externalizer_entry->cache_normalized_.get());
    return (char*)ptr;
  }
  if (vector_externalizer_entry->magnitude.has_value()) {
    auto vector =
        DenormalizeVector(vector_externalizer_entry->vector->Str(),
                          sizeof(float), *vector_externalizer_entry->magnitude);
    vector_externalizer_entry->cache_normalized_ =
        std::make_unique<VectorExternalizer::LRUCacheEntry>(
            std::move(vector), vector_externalizer_entry);
    *len =
        vector_externalizer_entry->cache_normalized_->normalized_vector.size();
    auto ptr =
        vector_externalizer_entry->cache_normalized_->normalized_vector.data();
    auto lru_removed = VectorExternalizer::Instance().LRUAdd(
        vector_externalizer_entry->cache_normalized_.get());
    if (lru_removed) {
      lru_removed->entry->cache_normalized_ = nullptr;
    }
    return (char*)ptr;
  }
  auto vector = vector_externalizer_entry->vector->Str();
  *len = vector.size();
  return (char*)vector.data();
}

void VectorExternalizer::LRURemove(VectorExternalizer::LRUCacheEntry* entry) {
  lru_cache_.Get()->Remove(entry);
}

VectorExternalizer::LRUCacheEntry* VectorExternalizer::LRUAdd(
    VectorExternalizer::LRUCacheEntry* entry) {
  CHECK(entry->next == nullptr && entry->prev == nullptr);
  return lru_cache_.Get()->InsertAtTop(entry);
}

void VectorExternalizer::LRUPromote(VectorExternalizer::LRUCacheEntry* entry) {
  ++stats_.Get().lru_promote_cnt;
  lru_cache_.Get()->Promote(entry);
}

void VectorExternalizer::Init(ValkeyModuleCtx* ctx) {
  hash_registration_supported_ =
      (ValkeyModule_GetApi("ValkeyModule_HashExternalize",
                           (void**)&ValkeyModule_HashExternalize) ==
       VALKEYMODULE_OK);
  ctx_ = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
}

VectorExternalizer::LRUCacheEntry::~LRUCacheEntry() {
  VectorExternalizer::Instance().LRURemove(this);
}

bool VectorExternalizer::Externalize(
    const InternedStringPtr& key, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type,
    const InternedStringPtr& vector, std::optional<float> magnitude) {
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  // Defer updating the engine until the key has been processed for all indexes.
  // This ensures that consecutive reads of the record do not lose precision due
  // to vector denormalization.
  auto& deferred_shared_vectors = deferred_shared_vectors_.Get();
  VectorExternalizerEntry entry = {vector, magnitude};
  auto result = deferred_shared_vectors[key].emplace(attribute_identifier,
                                                     std::move(entry));
  if (!result.second) {
    // To maintain precision and reduce denormalization overhead, prefer
    // externalizing the unnormalized vector, if available.
    if (result.first->second.magnitude != std::nullopt) {
      VectorExternalizerEntry tmp = {vector, magnitude};
      result.first->second = std::move(tmp);
    }
  }
  return true;
}

void VectorExternalizer::ProcessEngineUpdateQueue() {
  if (!hash_registration_supported_) {
    return;
  }
  auto& deferred_shared_vectors = deferred_shared_vectors_.Get();
  auto& shared_vectors = shared_vectors_.Get();
  for (auto& [key, attribute_identifiers] : deferred_shared_vectors) {
    vmsdk::UniqueValkeyOpenKey key_obj;
    for (auto& [attribute_identifier, vector_externalizer_entry] :
         attribute_identifiers) {
      auto it = shared_vectors[key].find(attribute_identifier);
      if (it != shared_vectors[key].end()) {
        it->second.magnitude = vector_externalizer_entry.magnitude;
        it->second.vector = std::move(vector_externalizer_entry.vector);
        it->second.cache_normalized_ = nullptr;
        continue;
      }
      auto& entry = shared_vectors[key][attribute_identifier];
      entry.magnitude = vector_externalizer_entry.magnitude;
      entry.vector = std::move(vector_externalizer_entry.vector);
      if (!key_obj) {
        auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
        key_obj = vmsdk::MakeUniqueValkeyOpenKey(
            ctx_.Get().get(), key_str.get(), VALKEYMODULE_WRITE);
        if (!key_obj) {
          break;
        }
      }

      if (ValkeyModule_HashExternalize(
              key_obj.get(),
              vmsdk::MakeUniqueValkeyString(attribute_identifier).get(),
              ExternalizeCB, &entry) != VALKEYMODULE_OK) {
        shared_vectors[key].erase(attribute_identifier);
        ++stats_.Get().hash_extern_errors;
      }
    }
  }
  deferred_shared_vectors.clear();
}

void VectorExternalizer::Remove(
    const InternedStringPtr& key, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type) {
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return;
  }
  shared_vectors_.Get()[key].erase(attribute_identifier);
  deferred_shared_vectors_.Get()[key].erase(attribute_identifier);
}

VectorExternalizer::Stats VectorExternalizer::GetStats() const {
  Stats ret = stats_.Get();
  ret.num_lru_entries = lru_cache_.Get()->Size();
  ret.entry_cnt = EntriesCnt();
  ret.deferred_entry_cnt = PendingEntriesCnt();
  return ret;
}

size_t VectorExternalizer::EntriesCnt() const {
  size_t size = 0;
  auto& deferred_shared_vectors = deferred_shared_vectors_.Get();
  auto& shared_vectors = shared_vectors_.Get();
  for (const auto& keys_it : shared_vectors) {
    size += keys_it.second.size();
  }
  for (auto& [key, attribute_identifiers] : deferred_shared_vectors) {
    auto it = shared_vectors.find(key);
    if (it == shared_vectors.end()) {
      size += attribute_identifiers.size();
    } else {
      for (auto& [attribute_identifier, _] : attribute_identifiers) {
        if (!it->second.contains(attribute_identifier)) {
          ++size;
        }
      }
    }
  }
  return size;
}

size_t VectorExternalizer::PendingEntriesCnt() const {
  size_t size = 0;
  auto& deferred_shared_vectors = deferred_shared_vectors_.Get();
  for (const auto& keys_it : deferred_shared_vectors) {
    size += keys_it.second.size();
  }
  return size;
}

vmsdk::UniqueValkeyString VectorExternalizer::GetRecord(
    ValkeyModuleCtx* ctx, const AttributeDataType* attribute_data_type,
    ValkeyModuleKey* key_obj, absl::string_view key_cstr,
    absl::string_view attribute_identifier, bool& is_module_owned) {
  vmsdk::VerifyMainThread();
  vmsdk::UniqueValkeyString record;
  is_module_owned = false;
  auto generated_value_cnt = stats_.Get().generated_value_cnt;
  auto res = attribute_data_type->GetRecord(ctx, key_obj, key_cstr,
                                            attribute_identifier);
  if (!res.ok()) {
    return nullptr;
  }
  if (generated_value_cnt != stats_.Get().generated_value_cnt) {
    is_module_owned = true;
  }
  return std::move(res.value());
}

void VectorExternalizer::Reset() {
  ctx_.Get().reset();
  stats_.Get() = Stats();
  shared_vectors_.Get().clear();
  deferred_shared_vectors_.Get().clear();
}

}  // namespace valkey_search
