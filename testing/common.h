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

#ifndef VALKEYSEARCH_TESTING_COMMON_H_
#define VALKEYSEARCH_TESTING_COMMON_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/hnswlib/iostream.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
#include "src/query/search.h"
#include "src/rdb_io_stream.h"
#include "src/schema_manager.h"
#include "src/server_events.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search {
template <typename T, typename K>
class IndexTeser : public T {
 public:
  explicit IndexTeser(K proto) : T(K(proto)) {}
  absl::StatusOr<bool> AddRecord(absl::string_view key,
                                 absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    return T::AddRecord(interned_key, data);
  }
  absl::StatusOr<bool> RemoveRecord(
      absl::string_view key,
      indexes::DeletionType deletion_type = indexes::DeletionType::kNone) {
    auto interned_key = StringInternStore::Intern(key);
    return T::RemoveRecord(interned_key, deletion_type);
  }
  absl::StatusOr<bool> ModifyRecord(absl::string_view key,
                                    absl::string_view data) {
    auto interned_key = StringInternStore::Intern(key);
    return T::ModifyRecord(interned_key, data);
  }
  bool IsTracked(absl::string_view key) const {
    auto interned_key = StringInternStore::Intern(key);
    return T::IsTracked(interned_key);
  }
};

class MockIndex : public indexes::IndexBase {
 public:
  MockIndex() : indexes::IndexBase(indexes::IndexerType::kNone) {}
  MOCK_METHOD(absl::StatusOr<bool>, AddRecord,
              (const InternedStringPtr& key, absl::string_view data),
              (override));
  MOCK_METHOD(absl::StatusOr<bool>, RemoveRecord,
              (const InternedStringPtr& key,
               indexes::DeletionType deletion_type),
              (override));
  MOCK_METHOD(absl::StatusOr<bool>, ModifyRecord,
              (const InternedStringPtr& key, absl::string_view data),
              (override));
  MOCK_METHOD(bool, IsTracked, (const InternedStringPtr& key),
              (const, override));
  MOCK_METHOD(std::unique_ptr<data_model::Index>, ToProto, (),
              (const, override));
  MOCK_METHOD(int, RespondWithInfo, (RedisModuleCtx * ctx), (const, override));
  MOCK_METHOD(absl::Status, SaveIndex, (RDBOutputStream & rdb_stream),
              (const, override));
  MOCK_METHOD((void), ForEachTrackedKey,
              (absl::AnyInvocable<void(const InternedStringPtr& key)> fn),
              (const, override));
  MOCK_METHOD((uint64_t), GetRecordCount, (), (const, override));
};

class MockKeyspaceEventSubscription : public KeyspaceEventSubscription {
 public:
  MOCK_METHOD(AttributeDataType&, GetAttributeDataType, (), (override, const));
  MOCK_METHOD(const std::vector<std::string>&, GetKeyPrefixes, (),
              (override, const));
  MOCK_METHOD(void, OnKeyspaceNotification,
              (RedisModuleCtx * ctx, int type, const char* event,
               RedisModuleString* key),
              (override));
};

class MockAttributeDataType : public AttributeDataType {
 public:
  MOCK_METHOD(absl::StatusOr<vmsdk::UniqueRedisString>, GetRecord,
              (RedisModuleCtx * ctx, RedisModuleKey* open_key,
               absl::string_view key, absl::string_view identifier),
              (override, const));
  MOCK_METHOD(int, GetRedisEventTypes, (), (override, const));
  MOCK_METHOD((absl::StatusOr<RecordsMap>), FetchAllRecords,
              (RedisModuleCtx * ctx, const std::string& query_attribute_name,
               absl::string_view key,
               const absl::flat_hash_set<absl::string_view>& identifiers),
              (override, const));
  MOCK_METHOD((data_model::AttributeDataType), ToProto, (), (override, const));
  MOCK_METHOD((std::string), ToString, (), (override, const));
  MOCK_METHOD((bool), IsProperType, (RedisModuleKey * key), (override, const));
  MOCK_METHOD(bool, RecordsProvidedAsString, (), (override, const));
};

class FakeRDBIOStream : public RDBInputStream, public RDBOutputStream {
 public:
  FakeRDBIOStream() {}
  FakeRDBIOStream(unsigned char dump_rdb[], size_t len) {
    buffer_.write((const char*)dump_rdb, len);
  }
  ~FakeRDBIOStream() override = default;
  absl::Status loadSizeT(size_t& val) override { return loadPOD(val); }
  absl::Status loadUnsigned(unsigned int& val) override { return loadPOD(val); }
  absl::Status loadSigned(int& val) override { return loadPOD(val); }
  absl::Status loadDouble(double& val) override { return loadPOD(val); }

  absl::StatusOr<hnswlib::StringBufferUniquePtr> loadStringBuffer(
      const size_t len) override {
    size_t _len;
    VMSDK_EXPECT_OK(loadPOD(_len));
    EXPECT_EQ(_len, len);
    auto str = hnswlib::MakeStringBufferUniquePtr(len);
    buffer_.read(str.get(), len);
    EXPECT_TRUE(buffer_);
    return str;
  }

  absl::StatusOr<vmsdk::UniqueRedisString> loadString() override {
    size_t len;
    VMSDK_EXPECT_OK(loadPOD(len));
    auto _str = std::make_unique<char[]>(len);
    buffer_.read(_str.get(), len);
    EXPECT_TRUE(buffer_);
    auto str = vmsdk::UniqueRedisString(
        RedisModule_CreateString(nullptr, _str.get(), len));
    return str;
  }

  absl::Status saveSizeT(size_t val) override { return savePOD(val); }
  absl::Status saveUnsigned(unsigned int val) override { return savePOD(val); }
  absl::Status saveSigned(int val) override { return savePOD(val); }
  absl::Status saveDouble(double val) override { return savePOD(val); }

  absl::Status saveStringBuffer(const char* str, size_t len) override {
    VMSDK_EXPECT_OK(savePOD(len));
    buffer_.write(str, len);
    EXPECT_TRUE(buffer_);
    return absl::OkStatus();
  }

 private:
  template <typename T>
  absl::Status loadPOD(T& val) {
    buffer_.read((char*)&val, sizeof(T));
    EXPECT_TRUE(buffer_);
    return absl::OkStatus();
  }

  template <typename T>
  absl::Status savePOD(const T val) {
    buffer_.write((char*)&val, sizeof(T));
    EXPECT_TRUE(buffer_);
    return absl::OkStatus();
  }

  std::stringstream buffer_;
};

data_model::VectorIndex CreateHNSWVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    int m, int ef_construction, size_t ef_runtime);

data_model::VectorIndex CreateFlatVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    uint32_t block_size);

class MockIndexSchema : public IndexSchema {
 public:
  static absl::StatusOr<std::shared_ptr<MockIndexSchema>> Create(
      RedisModuleCtx* ctx, absl::string_view key,
      const std::vector<absl::string_view>& subscribed_key_prefixes,
      std::unique_ptr<AttributeDataType> attribute_data_type,
      RedisModuleType* module_type, vmsdk::ThreadPool* mutations_thread_pool) {
    data_model::IndexSchema index_schema_proto;
    index_schema_proto.set_name(std::string(key));
    index_schema_proto.mutable_subscribed_key_prefixes()->Add(
        subscribed_key_prefixes.begin(), subscribed_key_prefixes.end());
    auto res = std::shared_ptr<MockIndexSchema>(new MockIndexSchema(
        ctx, index_schema_proto, std::move(attribute_data_type), module_type,
        mutations_thread_pool));
    VMSDK_RETURN_IF_ERROR(res->Init(ctx));
    return res;
  }
  MockIndexSchema(RedisModuleCtx* ctx,
                  const data_model::IndexSchema& index_schema_proto,
                  std::unique_ptr<AttributeDataType> attribute_data_type,
                  RedisModuleType* module_type,
                  vmsdk::ThreadPool* mutations_thread_pool)
      : IndexSchema(ctx, index_schema_proto, std::move(attribute_data_type),
                    module_type, mutations_thread_pool) {
    ON_CALL(*this, OnLoadingEnded(testing::_))
        .WillByDefault(testing::Invoke([this](RedisModuleCtx* ctx) {
          return IndexSchema::OnLoadingEnded(ctx);
        }));
    ON_CALL(*this, OnSwapDB(testing::_))
        .WillByDefault(
            testing::Invoke([this](RedisModuleSwapDbInfo* swap_db_info) {
              return IndexSchema::OnSwapDB(swap_db_info);
            }));
    ON_CALL(*this, RDBSave(testing::_))
        .WillByDefault(testing::Invoke([this](RDBOutputStream& rdb_os) {
          return IndexSchema::RDBSave(rdb_os);
        }));
    ON_CALL(*this, GetIdentifier(testing::_))
        .WillByDefault(testing::Invoke([](absl::string_view attribute_name) {
          return std::string(attribute_name);
        }));
  }
  MOCK_METHOD(void, OnLoadingEnded, (RedisModuleCtx * ctx), (override));
  MOCK_METHOD(void, OnSwapDB, (RedisModuleSwapDbInfo * swap_db_info),
              (override));
  MOCK_METHOD(absl::Status, RDBSave, (RDBOutputStream & rdb_os),
              (const, override));
  MOCK_METHOD(absl::StatusOr<std::string>, GetIdentifier,
              (absl::string_view attribute_name), (const, override));
};

// TestableValkeySearch subclasses ValkeySearch and makes it creatable for
// testing purposes.
class TestableValkeySearch : public ValkeySearch {
 public:
  void InitThreadPools(std::optional<size_t> readers,
                       std::optional<size_t> writers);

  vmsdk::ThreadPool* GetWriterThreadPool() const {
    return writer_thread_pool_.get();
  }
  vmsdk::ThreadPool* GetReaderThreadPool() const {
    return reader_thread_pool_.get();
  }
  size_t GetMaxWorkerThreadPoolSuspensionSec() const override { return 1; }
};

class TestableSchemaManager : public SchemaManager {
 public:
  TestableSchemaManager(
      RedisModuleCtx* ctx,
      absl::AnyInvocable<void()> server_events_callback = []() {},
      vmsdk::ThreadPool* writer_thread_pool = nullptr,
      bool coordinator_enabled = false)
      : SchemaManager(ctx, std::move(server_events_callback),
                      writer_thread_pool, coordinator_enabled) {
    index_schema_module_type_ = GetFakeIndexSchemaModuleType();
    schema_manager_module_type_ = GetFakeSchemaManagerModuleType();
  }
  static RedisModuleType* GetFakeIndexSchemaModuleType() {
    static RedisModuleType* index_schema_module_type =
        (RedisModuleType*)0xBAADF00D;
    return index_schema_module_type;
  }
  static RedisModuleType* GetFakeSchemaManagerModuleType() {
    static RedisModuleType* schema_manager_module_type =
        (RedisModuleType*)0xBADF00D1;
    return schema_manager_module_type;
  }
};

class TestableMetadataManager : public coordinator::MetadataManager {
 public:
  TestableMetadataManager(RedisModuleCtx* ctx,
                          coordinator::ClientPool& client_pool)
      : coordinator::MetadataManager(ctx, client_pool) {
    module_type_ = GetFakeMetadataManagerModuleType();
  }
  static RedisModuleType* GetFakeMetadataManagerModuleType() {
    static RedisModuleType* metadata_manager_module_type =
        (RedisModuleType*)0xBADF00D2;
    return metadata_manager_module_type;
  }
};

inline void InitThreadPools(std::optional<size_t> readers,
                            std::optional<size_t> writers) {
  ((TestableValkeySearch*)&ValkeySearch::Instance())
      ->InitThreadPools(readers, writers);
}

absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateIndexSchema(
    std::string index_schema_key, RedisModuleCtx* fake_ctx = nullptr,
    vmsdk::ThreadPool* writer_thread_pool = nullptr,
    const std::vector<absl::string_view>* key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);
absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateVectorHNSWSchema(
    std::string index_schema_key, RedisModuleCtx* fake_ctx = nullptr,
    vmsdk::ThreadPool* writer_thread_pool = nullptr,
    const std::vector<absl::string_view>* key_prefixes = nullptr,
    int32_t index_schema_db_num = 0);

// TestableKeyspaceEventManager subclasses KeyspaceEventManager and makes it
// creatable for testing purposes.
class TestableKeyspaceEventManager : public KeyspaceEventManager {
 public:
  TestableKeyspaceEventManager() : KeyspaceEventManager() {}
};

class MockThreadPool : public vmsdk::ThreadPool {
 public:
  MockThreadPool(const std::string& name, size_t num_threads)
      : vmsdk::ThreadPool(name, num_threads) {
    ON_CALL(*this, Schedule(testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [this](absl::AnyInvocable<void()> task, Priority priority) {
              return ThreadPool::Schedule(std::move(task), priority);
            }));
  }
  MOCK_METHOD(bool, Schedule,
              (absl::AnyInvocable<void()> task, Priority priority), (override));
};

class ValkeySearchTest : public vmsdk::RedisTest {
 protected:
  RedisModuleCtx fake_ctx_;
  RedisModuleCtx registry_ctx_;

  void SetUp() override {
    RedisTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); }, nullptr,
        false));
    ON_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](RedisModuleCtx* ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorExternalizer::Instance().Init(&registry_ctx_);
  }
  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorExternalizer::Instance().Reset();
    RedisTest::TearDown();
  }
};

struct TestReturnAttribute {
  std::string identifier;
  std::string alias;
};

query::ReturnAttribute ToReturnAttribute(
    const TestReturnAttribute& test_return_attribute);

std::unordered_map<std::string, std::string> ToStringMap(const RecordsMap& map);

struct NeighborTest {
  std::string external_id;
  float distance;
  std::optional<std::unordered_map<std::string, std::string>>
      attribute_contents;
  inline bool operator==(const NeighborTest& other) const {
    return external_id == other.external_id && distance == other.distance &&
           attribute_contents == other.attribute_contents;
  }
};

indexes::Neighbor ToIndexesNeighbor(const NeighborTest& neighbor_test);
NeighborTest ToNeighborTest(const indexes::Neighbor& neighbor_test);

template <typename T>
std::vector<NeighborTest> ToVectorNeighborTest(const T& neighbors) {
  std::vector<NeighborTest> neighbors_test(neighbors.size());
  for (const auto& neighbor : neighbors) {
    neighbors_test.push_back(ToNeighborTest(neighbor));
  }
  return neighbors_test;
}

template <typename T>
class ValkeySearchTestWithParam : public vmsdk::RedisTestWithParam<T> {
 protected:
  RedisModuleCtx fake_ctx_;
  RedisModuleCtx registry_ctx_;

  void SetUp() override {
    vmsdk::RedisTestWithParam<T>::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); }, nullptr,
        false));
      ON_CALL(*kMockRedisModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault([&](RedisModuleCtx* ctx) {
          return ctx == &registry_ctx_ ? ctx : nullptr;
        });
    VectorExternalizer::Instance().Init(&registry_ctx_);
  }
  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    VectorExternalizer::Instance().Reset();
    vmsdk::RedisTestWithParam<T>::TearDown();
  }
};

std::vector<std::vector<float>> DeterministicallyGenerateVectors(
    int size, int dimensions, float max_value);

struct RespReply {
  using RespArray = std::vector<RespReply>;
  std::variant<std::string, int64_t, RespArray> value;

  static bool CompareArrays(const RespArray& array1, const RespArray& array2);

  // Equality operator to compare two RespReply objects
  bool operator==(const RespReply& other) const;

  // Inequality operator
  bool operator!=(const RespReply& other) const { return !(*this == other); }
};

RespReply ParseRespReply(absl::string_view input);
void WaitWorkerTasksAreCompleted(vmsdk::ThreadPool& mutations_thread_pool);

inline auto VectorToStr = [](const std::vector<float>& v) {
  return absl::string_view((char*)v.data(), v.size() * sizeof(float));
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_TESTING_COMMON_H_
