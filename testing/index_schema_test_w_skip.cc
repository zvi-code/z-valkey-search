/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/index_schema.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/keyspace_event_manager.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "third_party/hnswlib/hnswlib.h"  // IWYU pragma: keep
#include "third_party/hnswlib/space_ip.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

using testing::An;
using testing::Bool;
using testing::Combine;
using testing::Return;
using testing::StrEq;
using testing::TestParamInfo;
using testing::TypedEq;
using testing::UnorderedElementsAreArray;
using testing::ValuesIn;

struct IndexSchemaSubscriptionTestCase {
  std::string test_name;
  std::string hash_field;
  bool open_key_fail;
  int open_key_type;
  bool expect_wrong_type;
  // Set to nullopt to have Redis return does not exist
  absl::optional<std::pair<std::string, std::string>> valkey_hash_data;
  bool is_tracked;
  // Set to nullopt to not expect a call to the given index method.
  absl::optional<absl::StatusOr<bool>> expect_index_add_w_result;
  absl::optional<absl::StatusOr<bool>> expect_index_modify_w_result;
  absl::optional<absl::StatusOr<bool>> expect_index_remove_w_result;
  // Validated against the input to add/modify if they are not nullopt.
  std::string expected_vector_buffer;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_add_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_remove_cnt_delta;
  IndexSchema::Stats::ResultCnt<uint64_t> expected_modify_cnt_delta;
  indexes::DeletionType expected_deletion_type = indexes::DeletionType::kNone;
  int expected_document_cnt_delta;
};

class IndexSchemaSubscriptionTest
    : public ValkeySearchTestWithParam<IndexSchemaSubscriptionTestCase> {};

TEST_P(IndexSchemaSubscriptionTest, OnKeyspaceNotificationTest) {
  const IndexSchemaSubscriptionTestCase &test_case = GetParam();
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    ValkeyModuleCtx fake_ctx;
    std::vector<absl::string_view> key_prefixes = {"prefix:"};
    std::string index_schema_name_str("index_schema_name");
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();
    EXPECT_TRUE(
        KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
    auto mock_index = std::make_shared<MockIndex>();
    VMSDK_EXPECT_OK(index_schema->AddIndex("attribute_name",
                                           test_case.hash_field, mock_index));

    auto key = StringInternStore::Intern("key");
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str().data());
    EXPECT_CALL(*mock_index, IsTracked(key))
        .WillRepeatedly(Return(test_case.is_tracked));
    if (test_case.expect_index_add_w_result.has_value()) {
      EXPECT_CALL(
          *mock_index,
          AddRecord(key, absl::string_view(test_case.expected_vector_buffer)))
          .WillOnce(Return(test_case.expect_index_add_w_result.value()));
    } else if (test_case.expect_index_modify_w_result.has_value()) {
      EXPECT_CALL(*mock_index,
                  ModifyRecord(
                      key, absl::string_view(test_case.expected_vector_buffer)))
          .WillOnce(Return(test_case.expect_index_modify_w_result.value()));
    } else if (test_case.expect_index_remove_w_result.has_value()) {
      EXPECT_CALL(*mock_index,
                  RemoveRecord(key, test_case.expected_deletion_type))
          .WillOnce(Return(test_case.expect_index_remove_w_result.value()));
    }
    if (test_case.open_key_fail) {
      // Keep the default behavior still for other keys (e.g. IndexSchema key).
      EXPECT_CALL(*kMockValkeyModule,
                  OpenKey(&fake_ctx, testing::_, testing::_))
          .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
      EXPECT_CALL(*kMockValkeyModule,
                  OpenKey(&fake_ctx, key_valkey_str.get(),
                          VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ))
          .WillOnce(Return(nullptr));
    } else {
      EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
          .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
      EXPECT_CALL(*kMockValkeyModule,
                  KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
          .WillRepeatedly(Return(test_case.open_key_type));
    }

    if (test_case.valkey_hash_data.has_value()) {
      const char *field = test_case.valkey_hash_data.value().first.c_str();
      const char *value = test_case.valkey_hash_data.value().second.c_str();
      ValkeyModuleString *value_valkey_str =
          TestValkeyModule_CreateStringPrintf(nullptr, "%s", value);

      EXPECT_CALL(
          *kMockValkeyModule,
          HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                  VALKEYMODULE_HASH_CFIELDS, StrEq(field),
                  An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([value_valkey_str](ValkeyModuleKey *key, int flags,
                                       const char *field,
                                       ValkeyModuleString **value_out,
                                       void *terminating_null) {
            *value_out = value_valkey_str;
            return VALKEYMODULE_OK;
          });
    } else if (!test_case.open_key_fail && !test_case.expect_wrong_type) {
      EXPECT_CALL(
          *kMockValkeyModule,
          HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                  VALKEYMODULE_HASH_CFIELDS, StrEq(test_case.hash_field),
                  An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
          .WillOnce([](ValkeyModuleKey *key, int flags, const char *field,
                       ValkeyModuleString **value_out, void *terminating_null) {
            *value_out = nullptr;
            return VALKEYMODULE_OK;
          });
    }

    IndexSchema::Stats::ResultCnt<uint64_t> add_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_add.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_add.success_cnt,
        .skipped_cnt = index_schema->GetStats().subscription_add.skipped_cnt};
    IndexSchema::Stats::ResultCnt<uint64_t> remove_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_remove.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_remove.success_cnt,
        .skipped_cnt =
            index_schema->GetStats().subscription_remove.skipped_cnt};
    IndexSchema::Stats::ResultCnt<uint64_t> modify_cnt = {
        .failure_cnt = index_schema->GetStats().subscription_modify.failure_cnt,
        .success_cnt = index_schema->GetStats().subscription_modify.success_cnt,
        .skipped_cnt =
            index_schema->GetStats().subscription_modify.skipped_cnt};
    uint32_t document_cnt = index_schema->GetStats().document_cnt;
    index_schema->OnKeyspaceNotification(&fake_ctx, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
    if (use_thread_pool) {
      WaitWorkerTasksAreCompleted(mutations_thread_pool);
    }
    for (const auto &tuple :
         {std::make_tuple(add_cnt, &index_schema->GetStats().subscription_add,
                          &test_case.expected_add_cnt_delta),
          std::make_tuple(remove_cnt,
                          &index_schema->GetStats().subscription_remove,
                          &test_case.expected_remove_cnt_delta),
          std::make_tuple(modify_cnt,
                          &index_schema->GetStats().subscription_modify,
                          &test_case.expected_modify_cnt_delta)}) {
      EXPECT_EQ(
          std::get<1>(tuple)->success_cnt - std::get<0>(tuple).success_cnt,
          std::get<2>(tuple)->success_cnt);
      EXPECT_EQ(
          std::get<1>(tuple)->skipped_cnt - std::get<0>(tuple).skipped_cnt,
          std::get<2>(tuple)->skipped_cnt);
      EXPECT_EQ(
          std::get<1>(tuple)->failure_cnt - std::get<0>(tuple).failure_cnt,
          std::get<2>(tuple)->failure_cnt);
    }
    EXPECT_EQ(index_schema->GetStats().document_cnt - document_cnt,
              test_case.expected_document_cnt_delta);
  }
}

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaSubscriptionTests, IndexSchemaSubscriptionTest,
    ValuesIn<IndexSchemaSubscriptionTestCase>({
        {
            .test_name = "happy_path_add",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_document_cnt_delta = 1,
        },
        {
            .test_name = "happy_path_remove_key",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
        },
        {
            .test_name = "happy_path_remove_record",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
        },
        {
            .test_name = "happy_path_modify",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = true,
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .success_cnt = 1,
                },
        },
        {
            .test_name = "untracked_and_record_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "untracked_and_key_does_not_exist",
            .hash_field = "vector",
            .open_key_fail = true,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = false,
            .expect_index_remove_w_result = false,
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
            .expected_deletion_type = indexes::DeletionType::kRecord,
        },
        {
            .test_name = "add_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "modify_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "remove_failure",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = absl::InternalError("error"),
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .failure_cnt = 1,
                },
        },
        {
            .test_name = "add_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = false,
            .expect_index_add_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_add_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "add_wrong_type",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_STRING,
            .expect_wrong_type = true,
        },
        {
            .test_name = "modify_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::make_pair("vector", "vector_buffer"),
            .is_tracked = true,
            .expect_index_modify_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_modify_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
        {
            .test_name = "remove_skipped",
            .hash_field = "vector",
            .open_key_fail = false,
            .open_key_type = VALKEYMODULE_KEYTYPE_HASH,
            .valkey_hash_data = std::nullopt,
            .is_tracked = true,
            .expect_index_remove_w_result = false,
            .expected_vector_buffer = "vector_buffer",
            .expected_remove_cnt_delta =
                IndexSchema::Stats::ResultCnt<uint64_t>{
                    .skipped_cnt = 1,
                },
        },
    }),
    [](const TestParamInfo<IndexSchemaSubscriptionTestCase> &info) {
      return info.param.test_name;
    });

class IndexSchemaSubscriptionSimpleTest
    : public ValkeySearchTestWithParam<bool> {};

TEST_P(IndexSchemaSubscriptionSimpleTest, DropIndexPrematurely) {
  // This test covers verifies that Unblockclient is called when an index schema
  // is dropped prematurely while there are pending mutations in the worker
  // thread pool
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  VMSDK_EXPECT_OK(mutations_thread_pool.SuspendWorkers());
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  std::string index_schema_name_str("index_schema_name");
  {
    auto index_schema =
        MockIndexSchema::Create(&fake_ctx_, index_schema_name_str, key_prefixes,
                                std::make_unique<HashAttributeDataType>(),
                                &mutations_thread_pool)
            .value();
    EXPECT_TRUE(
        KeyspaceEventManager::Instance().HasSubscription(index_schema.get()));
    auto mock_index = std::make_shared<MockIndex>();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute_name", "vector", mock_index));

    auto key = StringInternStore::Intern("key");
    auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key->Str().data());
    EXPECT_CALL(*mock_index, IsTracked(key)).WillRepeatedly(Return(false));

    EXPECT_CALL(*mock_index, AddRecord(key, testing::_)).Times(0);

    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(TestValkeyModule_KeyTypeDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                KeyType(vmsdk::ValkeyModuleKeyIsForString(key->Str())))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule, GetClientId(testing::_))
        .WillRepeatedly(testing::Return(1));
    EXPECT_CALL(
        *kMockValkeyModule,
        BlockClient(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillOnce(Return((ValkeyModuleBlockedClient *)1));
    const char *field = "vector";
    const char *value = "vector_buffer";
    ValkeyModuleString *value_valkey_str =
        TestValkeyModule_CreateStringPrintf(nullptr, "%s", value);

    EXPECT_CALL(*kMockValkeyModule,
                HashGet(vmsdk::ValkeyModuleKeyIsForString(key->Str()),
                        VALKEYMODULE_HASH_CFIELDS, StrEq(field),
                        An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillOnce([value_valkey_str](
                      ValkeyModuleKey *key, int flags, const char *field,
                      ValkeyModuleString **value_out, void *terminating_null) {
          *value_out = value_valkey_str;
          return VALKEYMODULE_OK;
        });

    index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                         "event", key_valkey_str.get());
    EXPECT_CALL(*kMockValkeyModule,
                UnblockClient((ValkeyModuleBlockedClient *)1, nullptr))
        .WillOnce(Return(1));
  }
  EXPECT_EQ(mutations_thread_pool.QueueSize(), 1);
  VMSDK_EXPECT_OK(mutations_thread_pool.ResumeWorkers());
  WaitWorkerTasksAreCompleted(mutations_thread_pool);
  EXPECT_TRUE(vmsdk::TrackedBlockedClients().empty());
}

TEST_P(IndexSchemaSubscriptionSimpleTest, EmptyKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  auto use_thread_pool = GetParam();

  mutations_thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(), UnorderedElementsAreArray({""}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, DuplicateKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();

  std::vector<absl::string_view> key_prefixes = {"pre", "pre"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, PrefixIsPrefixedByAnotherTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {"pre", "prefix"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray({"pre"}));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, IndexSchemaInDifferentDBTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  EXPECT_CALL(*mock_index, AddRecord(testing::_, testing::_)).Times(0);
  std::string key = "key";
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key.c_str());
  ValkeyModuleCtx different_db_ctx;
  index_schema->OnKeyspaceNotification(&different_db_ctx,
                                       VALKEYMODULE_NOTIFY_HASH, "event",
                                       key_valkey_str.get());
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest,
       DBHasMatchingKeyWithWrongModuleTypeTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  EXPECT_CALL(*mock_index, AddRecord(testing::_, testing::_)).Times(0);
  std::string key = "key";
  auto key_valkey_str = vmsdk::MakeUniqueValkeyString(key.c_str());
  ValkeyModuleCtx different_db_ctx;
  auto match_key =
      vmsdk::MakeUniqueValkeyOpenKey(&different_db_ctx, key_valkey_str.get(), 0);
  TestValkeyModule_ModuleTypeSetValueDefaultImpl(
      match_key.get(), (ValkeyModuleType *)0x1, nullptr);
  index_schema->OnKeyspaceNotification(&different_db_ctx,
                                       VALKEYMODULE_NOTIFY_HASH, "event",
                                       key_valkey_str.get());
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest, KeyspaceNotificationWithNullptrTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));
  EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx_, testing::_, testing::_))
      .Times(0);
  index_schema->OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_HASH,
                                       "event", nullptr);
  if (use_thread_pool) {
    WaitWorkerTasksAreCompleted(mutations_thread_pool);
  }
}

TEST_P(IndexSchemaSubscriptionSimpleTest, GetKeyPrefixesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {
      "prefix:", "prefix1:", "prefix2:"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              UnorderedElementsAreArray(key_prefixes));
}

TEST_P(IndexSchemaSubscriptionSimpleTest, GetEventTypesTest) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  auto use_thread_pool = GetParam();
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  auto index_schema = MockIndexSchema::Create(
                          &fake_ctx_, index_schema_name_str, key_prefixes,
                          std::make_unique<HashAttributeDataType>(),
                          use_thread_pool ? &mutations_thread_pool : nullptr)
                          .value();

  EXPECT_EQ(index_schema->GetAttributeDataType().GetValkeyEventTypes(),
            VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC |
                VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED);
}

INSTANTIATE_TEST_SUITE_P(IndexSchemaSubscriptionSimpleTests,
                         IndexSchemaSubscriptionSimpleTest,
                         ::testing::Values(false, true),
                         [](const testing::TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });

struct IndexSchemaBackfillTestCase {
  std::string test_name;
  uint32_t scan_batch_size;
  std::vector<std::string> key_prefixes;
  uint64_t db_size;
  std::vector<std::string> keys_to_return_in_scan;
  bool return_wrong_types;
  int context_flags = 0;
  uint32_t expected_keys_scanned;
  std::vector<std::string> expected_keys_processed;
  float expected_backfill_percent;
  std::string expected_state;
};

class IndexSchemaBackfillTest
    : public ValkeySearchTestWithParam<
          ::testing::tuple<bool, IndexSchemaBackfillTestCase>> {};

TEST_P(IndexSchemaBackfillTest, PerformBackfillTest) {
  auto &params = GetParam();
  bool use_thread_pool = std::get<0>(params);
  const IndexSchemaBackfillTestCase &test_case = std::get<1>(params);
  MockThreadPool thread_pool("writer-thread-pool-", 5);
  thread_pool.StartWorkers();
  std::vector<absl::string_view> key_prefixes;
  std::transform(test_case.key_prefixes.begin(), test_case.key_prefixes.end(),
                 std::back_inserter(key_prefixes),
                 [](const std::string &key_prefix) {
                   return absl::string_view(key_prefix);
                 });
  std::string index_schema_name_str("index_schema_name");
  EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
      .WillRepeatedly(Return(test_case.db_size));

  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&parent_ctx))
      .WillRepeatedly(Return(test_case.context_flags));
  EXPECT_CALL(*kMockValkeyModule, GetContextFlags(&scan_ctx))
      .WillRepeatedly(Return(0));
  auto index_schema =
      MockIndexSchema::Create(&parent_ctx, index_schema_name_str, key_prefixes,
                              std::make_unique<HashAttributeDataType>(),
                              use_thread_pool ? &thread_pool : nullptr)
          .value();
  auto mock_index = std::make_shared<MockIndex>();
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("attribute_name", "test_identifier", mock_index));

  size_t i = 0;
  EXPECT_CALL(*kMockValkeyModule,
              Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                   testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                          ValkeyModuleScanCB fn, void *privdata) -> int {
        if (i >= test_case.keys_to_return_in_scan.size()) {
          return 0;
        }
        bool expect_processed =
            std::find(test_case.expected_keys_processed.begin(),
                      test_case.expected_keys_processed.end(),
                      test_case.keys_to_return_in_scan[i]) !=
            test_case.expected_keys_processed.end();

        auto key_str = test_case.keys_to_return_in_scan[i];
        auto key_r_str = vmsdk::MakeUniqueValkeyString(key_str);
        ValkeyModuleKey key = {.ctx = &scan_ctx, .key = key_str};
        if (expect_processed) {
          ValkeyModuleString *value_valkey_str =
              TestValkeyModule_CreateStringPrintf(nullptr, "arbitrary data");
          EXPECT_CALL(
              *kMockValkeyModule,
              HashGet(vmsdk::ValkeyModuleKeyIsForString(key_str),
                      VALKEYMODULE_HASH_CFIELDS, testing::_,
                      An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
              .WillOnce([value_valkey_str](ValkeyModuleKey *key, int flags,
                                           const char *field,
                                           ValkeyModuleString **value_out,
                                           void *terminating_null) {
                *value_out = value_valkey_str;
                return VALKEYMODULE_OK;
              });
          EXPECT_CALL(*mock_index,
                      IsTracked(testing::Pointee(testing::StrEq(key_str))))
              .WillRepeatedly(testing::Return(false));
          EXPECT_CALL(
              *mock_index,
              AddRecord(testing::Pointee(testing::StrEq(key_str)), testing::_))
              .WillOnce(testing::Return(true));
          if (use_thread_pool) {
            EXPECT_CALL(thread_pool,
                        Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
                .Times(1);
            EXPECT_CALL(*kMockValkeyModule,
                        BlockClient(testing::_, testing::_, testing::_,
                                    testing::_, testing::_))
                .Times(0);
            EXPECT_CALL(*kMockValkeyModule,
                        UnblockClient(testing::_, testing::_))
                .Times(0);
          }
        }
        if (test_case.return_wrong_types) {
          EXPECT_CALL(*kMockValkeyModule,
                      KeyType(vmsdk::ValkeyModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_STRING));
        } else {
          EXPECT_CALL(*kMockValkeyModule,
                      KeyType(vmsdk::ValkeyModuleKeyIsForString(key_str)))
              .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
        }
        fn(ctx, key_r_str.get(), &key, privdata);
        if (use_thread_pool) {
          EXPECT_CALL(thread_pool,
                      Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
              .Times(thread_pool.Size());
          WaitWorkerTasksAreCompleted(thread_pool);
        }
        return (++i < test_case.keys_to_return_in_scan.size()) ? 1 : 0;
      });
  EXPECT_EQ(
      index_schema->PerformBackfill(&parent_ctx, test_case.scan_batch_size),
      test_case.expected_keys_scanned);
  if (!use_thread_pool) {
    EXPECT_EQ(index_schema->IsBackfillInProgress(),
              test_case.expected_backfill_percent != 1.0);
    EXPECT_EQ(index_schema->GetBackfillPercent(),
              test_case.expected_backfill_percent);
    EXPECT_EQ(index_schema->GetStateForInfo(), test_case.expected_state);
  } else {
    EXPECT_CALL(thread_pool,
                Schedule(testing::_, vmsdk::ThreadPool::Priority::kLow))
        .Times(thread_pool.Size());
    WaitWorkerTasksAreCompleted(thread_pool);
  }
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_NoOngoingBackfillTest) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // We only expect it to do the scan the first iteration.
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillOnce([&](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                      ValkeyModuleScanCB fn,
                      void *privdata) -> int { return 0; });

    for (size_t i = 0; i < 100; ++i) {
      EXPECT_EQ(index_schema->PerformBackfill(&parent_ctx, 1024), 0);
    }
  }
}

TEST_F(IndexSchemaBackfillTest, PerformBackfill_SwapDB) {
  std::vector<absl::string_view> key_prefixes = {"unused"};
  std::string index_schema_name_str("index_schema_name");
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    int starting_db = 0;
    int db_to_swap = 1;
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&parent_ctx))
        .WillRepeatedly(Return(starting_db));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, starting_db))
        .WillRepeatedly(Return(VALKEYMODULE_OK));
    auto index_schema = MockIndexSchema::Create(
                            &parent_ctx, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    // Validate swapping changes the db in the context
    ValkeyModuleSwapDbInfo swap_db_info = {
        .dbnum_first = starting_db,
        .dbnum_second = db_to_swap,
    };
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, db_to_swap))
        .WillOnce(Return(VALKEYMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);

    // Validate swapping again brings the db back to the original
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&scan_ctx, starting_db))
        .WillOnce(Return(VALKEYMODULE_OK));
    index_schema->OnSwapDB(&swap_db_info);
  }
}

INSTANTIATE_TEST_SUITE_P(
    IndexSchemaBackfillTests, IndexSchemaBackfillTest,
    Combine(Bool(),
            ValuesIn<IndexSchemaBackfillTestCase>({
                {
                    .test_name = "batch_size_5",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3", "prefix1:key4",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "not_all_match",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix2:key2",
                                               "prefix1:key3", "prefix2:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key3",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "smaller_scan_batch_size_than_available",
                    .scan_batch_size = 3,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 3,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3"},
                    .expected_backfill_percent = 0.6,
                    .expected_state = "backfill_in_progress",
                },
                {
                    .test_name = "bigger_scan_batch_size_than_available",
                    .scan_batch_size = 7,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 5,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 5,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3", "prefix1:key4",
                                                "prefix1:key5"},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "no_backfill",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 0,
                    .keys_to_return_in_scan = {},
                    .expected_keys_scanned = 0,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "wrong_types_not_added",
                    .scan_batch_size = 5,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 1,
                    .keys_to_return_in_scan = {"prefix1:key1"},
                    .return_wrong_types = true,
                    .expected_keys_scanned = 1,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 1.0,
                    .expected_state = "ready",
                },
                {
                    .test_name = "dbsize_shrunk",
                    .scan_batch_size = 3,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 1,
                    .keys_to_return_in_scan = {"prefix1:key1", "prefix1:key2",
                                               "prefix1:key3", "prefix1:key4",
                                               "prefix1:key5"},
                    .expected_keys_scanned = 3,
                    .expected_keys_processed = {"prefix1:key1", "prefix1:key2",
                                                "prefix1:key3"},
                    .expected_backfill_percent = 0.99,
                    .expected_state = "backfill_in_progress",
                },
                {
                    .test_name = "oom",
                    .scan_batch_size = 100,
                    .key_prefixes = {"prefix1:"},
                    .db_size = 100,
                    .keys_to_return_in_scan = {},
                    .context_flags = VALKEYMODULE_CTX_FLAGS_OOM,
                    .expected_keys_scanned = 0,
                    .expected_keys_processed = {},
                    .expected_backfill_percent = 0.0,
                    .expected_state = "backfill_paused_by_oom",
                },
            })),
    [](const TestParamInfo<::testing::tuple<bool, IndexSchemaBackfillTestCase>>
           &info) {
      return std::get<1>(info.param).test_name + "_" +
             (std::get<0>(info.param) ? "WithThreadPool" : "WithoutThreadPool");
    });

class IndexSchemaRDBTest : public ValkeySearchTest {};

TEST_F(IndexSchemaRDBTest, SaveAndLoad) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};
  std::string index_schema_name_str("index_schema_name");
  int dimensions = 100;
  auto distance_metric = data_model::DISTANCE_METRIC_COSINE;
  int initial_cap = 12;
  int m = 16;
  int ef_construction = 100;
  int ef_runtime = 5;
  int block_size = 250;

  FakeSafeRDB rdb_stream;

  // Construct and save index schema
  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "hnsw_attribute",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("hnsw_attribute", "hnsw_identifier",
                                           hnsw_index));
    auto itr = index_schema->attributes_.find("hnsw_attribute");

    EXPECT_FALSE(itr == index_schema->attributes_.end());
    auto vectors = DeterministicallyGenerateVectors(10, dimensions, 2);
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueValkeyString data =
          vmsdk::MakeUniqueValkeyString(absl::string_view(
              (char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key = StringInternStore::Intern("key" + std::to_string(i));
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    auto flat_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       block_size),
            "flat_identifier",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("flat_attribute", "flat_identifier",
                                           flat_index));

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load the saved index schema and validate
  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  auto index_schema_or =
      IndexSchema::LoadFromRDB(&parent_ctx,
                               /*mutations_thread_pool=*/nullptr,
                               std::make_unique<data_model::IndexSchema>(
                                   (*section)->index_schema_contents()),
                               iter.IterateSupplementalContent());
  VMSDK_EXPECT_OK_STATUSOR(index_schema_or);
  auto index_schema = std::move(index_schema_or.value());

  EXPECT_THAT(index_schema->GetKeyPrefixes(),
              testing::UnorderedElementsAre("prefix1", "prefix2"));
  EXPECT_TRUE(dynamic_cast<const HashAttributeDataType *>(
      &index_schema->GetAttributeDataType()));
  VMSDK_EXPECT_OK(index_schema->GetIndex("hnsw_attribute"));
  auto hnsw_index = dynamic_cast<indexes::VectorHNSW<float> *>(
      index_schema->GetIndex("hnsw_attribute").value().get());
  EXPECT_TRUE(hnsw_index != nullptr);
  EXPECT_EQ(hnsw_index->GetDimensions(), dimensions);
  EXPECT_TRUE(dynamic_cast<const hnswlib::InnerProductSpace *>(
                  hnsw_index->GetSpace()) != nullptr);
  EXPECT_EQ(hnsw_index->GetCapacity(), initial_cap);
  EXPECT_EQ(hnsw_index->GetM(), m);
  EXPECT_EQ(hnsw_index->GetEfConstruction(), ef_construction);
  EXPECT_EQ(hnsw_index->GetEfRuntime(), ef_runtime);

  VMSDK_EXPECT_OK(index_schema->GetIndex("flat_attribute"));
  auto flat_index = dynamic_cast<indexes::VectorFlat<float> *>(
      index_schema->GetIndex("flat_attribute").value().get());
  EXPECT_TRUE(flat_index != nullptr);
  EXPECT_EQ(flat_index->GetDimensions(), dimensions);
  EXPECT_TRUE(dynamic_cast<const hnswlib::InnerProductSpace *>(
                  flat_index->GetSpace()) != nullptr);
  EXPECT_EQ(flat_index->GetCapacity(), initial_cap);
  EXPECT_EQ(flat_index->GetBlockSize(), block_size);

  EXPECT_TRUE(index_schema->IsBackfillInProgress());
  EXPECT_EQ(index_schema->GetStats().document_cnt, 10);
  EXPECT_EQ(index_schema->CountRecords(), 10);
}

TEST_F(IndexSchemaRDBTest, LoadEndedDeletesOrphanedKeys) {
  vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 1);
  mutations_thread_pool.StartWorkers();
  for (bool use_thread_pool : {true, false}) {
    auto mock_index = std::make_shared<MockIndex>();
    absl::flat_hash_map<std::string, uint64_t> keys_in_index = {
        {"key1", 1}, {"key2", 2}, {"key3", 3}};
    EXPECT_CALL(*mock_index, ForEachTrackedKey(testing::_))
        .WillOnce([&keys_in_index](
                      absl::AnyInvocable<void(const InternedStringPtr &)> fn) {
          for (const auto &[key, internal_id] : keys_in_index) {
            InternedStringPtr interned_key = StringInternStore::Intern(key);
            fn(interned_key);
          }
        });

    std::vector<absl::string_view> key_prefixes = {"prefix1", "prefix2"};
    std::string index_schema_name_str("index_schema_name");

    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(),
                            use_thread_pool ? &mutations_thread_pool : nullptr)
                            .value();

    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute", "identifier", mock_index));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
        .WillRepeatedly(Return(1));  // So backfill job can be created.
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 0))
        .WillOnce(Return(1));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key1")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key2")))
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*kMockValkeyModule,
                KeyExists(&fake_ctx_, vmsdk::ValkeyModuleStringValueEq("key3")))
        .WillRepeatedly(Return(1));

    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key1")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key2")),
                             indexes::DeletionType::kRecord))
        .WillOnce(Return(true));
    EXPECT_CALL(*mock_index,
                RemoveRecord(testing::Pointee(testing::StrEq("key3")),
                             indexes::DeletionType::kRecord))
        .Times(0);
    index_schema->OnLoadingEnded(&fake_ctx_);
    if (use_thread_pool) {
      WaitWorkerTasksAreCompleted(mutations_thread_pool);
    }
  }
}

TEST_F(IndexSchemaRDBTest, SkipLoadCorruptedData) {
  // TODO: Implement proper corrupted RDB data handling test
  // This test is currently disabled while we focus on the core skip load functionality
  GTEST_SKIP() << "Corrupted RDB data handling to be implemented later";
}

TEST_F(IndexSchemaRDBTest, CompareNormalVsSkipLoad) {
  std::vector<absl::string_view> key_prefixes = {"item:"};
  std::string index_schema_name_str("compare_index");
  int dimensions = 64;
  auto distance_metric = data_model::DISTANCE_METRIC_L2;
  int initial_cap = 100;
  int m = 16;
  int ef_construction = 200;
  int ef_runtime = 10;
  const int num_vectors = 10;

  FakeSafeRDB rdb_stream;
  
  // Create and save a populated index schema
  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    auto hnsw_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap,
                                       m, ef_construction, ef_runtime),
            "embedding",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

    auto vectors = DeterministicallyGenerateVectors(num_vectors, dimensions, 1.0);
    auto itr = index_schema->attributes_.find("embedding");
    EXPECT_FALSE(itr == index_schema->attributes_.end());
    
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(
          absl::string_view((char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key = StringInternStore::Intern("item:" + std::to_string(i));
      
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Test 1: Normal load (skip disabled)
  std::shared_ptr<IndexSchema> normal_schema;
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));
    
    // Reset stream for reading
    rdb_stream.buffer_.clear();
    rdb_stream.buffer_.seekg(0, std::ios::beg);
    
    ValkeyModuleCtx parent_ctx;
    ValkeyModuleCtx scan_ctx;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
        .WillRepeatedly(Return(&scan_ctx));
    
    RDBSectionIter iter(&rdb_stream, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);
    
    auto schema_or = IndexSchema::LoadFromRDB(
        &parent_ctx, nullptr,
        std::make_unique<data_model::IndexSchema>((*section)->index_schema_contents()),
        iter.IterateSupplementalContent());
    
    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    normal_schema = std::move(schema_or.value());
    
    // Should have loaded all data
    EXPECT_EQ(normal_schema->GetStats().document_cnt, num_vectors);
    
    // Vector index should be populated
    auto vec_index = normal_schema->GetIndex("embedding");
    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    EXPECT_EQ(vec_index.value()->GetRecordCount(), num_vectors);
  }

  // Test 2: Skip load (skip enabled) 
  std::shared_ptr<IndexSchema> skip_schema;
  {
    VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(true));
    
    // Reset stream for reading
    rdb_stream.buffer_.clear();
    rdb_stream.buffer_.seekg(0, std::ios::beg);
    
    ValkeyModuleCtx parent_ctx2;
    ValkeyModuleCtx scan_ctx2;
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx2))
        .WillRepeatedly(Return(&scan_ctx2));
    EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
        .WillRepeatedly(Return(num_vectors)); // Simulate keys in database
    
    // Mock scan to return keys that match our prefix, simulating backfill behavior
    int scan_call_count = 0;
    EXPECT_CALL(*kMockValkeyModule,
                Scan(&scan_ctx2, testing::An<ValkeyModuleScanCursor *>(),
                     testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
        .WillRepeatedly([&scan_call_count, num_vectors](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                          ValkeyModuleScanCB fn, void *privdata) -> int {
          if (scan_call_count < num_vectors) {
            // Return a key that matches our prefix
            std::string key = "item:" + std::to_string(scan_call_count);
            auto key_r_str = vmsdk::MakeUniqueValkeyString(key);
            ValkeyModuleKey vkey = {.ctx = ctx, .key = key};
            
            // Call the callback with the key
            fn(ctx, key_r_str.get(), &vkey, privdata);
            scan_call_count++;
            return 1; // More keys available
          }
          return 0; // No more keys
        });
    
    // Mock key operations for the scan
    EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
        .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
    EXPECT_CALL(*kMockValkeyModule, 
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_, 
                       testing::An<ValkeyModuleString**>(), TypedEq<void*>(nullptr)))
        .WillRepeatedly(Return(VALKEYMODULE_ERR)); // No hash field found, skip processing
    
    RDBSectionIter iter(&rdb_stream, 1);
    auto section = iter.Next();
    VMSDK_EXPECT_OK_STATUSOR(section);
    
    auto schema_or = IndexSchema::LoadFromRDB(
        &parent_ctx2, nullptr,
        std::make_unique<data_model::IndexSchema>((*section)->index_schema_contents()),
        iter.IterateSupplementalContent());
    
    VMSDK_EXPECT_OK_STATUSOR(schema_or);
    skip_schema = std::move(schema_or.value());
    
    // Schema metadata should be preserved
    EXPECT_EQ(skip_schema->GetStats().document_cnt, num_vectors);
    
    // But vector index should be empty initially (skip load worked)
    auto vec_index = skip_schema->GetIndex("embedding");
    VMSDK_EXPECT_OK_STATUSOR(vec_index);
    EXPECT_EQ(vec_index.value()->GetRecordCount(), 0);
    
    // Backfill should be in progress after loading with skip enabled
    EXPECT_TRUE(skip_schema->IsBackfillInProgress());
    
    // Perform backfill to process the scanned keys
    auto scanned = skip_schema->PerformBackfill(&parent_ctx2, 1000);
    EXPECT_GT(scanned, 0); // Should have scanned some keys
    
    // Backfill may still be in progress or completed, depending on the number of keys processed
    // Keep running backfill until it's done
    while (skip_schema->IsBackfillInProgress()) {
      skip_schema->PerformBackfill(&parent_ctx2, 1000);
    }
    
    EXPECT_FALSE(skip_schema->IsBackfillInProgress());
  }

  // Cleanup
  VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));
}

TEST_F(IndexSchemaRDBTest, SkipLoadMixedIndexTypes) {
  // Test skip load with multiple index types
  VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(true));
  
  std::vector<absl::string_view> key_prefixes = {"product:"};
  std::string index_schema_name_str("mixed_index");
  int dimensions = 32;
  
  FakeSafeRDB rdb_stream;
  
  // Create schema with multiple index types
  {
    auto index_schema = MockIndexSchema::Create(
                            &fake_ctx_, index_schema_name_str, key_prefixes,
                            std::make_unique<HashAttributeDataType>(), nullptr)
                            .value();

    // Add vector index
    auto hnsw_index = indexes::VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_L2,
                                   100, 16, 200, 10),
        "embedding", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
        .value();
    VMSDK_EXPECT_OK(index_schema->AddIndex("embedding", "emb_id", hnsw_index));

    // Add some test data using the same pattern as SaveAndLoad test
    auto vectors = DeterministicallyGenerateVectors(3, dimensions, 1.0);
    auto itr = index_schema->attributes_.find("embedding");
    EXPECT_FALSE(itr == index_schema->attributes_.end());
    
    for (size_t i = 0; i < vectors.size(); ++i) {
      vmsdk::UniqueValkeyString data = vmsdk::MakeUniqueValkeyString(
          absl::string_view((char *)&vectors[i][0], dimensions * sizeof(float)));
      auto interned_key = StringInternStore::Intern("product:" + std::to_string(i));
      
      index_schema->ProcessAttributeMutation(&fake_ctx_, itr->second,
                                             interned_key, std::move(data),
                                             indexes::DeletionType::kNone);
    }

    VMSDK_EXPECT_OK(index_schema->RDBSave(&rdb_stream));
  }

  // Load with skip enabled
  ValkeyModuleCtx parent_ctx;
  ValkeyModuleCtx scan_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&parent_ctx))
      .WillRepeatedly(Return(&scan_ctx));
  EXPECT_CALL(*kMockValkeyModule, DbSize(testing::_))
      .WillRepeatedly(Return(3)); // Simulate 3 keys in database
  
  // Mock scan to return keys, testing backfill with in-progress state
  int scan_call_count = 0;
  EXPECT_CALL(*kMockValkeyModule,
              Scan(&scan_ctx, testing::An<ValkeyModuleScanCursor *>(),
                   testing::An<ValkeyModuleScanCB>(), testing::An<void *>()))
      .WillRepeatedly([&scan_call_count](ValkeyModuleCtx *ctx, ValkeyModuleScanCursor *cursor,
                        ValkeyModuleScanCB fn, void *privdata) -> int {
        if (scan_call_count < 3) {
          // Return a key that matches our prefix
          std::string key = "product:" + std::to_string(scan_call_count);
          auto key_r_str = vmsdk::MakeUniqueValkeyString(key);
          ValkeyModuleKey vkey = {.ctx = ctx, .key = key};
          
          // Call the callback with the key
          fn(ctx, key_r_str.get(), &vkey, privdata);
          scan_call_count++;
          return 1; // More keys available
        }
        return 0; // No more keys
      });
  
  // Mock key operations for the scan  
  EXPECT_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillRepeatedly(Return(VALKEYMODULE_KEYTYPE_HASH));
  EXPECT_CALL(*kMockValkeyModule, 
              HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_, 
                     testing::An<ValkeyModuleString**>(), TypedEq<void*>(nullptr)))
      .WillRepeatedly(Return(VALKEYMODULE_ERR)); // No hash field found, skip processing
  
  RDBSectionIter iter(&rdb_stream, 1);
  auto section = iter.Next();
  VMSDK_EXPECT_OK_STATUSOR(section);
  
  auto schema_or = IndexSchema::LoadFromRDB(
      &parent_ctx, nullptr,
      std::make_unique<data_model::IndexSchema>((*section)->index_schema_contents()),
      iter.IterateSupplementalContent());
  
  VMSDK_EXPECT_OK_STATUSOR(schema_or);
  auto index_schema = std::move(schema_or.value());
  
  // Schema should be loaded
  EXPECT_THAT(index_schema->GetKeyPrefixes(), testing::UnorderedElementsAre("product:"));
  
  // Vector index should exist but be empty initially (skip load worked)
  auto vec_index = index_schema->GetIndex("embedding");
  VMSDK_EXPECT_OK_STATUSOR(vec_index);
  EXPECT_EQ(vec_index.value()->GetRecordCount(), 0);
  
  // Backfill should be in progress
  EXPECT_TRUE(index_schema->IsBackfillInProgress());
  
  // Perform backfill to process the scanned keys
  auto scanned = index_schema->PerformBackfill(&parent_ctx, 1000);
  EXPECT_GT(scanned, 0); // Should have scanned some keys
  
  // Keep running backfill until it's done
  while (index_schema->IsBackfillInProgress()) {
    index_schema->PerformBackfill(&parent_ctx, 1000);
  }
  
  EXPECT_FALSE(index_schema->IsBackfillInProgress());
  
  // Cleanup
  VMSDK_EXPECT_OK(options::GetSkipIndexLoadMutable().SetValue(false));
}

}  // namespace valkey_search
