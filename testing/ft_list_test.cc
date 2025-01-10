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

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/text_format.h"
#include "src/commands/commands.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search {

namespace {

class FTListTest : public ValkeySearchTest {};

TEST_F(FTListTest, basic) {
  // Setup the data structures for the test case.
  for (bool use_thread_pool : {true, false}) {
    RedisModuleCtx fake_ctx;
    vmsdk::ThreadPool mutations_thread_pool("writer-thread-pool-", 5);
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, use_thread_pool ? &mutations_thread_pool : nullptr,
        false));

    auto fake_prefixes = std::vector<absl::string_view>{"prefix_1"};
    std::string index_schema_name_1_str = "index_schema_name_1";
    std::string index_schema_name_2_str = "index_schema_name_2";
    std::string different_db_index_schema_name_str = "index_schema_name_3";
    data_model::IndexSchema base_index_schema;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        R"(
          subscribed_key_prefixes: "prefix_1"
          attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
          attributes: {
            alias: "attribute_1"
            identifier: "attribute_1"
            index: {
              vector_index: {
                dimension_count: 756
                normalize: true
                distance_metric: DISTANCE_METRIC_COSINE
                vector_data_type: VECTOR_DATA_TYPE_FLOAT32
                initial_cap: 10240
                hnsw_algorithm {
                  m: 128
                  ef_construction: 128
                  ef_runtime: 128
                }
              }
            }
          }
        )",
        &base_index_schema));
    data_model::IndexSchema index_schema_1(base_index_schema);
    index_schema_1.set_name(index_schema_name_1_str);
    index_schema_1.set_db_num(0);
    data_model::IndexSchema index_schema_2(base_index_schema);
    index_schema_2.set_name(index_schema_name_2_str);
    index_schema_2.set_db_num(0);
    data_model::IndexSchema different_db_index_schema(base_index_schema);
    different_db_index_schema.set_name(different_db_index_schema_name_str);
    different_db_index_schema.set_db_num(1);
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx, index_schema_1));
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx, index_schema_2));
    VMSDK_EXPECT_OK(SchemaManager::Instance().CreateIndexSchema(
        &fake_ctx, different_db_index_schema));

    EXPECT_CALL(*kMockRedisModule, GetSelectedDb(&fake_ctx))
        .WillRepeatedly(testing::Return(0));
    VMSDK_EXPECT_OK(FTListCmd(&fake_ctx, nullptr, 0));
    EXPECT_THAT(fake_ctx.reply_capture.GetReply(),
                testing::AnyOf(
                    "*2\r\n+index_schema_name_1\r\n+index_schema_name_2\r\n",
                    "*2\r\n+index_schema_name_2\r\n+index_schema_name_1\r\n"));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        0, index_schema_name_1_str));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        0, index_schema_name_2_str));
    VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(
        1, different_db_index_schema_name_str));
  }
}
TEST_F(FTListTest, no_indexes) {
  EXPECT_CALL(*kMockRedisModule, ReplyWithArray(&fake_ctx_, 0));
  VMSDK_EXPECT_OK(FTListCmd(&fake_ctx_, nullptr, 0));
}

}  // namespace

}  // namespace valkey_search
