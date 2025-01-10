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

#include "vmsdk/src/module_type.h"

#include <cstddef>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

// Test fixture for module_type tests
class ModuleTypeTest : public vmsdk::RedisTest {
 protected:
  // Helper function to create RedisModuleString
  RedisModuleString* CreateRedisModuleString(const std::string& str) {
    return RedisModule_CreateString(NULL, str.c_str(), str.size());
  }
};

TEST_F(ModuleTypeTest, FailOpenRegisterKey) {
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(
          testing::Return(reinterpret_cast<RedisModuleKey*>(REDISMODULE_OK)));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  EXPECT_EQ(result.message(), "failed to open Redis module key: test_key");
  EXPECT_TRUE(result.code() != absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, SuccessfullyRegistersKey) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(fake_key))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_EMPTY));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);
  EXPECT_CALL(*kMockRedisModule, ModuleTypeSetValue(fake_key, module_type, ptr))
      .WillOnce(testing::Return(REDISMODULE_OK));

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, RegisterKeyAlreadyExistsError) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(testing::_))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_STRING));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kAlreadyExists);
}

TEST_F(ModuleTypeTest, RegisterInternalError) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, KeyType(fake_key))
      .WillOnce(testing::Return(REDISMODULE_KEYTYPE_EMPTY));
  EXPECT_CALL(*kMockRedisModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(REDISMODULE_OK));

  EXPECT_CALL(*kMockRedisModule,
              ModuleTypeSetValue(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(1));

  std::string key = "test_key";
  RedisModuleType* module_type = reinterpret_cast<RedisModuleType*>(2);
  void* ptr = reinterpret_cast<void*>(3);

  // Call the Register function
  absl::Status result = ModuleType::Register(NULL, key, ptr, module_type);

  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}

TEST_F(ModuleTypeTest, DeregisterSuccessfully) {
  auto fake_key = reinterpret_cast<RedisModuleKey*>(1);
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(fake_key));
  EXPECT_CALL(*kMockRedisModule, CloseKey(fake_key))
      .WillOnce(testing::Return());
  EXPECT_CALL(*kMockRedisModule, DeleteKey(fake_key))
      .WillOnce(testing::Return(REDISMODULE_OK));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kOk);
}

TEST_F(ModuleTypeTest, DeregisterUnregisteredKey) {
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(0));

  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  VMSDK_EXPECT_OK(result);
}

#ifdef NDEBUG
TEST_F(ModuleTypeTest, DeregisterFailOpenRedisKey) {
  EXPECT_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(nullptr));
  // Call the Deregister function
  absl::Status result = ModuleType::Deregister(NULL, "test_key");

  LOG(INFO) << "result: " << result.code();
  EXPECT_TRUE(result.code() == absl::StatusCode::kInternal);
}
#endif

}  // namespace

}  // namespace vmsdk
