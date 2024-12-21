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

#include "src/rdb_io_stream.h"

#include <cstddef>
#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "third_party/hnswlib/iostream.h"
#include "testing/common.h"
#include "vmsdk/src/redismodule.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search {

namespace {

class RdbIoStreamTest : public ValkeySearchTest {
 protected:
  void SetUp() override { TestRedisModule_Init(); }
  RedisModuleIO* fake_redis_module_io_ = (RedisModuleIO*)0xBADF00D1;
};

TEST_F(RdbIoStreamTest, LoadSizeTSuccess) {
  size_t actual_value;
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.loadSizeT(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadSizeTFailure) {
  size_t actual_value;
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadSizeT(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadUnsignedSuccess) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.loadUnsigned(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadUnsignedFailure) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadUnsigned(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadSignedSuccess) {
  int actual_value;
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.loadSigned(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadSignedFailure) {
  int actual_value;
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadSigned(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadDoubleSuccess) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.loadDouble(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadDoubleFailure) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadDouble(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringBufferSuccess) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);
  auto expected_value_ptr = expected_value.get();
  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  auto actual_value_or = rdb_stream.loadStringBuffer(len);
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value_ptr);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureUnexpectedLength) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);

  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len + 1),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadStringBuffer(len).status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureZeroLength) {
  size_t len = 0;
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadStringBuffer(len).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureRedisIOError) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);
  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadStringBuffer(len).status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringSuccess) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  auto actual_value_or = rdb_stream.loadString();
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value);
}

TEST_F(RdbIoStreamTest, LoadStringFailure) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.loadString().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveSizeTSuccess) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.saveSizeT(value));
}

TEST_F(RdbIoStreamTest, SaveSizeTFailure) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveSizeT(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveUnsignedSuccess) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.saveUnsigned(value));
}

TEST_F(RdbIoStreamTest, SaveUnsignedFailure) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveUnsigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveSignedSuccess) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.saveSigned(value));
}

TEST_F(RdbIoStreamTest, SaveSignedFailure) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveSigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveDoubleSuccess) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.saveDouble(value));
}

TEST_F(RdbIoStreamTest, SaveDoubleFailure) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveDouble(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveStringBufferSuccess) {
  const char* value = "test";
  const size_t len = 5;
  EXPECT_CALL(*kMockRedisModule,
              SaveStringBuffer(fake_redis_module_io_, value, len));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.saveStringBuffer(value, len));
}

TEST_F(RdbIoStreamTest, SaveStringBufferFailureIOError) {
  const char* value = "test";
  const size_t len = 5;
  EXPECT_CALL(*kMockRedisModule,
              SaveStringBuffer(fake_redis_module_io_, value, len));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveStringBuffer(value, len).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveStringBufferFailureZeroLength) {
  const char* value = "test";
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.saveStringBuffer(value, 0).code(),
            absl::StatusCode::kInvalidArgument);
}

}  // namespace

}  // namespace valkey_search
