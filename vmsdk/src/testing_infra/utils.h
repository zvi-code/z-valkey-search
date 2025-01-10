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

#ifndef VMSDK_SRC_TESTING_INFRA_UTILS
#define VMSDK_SRC_TESTING_INFRA_UTILS

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

using ::testing::TestWithParam;

#define VMSDK_EXPECT_OK(status) EXPECT_TRUE((status).ok())

namespace vmsdk {

class RedisTest : public testing::Test {
 protected:
  void SetUp() override { TestRedisModule_Init(); }

  void TearDown() override { TestRedisModule_Teardown(); }
};

template <typename T>
class RedisTestWithParam : public TestWithParam<T> {
 protected:
  void SetUp() override { TestRedisModule_Init(); }

  void TearDown() override { TestRedisModule_Teardown(); }
};

std::vector<RedisModuleString *> ToRedisStringVector(
    absl::string_view params_str, absl::string_view exclude = "");

MATCHER_P(RedisModuleStringEq, value, "") {
  return *((std::string *)arg) == *((std::string *)value);
}

MATCHER_P(RedisModuleStringValueEq, value, "") {
  *result_listener << "where the string is " << *((std::string *)arg);
  return *((std::string *)arg) == value;
}

MATCHER_P(RedisModuleKeyIsForString, value, "") {
  *result_listener << "where the key is " << ((RedisModuleKey *)arg)->key;
  return ((RedisModuleKey *)arg)->key == value;
}

MATCHER_P(IsRedisModuleEvent, expected, "") {
  return arg.id == expected.id && arg.dataver == expected.dataver;
}

}  // namespace vmsdk

#endif  // VMSDK_SRC_TESTING_INFRA_UTILS
