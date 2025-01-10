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

#include "src/utils/string_interning.h"

#include <cstring>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "src/utils/allocator.h"
#include "src/utils/intrusive_ref_count.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {

using testing::TestParamInfo;

namespace {

class StringInterningTest : public vmsdk::RedisTestWithParam<bool> {};

TEST_F(StringInterningTest, BasicTest) {
  EXPECT_EQ(StringInternStore::Instance().Size(), 0);
  {
    auto interned_key_1 = StringInternStore::Intern("key1");
    auto interned_key_2 = StringInternStore::Intern("key2");
    auto interned_key_2_1 = StringInternStore::Intern("key2");
    auto interned_key_3 = std::make_shared<InternedString>("key3");

    EXPECT_EQ(std::string(*interned_key_1), "key1");
    EXPECT_EQ(std::string(*interned_key_2), "key2");
    EXPECT_EQ(std::string(*interned_key_3), "key3");
    EXPECT_EQ(interned_key_2.get(), interned_key_2_1.get());
    EXPECT_EQ(StringInternStore::Instance().Size(), 2);
  }
  EXPECT_EQ(StringInternStore::Instance().Size(), 0);
}

TEST_P(StringInterningTest, WithAllocator) {
  bool require_ptr_alignment = GetParam();
  auto allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, strlen("prefix_key1") + 1, require_ptr_alignment);
  {
    EXPECT_EQ(StringInternStore::Instance().Size(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
    {
      auto interned_key_1 =
          StringInternStore::Intern("prefix_key1", allocator.get());
      EXPECT_EQ(allocator->ActiveAllocations(), 1);
      auto interned_key_2 =
          StringInternStore::Intern("prefix_key2", allocator.get());
      auto interned_key_2_1 = StringInternStore::Intern("prefix_key2");
      EXPECT_EQ(allocator->ActiveAllocations(), 2);
      auto interned_key_2_2 =
          StringInternStore::Intern("prefix_key2", allocator.get());
      EXPECT_EQ(allocator->ActiveAllocations(), 2);

      EXPECT_EQ(std::string(*interned_key_1), "prefix_key1");
      EXPECT_EQ(std::string(*interned_key_2), "prefix_key2");
      EXPECT_EQ(std::string(*interned_key_2_1), "prefix_key2");
      EXPECT_EQ(interned_key_2.get(), interned_key_2_1.get());
      EXPECT_EQ(StringInternStore::Instance().Size(), 2);
    }
    EXPECT_EQ(StringInternStore::Instance().Size(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(StringInterningTests, StringInterningTest,
                         ::testing::Values(true, false),
                         [](const TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });

}  // namespace

}  // namespace valkey_search
