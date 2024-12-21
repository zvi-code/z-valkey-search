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

#include <cstddef>
#include <vector>
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/redismodule.h"

namespace vmsdk {

std::vector<RedisModuleString *> ToRedisStringVector(
    absl::string_view params_str, absl::string_view exclude = "") {
  std::vector<absl::string_view> params =
      absl::StrSplit(params_str, ' ', absl::SkipEmpty());
  std::vector<RedisModuleString *> ret;
  for (size_t i = 0; i < params.size(); i += 2) {
    if (exclude == params[i]) {
      continue;
    }
    ret.push_back(
        RedisModule_CreateString(nullptr, params[i].data(), params[i].size()));
    if (i + 1 == params.size()) {
      break;
    }
    ret.push_back(RedisModule_CreateString(nullptr, params[i + 1].data(),
                                           params[i + 1].size()));
  }
  return ret;
}

}  // namespace vmsdk
