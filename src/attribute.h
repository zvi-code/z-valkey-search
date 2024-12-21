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

#ifndef VALKEYSEARCH_SRC_ATTRIBUTE_H_
#define VALKEYSEARCH_SRC_ATTRIBUTE_H_

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"

namespace valkey_search {

class Attribute {
 public:
  Attribute(absl::string_view alias, absl::string_view identifier,
            std::shared_ptr<indexes::IndexBase> index)
      : alias_(alias), identifier_(identifier), index_(index) {}
  inline const std::string& GetAlias() const { return alias_; }
  inline const std::string& GetIdentifier() const { return identifier_; }
  std::shared_ptr<indexes::IndexBase> GetIndex() const { return index_; }
  std::unique_ptr<data_model::Attribute> ToProto() const {
    auto attribute_proto = std::make_unique<data_model::Attribute>();
    attribute_proto->set_alias(alias_);
    attribute_proto->set_identifier(identifier_);
    attribute_proto->set_allocated_index(index_->ToProto().release());
    return attribute_proto;
  }
  inline int RespondWithInfo(RedisModuleCtx* ctx) const {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    RedisModule_ReplyWithSimpleString(ctx, "identifier");
    RedisModule_ReplyWithSimpleString(ctx, GetIdentifier().c_str());
    RedisModule_ReplyWithSimpleString(ctx, "attribute");
    RedisModule_ReplyWithSimpleString(ctx, GetAlias().c_str());
    int added_fields = index_->RespondWithInfo(ctx);
    RedisModule_ReplySetArrayLength(ctx, added_fields + 4);
    return 1;
  }

  inline vmsdk::UniqueRedisString DefaultReplyScoreAs() const {
    if (!cached_score_as_) {
      cached_score_as_ =
          vmsdk::MakeUniqueRedisString(absl::StrCat("__", alias_, "_score"));
    }
    return vmsdk::RetainUniqueRedisString(cached_score_as_.get());
  }

 private:
  std::string alias_;
  std::string identifier_;
  std::shared_ptr<indexes::IndexBase> index_;
  // Maintaining a cached version
  mutable vmsdk::UniqueRedisString cached_score_as_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_H_
