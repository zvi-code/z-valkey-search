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

#ifndef VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_
#define VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_

#include <cstddef>
#include <cstdlib>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/hnswlib/iostream.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/redismodule.h"

namespace valkey_search {

class RDBInputStream : public hnswlib::InputStream {
 public:
  explicit RDBInputStream(RedisModuleIO *rdb) : rdb_(rdb) {}

  absl::Status loadSizeT(size_t &val) override {
    val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status loadUnsigned(unsigned int &val) override {
    val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status loadSigned(int &val) override {
    val = RedisModule_LoadSigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadSigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status loadDouble(double &val) override {
    val = RedisModule_LoadDouble(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadDouble failed");
    }
    return absl::OkStatus();
  }

  absl::StatusOr<hnswlib::StringBufferUniquePtr> loadStringBuffer(
      const size_t len) override {
    if (len == 0) {
      return absl::InvalidArgumentError("len must be > 0");
    }
    size_t len_read;
    auto str = hnswlib::StringBufferUniquePtr(
        RedisModule_LoadStringBuffer(rdb_, &len_read));
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadStringBuffer failed");
    }
    if (len_read != len) {
      return absl::InternalError(
          "Inconsistency between the expected and the actual string length");
    }
    return str;
  }

  virtual absl::StatusOr<vmsdk::UniqueRedisString> loadString() {
    auto str = vmsdk::UniqueRedisString(RedisModule_LoadString(rdb_));
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadString failed");
    }
    return str;
  }

 protected:
  RDBInputStream() = default;

 private:
  RedisModuleIO *rdb_;
};

class RDBOutputStream : public hnswlib::OutputStream {
 public:
  explicit RDBOutputStream(RedisModuleIO *rdb) : rdb_(rdb) {}

  absl::Status saveSizeT(size_t val) override {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status saveUnsigned(unsigned int val) override {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status saveSigned(int val) override {
    RedisModule_SaveSigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveSigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status saveDouble(double val) override {
    RedisModule_SaveDouble(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveDouble failed");
    }
    return absl::OkStatus();
  }

  absl::Status saveStringBuffer(const char *str, size_t len) override {
    if (len == 0) {
      return absl::InvalidArgumentError("len must be > 0");
    }
    RedisModule_SaveStringBuffer(rdb_, str, len);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveStringBuffer failed");
    }
    return absl::OkStatus();
  }

 protected:
  RDBOutputStream() = default;

 private:
  RedisModuleIO *rdb_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_
