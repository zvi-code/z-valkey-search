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

#include "src/utils/string_interning.h"

#include <cstring>
#include <memory>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"

namespace valkey_search {

InternedString::InternedString(absl::string_view str, bool shared)
    : length_(str.length()), is_shared_(shared), is_data_owner_(true) {
  data_ = new char[length_ + 1];
  memcpy(data_, str.data(), length_);
  data_[length_] = '\0';
}

InternedString::InternedString(char *data, size_t length)
    : data_(data),
      length_(length),
      is_shared_(true),
      is_data_owner_(false) {}

InternedString::~InternedString() {
  if (is_shared_) {
    StringInternStore::Instance().Release(this);
  }
  if (is_data_owner_) {
    delete[] data_;
  } else {
    Allocator::Free(data_);
  }
}

void StringInternStore::Release(InternedString *str) {
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(*str);
  if (it == str_to_interned_.end()) {
    return;
  }
  auto locked = it->second.lock();
  // During `StringIntern` destruction, a new `StringIntern` may be stored,
  // so we check if the `StringIntern` being released is the one currently
  // stored before removing it.
  if (!locked || locked.get() == str) {
    str_to_interned_.erase(*str);
  }
}

std::shared_ptr<InternedString> StringInternStore::Intern(
    absl::string_view str, Allocator* allocator) {
  return Instance()._Intern(str, allocator);
}

std::shared_ptr<InternedString> StringInternStore::_Intern(
    absl::string_view str, Allocator* allocator) {
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(str);
  if (it != str_to_interned_.end()) {
    if (auto locked = it->second.lock()) {
      return locked;
    }
  }
  std::shared_ptr<InternedString> interned_string;
  if (allocator) {
    auto buffer = allocator->Allocate(str.size() + 1);
    memcpy(buffer, str.data(), str.size());
    buffer[str.size()] = '\0';
    interned_string = std::shared_ptr<InternedString>(
        new InternedString(buffer, str.size()));
  } else {
    interned_string =
        std::shared_ptr<InternedString>(new InternedString(str, true));
  }
  str_to_interned_.insert({*interned_string, interned_string});
  return interned_string;
}

}  // namespace valkey_search
