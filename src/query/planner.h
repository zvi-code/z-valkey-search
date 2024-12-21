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

#ifndef VALKEYSEARCH_SRC_QUERY_PLANNER_H_
#define VALKEYSEARCH_SRC_QUERY_PLANNER_H_

#include "src/indexes/vector_base.h"

namespace valkey_search::query {

// Returns whether to use pre-filtering as opposed to inline filtering based on
// heuristics.
bool UsePreFiltering(size_t estimated_num_of_keys,
                     indexes::VectorBase *vector_index);
}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_PLANNER_H_
