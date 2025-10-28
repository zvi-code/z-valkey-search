/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_H_

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/query/fanout_template.h"
#include "src/query/search.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query::fanout {

absl::Status PerformSearchFanoutAsync(
    ValkeyModuleCtx* ctx, std::vector<FanoutSearchTarget>& search_targets,
    coordinator::ClientPool* coordinator_client_pool,
    std::unique_ptr<query::SearchParameters> parameters,
    vmsdk::ThreadPool* thread_pool, query::SearchResponseCallback callback);

std::vector<FanoutSearchTarget> GetSearchTargetsForFanout(
    ValkeyModuleCtx* ctx, FanoutTargetMode mode);

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_H_
