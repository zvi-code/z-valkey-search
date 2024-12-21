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

#ifndef VALKEYSEARCH_SRC_METRICS_H_
#define VALKEYSEARCH_SRC_METRICS_H_

#include <atomic>
#include <cstdint>

#include "absl/time/time.h"
#include "vmsdk/src/latency_sampler.h"

// 2 is the value used by Redis and correlates to ~40KiB and ~1% precision.
#define LATENCY_PRECISION 2

namespace valkey_search {
class Metrics {
 public:
  static Metrics& GetInstance() {
    static Metrics instance;
    return instance;
  }
  ~Metrics() = default;

  struct Stats {
    uint64_t query_successful_requests_cnt{0};
    uint64_t query_failed_requests_cnt{0};
    uint64_t query_hybrid_requests_cnt{0};
    uint64_t query_inline_filtering_requests_cnt{0};
    std::atomic<uint64_t> hnsw_add_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_remove_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_modify_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_search_exceptions_cnt{0};
    std::atomic<uint64_t> hnsw_create_exceptions_cnt{0};
    std::atomic<uint64_t> flat_add_exceptions_cnt{0};
    std::atomic<uint64_t> flat_remove_exceptions_cnt{0};
    std::atomic<uint64_t> flat_modify_exceptions_cnt{0};
    std::atomic<uint64_t> flat_search_exceptions_cnt{0};
    std::atomic<uint64_t> flat_create_exceptions_cnt{0};
    std::atomic<uint64_t> worker_thread_pool_suspend_cnt{0};
    std::atomic<uint64_t> writer_worker_thread_pool_resumed_cnt{0};
    std::atomic<uint64_t> reader_worker_thread_pool_resumed_cnt{0};
    std::atomic<uint64_t> writer_worker_thread_pool_suspension_expired_cnt{0};
    uint64_t rdb_load_success_cnt{0};
    uint64_t rdb_load_failure_cnt{0};
    uint64_t rdb_save_success_cnt{0};
    uint64_t rdb_save_failure_cnt{0};
    vmsdk::LatencySampler hnsw_vector_index_search_latency{
        absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
        absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler flat_vector_index_search_latency{
        absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
        absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    std::atomic<uint64_t> coordinator_server_get_global_metadata_success_cnt{0};
    std::atomic<uint64_t> coordinator_server_get_global_metadata_failure_cnt{0};
    std::atomic<uint64_t> coordinator_server_search_index_partition_success_cnt{
        0};
    std::atomic<uint64_t> coordinator_server_search_index_partition_failure_cnt{
        0};
    std::atomic<uint64_t> coordinator_client_get_global_metadata_success_cnt{0};
    std::atomic<uint64_t> coordinator_client_get_global_metadata_failure_cnt{0};
    std::atomic<uint64_t> coordinator_client_search_index_partition_success_cnt{
        0};
    std::atomic<uint64_t> coordinator_client_search_index_partition_failure_cnt{
        0};
    vmsdk::LatencySampler
        coordinator_client_get_global_metadata_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_search_index_partition_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_get_global_metadata_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_client_search_index_partition_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_get_global_metadata_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_search_index_partition_failure_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_get_global_metadata_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
    vmsdk::LatencySampler
        coordinator_server_search_index_partition_success_latency{
            absl::ToInt64Nanoseconds(absl::Nanoseconds(1)),
            absl::ToInt64Nanoseconds(absl::Seconds(1)), LATENCY_PRECISION};
  };
  static Stats& GetStats() { return GetInstance().stats_; }

 private:
  mutable Stats stats_;
  Metrics() {}
  Metrics(const Metrics&) = delete;
  Metrics& operator=(const Metrics&) = delete;
};
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_METRICS_H_
