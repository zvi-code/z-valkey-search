#ifndef VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_
#define VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_

#include <string>

#include "absl/status/status.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.pb.h"

namespace valkey_search {
inline grpc::Status ToGrpcStatus(const absl::Status& status) {
  if (status.ok()) {
    return grpc::Status::OK;
  }
  return {static_cast<grpc::StatusCode>(status.code()),
          std::string(status.message())};
}
namespace coordinator {
// This offset results in 26673 for Redis default port 6379 - which is COORD
// on a telephone keypad.
static constexpr int kCoordinatorPortOffset = 20294;

inline int GetCoordinatorPort(int redis_port) {
  // TODO(jkmurphy) Make handling of TLS more robust
  if (redis_port == 6378) {
    return redis_port + kCoordinatorPortOffset + 1;
  }
  return redis_port + kCoordinatorPortOffset;
}
}  // namespace coordinator

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_
