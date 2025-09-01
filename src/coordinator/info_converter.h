#ifndef VALKEYSEARCH_SRC_COORDINATOR_INFO_CONVERTER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_INFO_CONVERTER_H_

#include <memory>
#include <string>

#include "src/coordinator/coordinator.pb.h"

namespace valkey_search::coordinator {

std::unique_ptr<InfoIndexPartitionRequest> CreateInfoIndexPartitionRequest(
    const std::string& index_name, uint64_t timeout_ms = 5000);

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_INFO_CONVERTER_H_
