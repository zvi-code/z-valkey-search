#ifndef VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
#define VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_

#include "absl/strings/string_view.h"
#include "absl/status/status.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr absl::string_view kCreateCommand{"FT.CREATE"};
constexpr absl::string_view kDropIndexCommand{"FT.DROPINDEX"};
constexpr absl::string_view kInfoCommand{"FT.INFO"};
constexpr absl::string_view kListCommand{"FT._LIST"};
constexpr absl::string_view kSearchCommand{"FT.SEARCH"};

absl::Status FTCreateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
absl::Status FTDropIndexCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
absl::Status FTInfoCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
absl::Status FTListCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
absl::Status FTSearchCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
