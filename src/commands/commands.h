#ifndef VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
#define VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_

#include <string>

#include "absl/status/status.h"
#include "vmsdk/src/redismodule.h"

namespace valkey_search {

constexpr std::string kCreateCommand{"FT.CREATE"};
constexpr std::string kDropIndexCommand{"FT.DROPINDEX"};
constexpr std::string kInfoCommand{"FT.INFO"};
constexpr std::string kListCommand{"FT._LIST"};
constexpr std::string kSearchCommand{"FT.SEARCH"};

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
