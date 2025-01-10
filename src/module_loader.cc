#include <memory>

#include "src/commands/commands.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace {

vmsdk::module::Options options = {
    .name = "search",
    .info = valkey_search::ModuleInfo,
    .commands =
        {
            {
                .cmd_name = valkey_search::kCreateCommand,
                .permissions = "write deny-oom",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTCreateCmd>,
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = "write",
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
            },
        },
    .on_load =
        [](RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::KeyspaceEventManager::InitInstance(
              std::make_unique<valkey_search::KeyspaceEventManager>());
          valkey_search::ValkeySearch::InitInstance(
              std::make_unique<valkey_search::ValkeySearch>());

          return valkey_search::ValkeySearch::Instance().OnLoad(ctx, argv,
                                                                argc);
        },
    .on_unload =
        [](RedisModuleCtx *ctx,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::ValkeySearch::Instance().OnUnload(ctx);
        },
};
VALKEY_MODULE(options);
}  // namespace
