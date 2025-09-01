/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <absl/base/no_destructor.h>
#include <absl/strings/ascii.h>

#include "module_config.h"
#include "src/commands/commands.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"

extern vmsdk::module::Options options;  // Declared in module_loader.cc
namespace valkey_search {

//
// FT.DEBUG PAUSEPOINT [ SET | RESET | TEST | LIST] <pausepoint>
//
// Connects to the vmsdk::debug mechanism
// A pausepoint is a mechanism to pause a thread at a specific location for
// testing purposes.
//
// Individual pausepoints are labelled by a unique string. No checking for the
// uniqueness is done. A pause point in the code is enabled by calling
// vmsdk::debug::PausePoint("<string>"); If that pausepoint is not set, then
// this call does nothing. But if the pausepoint is set then the calling thread
// "hangs" at that point.
//
// The TEST option can be used to determine if one or more threads are paused at
// a particular pausepoint, the number of paused threads is returned by that
// command, with 0 indicating that no threads are paused. If threads are paused,
// then by RESETing the pausepoint they are released.
//
// A typical test scenario is to enable a pause point and then trigger some
// background activity, i.e.,
//  a query, ingestion or other background activity. The test program then waits
//  until the background
// thread reaches the pause point, which the test detects by polling the pause
// point with the TEST subcommand. Once the background thread is paused, the
// test can proceed by clearing the pausepoint with the RESET subcommand.
//
// Note, a pausepoint cannot be attempted by the main thread. It's also
// recommended that it not be done while holding a mutex/lock.
//
absl::Status PausePointControlCmd(ValkeyModuleCtx *ctx,
                                  vmsdk::ArgsIterator &itr) {
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "LIST") {
    vmsdk::debug::PausePointList(ctx);
    return absl::OkStatus();
  }
  std::string point;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, point));
  if (keyword == "TEST") {
    auto result = vmsdk::debug::PausePointWaiters(point);
    if (result.ok()) {
      ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*result));
    } else {
      ValkeyModule_ReplyWithSimpleString(ctx, result.status().message().data());
    }
  } else if (keyword == "SET" || keyword == "RESET") {
    vmsdk::debug::PausePointControl(point, keyword == "SET");
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithError(
        ctx, absl::StrCat("Unknown keyword", keyword).data());
  }
  return absl::OkStatus();
}

absl::Status FTDebugCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  if (!vmsdk::config::IsDebugModeEnabled()) {
    // Pretend like we don't exist
    std::ostringstream msg;
    msg << "ERR unknown command '" << vmsdk::ToStringView(argv[0])
        << "', with args beginning with:";
    for (int i = 1; i < argc; ++i) {
      msg << " '" << vmsdk::ToStringView(argv[i]) << "'";
    }
    ValkeyModule_ReplyWithError(ctx, msg.str().data());
    return absl::OkStatus();
  }
  std::string msg;
  for (int i = 1; i < argc; ++i) {
    msg += " ";
    msg += std::string(vmsdk::ToStringView(argv[i]));
  }
  VMSDK_LOG(WARNING, ctx) << "FT._DEBUG" << msg;
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "SHOW_INFO") {
    return vmsdk::info_field::ShowInfo(ctx, itr, options);
  } else if (keyword == "PAUSEPOINT") {
    return PausePointControlCmd(ctx, itr);
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("Unknown subcommand: ", *itr.GetStringView()));
  }
}

}  // namespace valkey_search