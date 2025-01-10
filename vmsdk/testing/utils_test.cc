#include "vmsdk/src/utils.h"
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/synchronization/blocking_counter.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"

namespace vmsdk {

namespace {

class UtilsTest : public vmsdk::RedisTest {};

TEST_F(UtilsTest, RunByMain) {
  absl::BlockingCounter blocking_refcount(1);
  ThreadPool thread_pool("test-pool", 1);
  thread_pool.StartWorkers();
  RedisModuleEventLoopOneShotFunc captured_callback;
  void *captured_data;
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([&](RedisModuleEventLoopOneShotFunc callback, void *data) {
        captured_callback = callback;
        captured_data = data;
        blocking_refcount.DecrementCount();
        return 0;
      });
  bool run = false;
  EXPECT_TRUE(thread_pool.Schedule(
      [&]() {
        RunByMain([&run] {
          EXPECT_TRUE(IsMainThread());
          run = true;
        });
      },
      ThreadPool::Priority::kLow));
  blocking_refcount.Wait();
  captured_callback(captured_data);
  EXPECT_TRUE(run);
  thread_pool.JoinWorkers();
}

TEST_F(UtilsTest, RunByMainWhileInMain) {
  absl::BlockingCounter blocking_refcount(1);
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
      .Times(0);
  bool run = false;
  RunByMain([&blocking_refcount, &run] {
    EXPECT_TRUE(IsMainThread());
    blocking_refcount.DecrementCount();
    run = true;
  });
  blocking_refcount.Wait();
  EXPECT_TRUE(run);
}

}  // namespace

}  // namespace vmsdk
