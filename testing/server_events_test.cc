#include "src/server_events.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "testing/common.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::server_events {
#ifndef BAZEL_BUILD
// SubscribesToServerEvents leads to segfault due to a call to pthread_atfork
class MockPthreadAtfork {
 public:
  MOCK_METHOD(int, pthread_atfork,
              (void (*__prepare)(), void (*__parent)(), void (*__child)()), ());
};
MockPthreadAtfork mock_pthread_atfork;

int pthread_atfork(void (*prepare)(), void (*parent)(), void (*child)()) {
  return mock_pthread_atfork.pthread_atfork(prepare, parent, child);
}
#endif  // BAZEL_BUILD

class ServerEventsTest : public ValkeySearchTest {};

TEST_F(ServerEventsTest, SubscribesToServerEvents) {
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsRedisModuleEvent(RedisModuleEvent_CronLoop),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsRedisModuleEvent(RedisModuleEvent_ForkChild),
          testing::_))
      .WillOnce(testing::Return(1));
#ifndef BAZEL_BUILD
  EXPECT_CALL(mock_pthread_atfork,
              pthread_atfork(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(1));
#endif  // BAZEL_BUILD
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(testing::_,
                             vmsdk::IsRedisModuleEvent(RedisModuleEvent_SwapDB),
                             testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsRedisModuleEvent(RedisModuleEvent_Loading),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsRedisModuleEvent(RedisModuleEvent_FlushDB),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockRedisModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsRedisModuleEvent(RedisModuleEvent_Persistence),
          testing::_))
      .WillOnce(testing::Return(1));
  SubscribeToServerEvents();
}

}  // namespace valkey_search::server_events
