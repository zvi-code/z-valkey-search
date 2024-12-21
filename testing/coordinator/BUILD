load("//vmsdk:redis_module_cc_test.bzl", "redis_module_cc_test")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//testing:__subpackages__"],
)

redis_module_cc_test(
    name = "metadata_manager_test",
    srcs = ["metadata_manager_test.cc"],
    tags = [
        "no_opt_test",
    ],
    deps = [
        ":common",
        "//src/coordinator:client",
        "//src/coordinator:client_pool",
        "//src/coordinator:coordinator_cc_proto",
        "//src/coordinator:metadata_manager",
        "//src/coordinator:util",
        "//testing:common",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:redis_module_sdk",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "common",
    testonly = True,
    hdrs = ["common.h"],
    deps = [
        "//src/coordinator:client",
        "//src/coordinator:client_pool",
        "//src/coordinator:server",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest",
    ],
)
