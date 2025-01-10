load("//vmsdk:redis_module_cc_test.bzl", "redis_module_cc_test")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//testing:__subpackages__"],
)

redis_module_cc_test(
    name = "fanout_test",
    srcs = ["fanout_test.cc"],
    deps = [
        "//src:index_schema_cc_proto",
        "//src:valkey_search",
        "//src/coordinator:coordinator_cc_proto",
        "//src/coordinator:search_converter",
        "//src/coordinator:util",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/query:fanout",
        "//testing:common",
        "//testing/coordinator:common",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

redis_module_cc_test(
    name = "response_generator_test",
    srcs = ["response_generator_test.cc"],
    deps = [
        "//src:attribute_data_type",
        "//src/indexes:vector_base",
        "//src/query:predicate",
        "//src/query:response_generator",
        "//src/query:search",
        "//src/utils:string_interning",
        "//testing:common",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/testing_infra:module",
        "//vmsdk/src/testing_infra:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
