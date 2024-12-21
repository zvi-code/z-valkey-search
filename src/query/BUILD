package(
    default_applicable_licenses = ["//:license"],
    default_visibility = [
        "//:__subpackages__",
    ],
)

cc_library(
    name = "predicate",
    srcs = ["predicate.cc"],
    hdrs = [
        "predicate.h",
    ],
    deps = [
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "predicate_header",
    hdrs = [
        "predicate.h",
    ],
    deps = [
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "search",
    srcs = ["search.cc"],
    hdrs = [
        "search.h",
    ],
    deps = [
        ":planner",
        ":predicate",
        "//src:attribute_data_type",
        "//src:index_schema",
        "//src:metrics",
        "//src/commands:filter_parser",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/indexes:vector_base",
        "//src/indexes:vector_flat",
        "//src/indexes:vector_hnsw",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:latency_sampler",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:redis_module_sdk",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:time_sliced_mrmw_mutex",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "search_header",
    hdrs = [
        "search.h",
    ],
    deps = [
        ":predicate_header",
        "//src:index_schema",
        "//src/commands:filter_parser",
        "//src/indexes:index_base",
        "//src/indexes:vector_base",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:thread_pool",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_library(
    name = "fanout",
    srcs = ["fanout.cc"],
    hdrs = ["fanout.h"],
    deps = [
        ":search",
        "//src:attribute_data_type",
        "//src:index_schema",
        "//src/coordinator:client_pool",
        "//src/coordinator:coordinator_cc_proto",
        "//src/coordinator:search_converter",
        "//src/coordinator:util",
        "//src/indexes:vector_base",
        "//src/utils:string_interning",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:redis_module_sdk",
        "//vmsdk/src:thread_pool",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "response_generator",
    srcs = ["response_generator.cc"],
    hdrs = ["response_generator.h"],
    deps = [
        ":predicate_header",
        ":search_header",
        "//src:attribute_data_type",
        "//src/coordinator:coordinator_cc_proto",
        "//src/indexes:tag",
        "//src/indexes:vector_base",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:redis_module_sdk",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "planner",
    srcs = ["planner.cc"],
    hdrs = [
        "planner.h",
    ],
    deps = [
        "//src/indexes:index_base",
        "//src/indexes:vector_base",
        "@com_google_absl//absl/log:check",
    ],
)
