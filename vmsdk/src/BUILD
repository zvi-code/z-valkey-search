package(
    default_applicable_licenses = ["//vmsdk:license"],
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "log",
    srcs = ["log.cc"],
    hdrs = ["log.h"],
    features = ["-use_header_modules"],
    deps = [
        ":managed_pointers",
        ":redis_module_sdk",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/log:globals",
        "@com_google_absl//absl/log:log_entry",
        "@com_google_absl//absl/log:log_sink",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    copts = ["-fexceptions"],
    features = ["-use_header_modules"],
    deps = [
        ":redis_module_sdk",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "latency_sampler",
    hdrs = ["latency_sampler.h"],
    deps = [
        ":utils",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "redis_module_sdk",
    hdrs = ["redismodule.h"],
)

cc_library(
    name = "valkey_module_sdk",
    hdrs = [
        "valkeymodule.h",
        "valkeymodule_g.h",
    ],
)

cc_library(
    name = "module_type",
    srcs = ["module_type.cc"],
    hdrs = ["module_type.h"],
    deps = [
        ":log",
        ":managed_pointers",
        ":redis_module_sdk",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "thread_pool",
    srcs = ["thread_pool.cc"],
    hdrs = ["thread_pool.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "time_sliced_mrmw_mutex",
    srcs = ["time_sliced_mrmw_mutex.cc"],
    hdrs = ["time_sliced_mrmw_mutex.h"],
    deps = [
        ":utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "command_parser",
    hdrs = ["command_parser.h"],
    deps = [
        ":managed_pointers",
        ":redis_module_sdk",
        ":type_conversions",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "module",
    srcs = ["module.cc"],
    hdrs = ["module.h"],
    deps = [
        ":log",
        ":memory_allocation",
        ":redis_module_sdk",
        ":utils",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "memory_allocation_overrides",
    srcs = [
        "memory_allocation_overrides.cc",
    ],
    hdrs = [
        "memory_allocation_overrides.h",
    ],
    deps = [
        ":memory_allocation",
        ":redis_module_sdk",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "memory_allocation",
    srcs = [
        "memory_allocation.cc",
    ],
    hdrs = [
        "memory_allocation.h",
    ],
)

cc_library(
    name = "type_conversions",
    hdrs = [
        "type_conversions.h",
    ],
    deps = [
        ":redis_module_sdk",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "managed_pointers",
    hdrs = [
        "managed_pointers.h",
    ],
    deps = [
        ":redis_module_sdk",
        ":utils",
        "@com_google_absl//absl/strings:string_view",
    ],
)
