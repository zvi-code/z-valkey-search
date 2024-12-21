package(
    default_applicable_licenses = ["//vmsdk:license"],
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "status_macros",
    srcs = ["status_builder.cc"],
    hdrs = [
        "source_location.h",
        "status_builder.h",
        "status_macros.h",
    ],
    deps = [
        "//vmsdk/src:redis_module_sdk",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:log_severity",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)
