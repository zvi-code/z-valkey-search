package(
    default_applicable_licenses = ["//vmsdk:license"],
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "module",
    testonly = True,
    hdrs = ["module.h"],
    deps = [
        "//vmsdk/src:log",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "utils",
    testonly = True,
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        ":module",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest",
    ],
)
