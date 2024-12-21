licenses(["notice"])

# hdr_test_util.h isn't parse_headers compatible
package(features = ["-parse_headers"])

cc_test(
    name = "hdr_atomic_test",
    srcs = ["hdr_atomic_test.c"],
    deps = [
        ":minunit",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
    ],
)

cc_test(
    name = "hdr_histogram_atomic_concurrency_test",
    srcs = [
        "hdr_histogram_atomic_concurrency_test.c",
        "hdr_test_util.h",
    ],
    deps = [
        ":minunit",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
    ],
)

cc_test(
    name = "hdr_histogram_atomic_test",
    srcs = [
        "hdr_histogram_atomic_test.c",
        "hdr_test_util.h",
    ],
    deps = [
        ":minunit",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
    ],
)

cc_test(
    name = "hdr_histogram_log_test",
    srcs = ["hdr_histogram_log_test.c"],
    data = [
        "hiccup.140623.1028.10646.hlog",
        "jHiccup-2.0.1.logV0.hlog",
        "jHiccup-2.0.6.logV1.hlog",
        "jHiccup-2.0.7S.logV2.hlog",
        "jHiccup-2.0.7S.logV3.hlog",
    ],
    deps = [
        ":minunit",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
    ],
)

cc_binary(
    name = "hdr_histogram_perf",
    srcs = ["hdr_histogram_perf.c"],
    deps = ["//third_party/hdrhistogram_c/src:hdrhistogram_c"],
)

cc_test(
    name = "hdr_histogram_test",
    srcs = [
        "hdr_histogram_test.c",
        "hdr_test_util.h",
    ],
    deps = [
        ":minunit",
        "//third_party/hdrhistogram_c/src:hdrhistogram_c",
    ],
)

cc_library(
    name = "minunit",
    srcs = ["minunit.c"],
    hdrs = ["minunit.h"],
)
