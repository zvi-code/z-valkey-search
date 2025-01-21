load("@rules_license//rules:license.bzl", "license")

package(
    default_applicable_licenses = [":license"],
    default_visibility = ["//visibility:public"],
)

license(
    name = "license",
    package_name = "vmsdk",
)

licenses(["notice"])

exports_files(["LICENSE"])

exports_files(srcs = [
    "memory_tracking_exclusions.lds",
    "versionscript.lds",
])
