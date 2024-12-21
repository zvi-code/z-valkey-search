
package(
    default_visibility = ["//visibility:public"],
)

# Define the header-only library. This does not enable the dynamic dispatch functionality.
cc_library(
    name = "simsimd",
    hdrs = [
        "binary.h",
        "dot.h",
        "geospatial.h",
        "probability.h",
        "simsimd.h",
        "spatial.h",
        "types.h",
    ],
    copts = [
    "-Wno-unused-function",
    "-Wno-unknown-pragmas",
    ],
)
