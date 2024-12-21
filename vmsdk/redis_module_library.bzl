"""
This module contains useful functions and macros for building redis modules.
"""

def redis_module_library(name, deps, additional_link_opts = [], **kwargs):
    """
    Use redis_module_library to build a .so file for to use as a Redis module.

    redis_module_library ensures only necessary symbols are exported and
    prevents linker errors caused by module SDK loading in multiple packages.
    The output will be statically linked to ensure consistency.

    Input "name" should match format "libgoogle-X.so".
    """
    native.cc_binary(
        name = name,
        # TODO(b/317402988) Investigate remaining dynamic linkages.
        linkopts = [
            # The Redis module SDK defines many global variables in the header file
            # which violates One Definition Rule (ODR). This causes linker errors
            # if we don't allow multiple definitions.
            "-z muldefs",
            # Prevent accidental symbol collisions by only exporting necesasry
            # symbols. See yaqs.corp.google.com/eng/q/3879504732816932864.
            "-Wl,--version-script=$(location //vmsdk:versionscript.lds)",
        ] + additional_link_opts + select({
            "//conditions:default": [
                # Disallow undefined symbols in object files.
                "-z defs",
            ],
        }),
        linkshared = 1,
        deps = [
            "//vmsdk:versionscript.lds",
            "//vmsdk/src:memory_allocation_overrides",
        ] + deps,
        **kwargs
    )
