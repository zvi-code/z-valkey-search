# Copyright (c) 2025, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause
#
# CMake configuration for the SIMD metrics dispatch system.
# Each implementation file is compiled with its own -march flags.

# Core dispatch files - no special flags needed
set(SIMD_CORE_SOURCES
    cpu_caps.cc
    simd_dispatch.cc
)

# Scalar baseline - no SIMD flags
set(SIMD_SCALAR_SOURCES
    impl/scalar/metrics_scalar.cc
)

# ARM64 sources with per-file flags
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(SIMD_ARM64_SOURCES
        impl/arm64/metrics_neon.cc
        impl/arm64/metrics_neon_dotprod.cc
        impl/arm64/metrics_sve.cc
    )

    # Set per-file compile flags for ARM64
    # NEON baseline: armv8-a (default on aarch64, no extra flags needed)
    set_source_files_properties(impl/arm64/metrics_neon.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8-a+fp+simd"
    )

    # NEON + dotprod: armv8.2-a with dot product extension (Graviton2+)
    set_source_files_properties(impl/arm64/metrics_neon_dotprod.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+dotprod"
    )

    # SVE: armv8.2-a with SVE (Graviton3+)
    # Note: This file will compile even without hardware SVE support,
    # as it has fallback stubs. Runtime detection determines usage.
    set_source_files_properties(impl/arm64/metrics_sve.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+sve"
    )
endif()

# x86-64 sources with per-file flags
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(SIMD_X86_SOURCES
        impl/x86/metrics_avx2.cc
        impl/x86/metrics_avx512.cc
    )

    # AVX2 + FMA + F16C (Haswell+, most modern x86-64)
    set_source_files_properties(impl/x86/metrics_avx2.cc
        PROPERTIES COMPILE_FLAGS "-mavx2 -mfma -mf16c"
    )

    # AVX-512F + BW + VL (Skylake-X, Ice Lake, Sapphire Rapids)
    set_source_files_properties(impl/x86/metrics_avx512.cc
        PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl"
    )
endif()

# Combine all sources
set(SIMD_SOURCES
    ${SIMD_CORE_SOURCES}
    ${SIMD_SCALAR_SOURCES}
    ${SIMD_ARM64_SOURCES}
    ${SIMD_X86_SOURCES}
)

# Create the simd_metrics library
add_library(simd_metrics STATIC ${SIMD_SOURCES})

target_include_directories(simd_metrics
    PUBLIC
        ${CMAKE_SOURCE_DIR}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link pthread for cpu_caps.cc (std::call_once)
target_link_libraries(simd_metrics
    PRIVATE
        pthread
)

# Ensure proper C++ standard
target_compile_features(simd_metrics PUBLIC cxx_std_17)

# Optional: Enable LTO for release builds
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set_target_properties(simd_metrics PROPERTIES
        INTERPROCEDURAL_OPTIMIZATION TRUE
    )
endif()

# Export the target for use by parent CMakeLists.txt
set(SIMD_METRICS_LIBRARY simd_metrics PARENT_SCOPE)
