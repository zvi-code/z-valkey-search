# Metrics library - SIMD-optimized similarity metrics with one-time vtable dispatch

# Core sources (always compiled)
set(METRICS_CORE_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/cpu_caps.cc
    ${CMAKE_CURRENT_LIST_DIR}/metrics_dispatch.cc
    ${CMAKE_CURRENT_LIST_DIR}/impl/scalar/metrics_scalar.cc
)

# x86_64 SIMD implementations
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(METRICS_X86_64_SOURCES
        ${CMAKE_CURRENT_LIST_DIR}/impl/x86_64/metrics_avx2.cc
        ${CMAKE_CURRENT_LIST_DIR}/impl/x86_64/metrics_avx512.cc
    )
endif()

# ARM64 SIMD implementations
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(METRICS_ARM64_SOURCES
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon.cc
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon_dotprod.cc
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon_i8mm.cc
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_sve.cc
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_sve2.cc
    )
endif()

# Combine all sources
set(METRICS_SOURCES
    ${METRICS_CORE_SOURCES}
    ${METRICS_X86_64_SOURCES}
    ${METRICS_ARM64_SOURCES}
)

# Create the static library
valkey_search_add_static_library(valkey_metrics "${METRICS_SOURCES}")
target_include_directories(valkey_metrics PUBLIC ${CMAKE_CURRENT_LIST_DIR})

# Per-file compile flags for x86_64 SIMD implementations
# These override the default flags to enable specific instruction sets
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/x86_64/metrics_avx2.cc
        PROPERTIES COMPILE_FLAGS "-mavx2 -mfma"
    )
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/x86_64/metrics_avx512.cc
        PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512dq -mavx512bw -mavx512vl"
    )
endif()

# Per-file compile flags for ARM64 SIMD implementations
# Each file is compiled with the minimum -march required for its instructions
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8-a"
    )
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon_dotprod.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+dotprod"
    )
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_neon_i8mm.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+dotprod+i8mm"
    )
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_sve.cc
        PROPERTIES COMPILE_FLAGS "-march=armv8.2-a+sve"
    )
    set_source_files_properties(
        ${CMAKE_CURRENT_LIST_DIR}/impl/arm64/metrics_sve2.cc
        PROPERTIES COMPILE_FLAGS "-march=armv9-a+sve2+i8mm"
    )
endif()
