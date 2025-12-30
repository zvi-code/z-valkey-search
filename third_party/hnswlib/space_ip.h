#pragma once
#include "hnswlib.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
  #include "vmsdk/src/memory_allocation_overrides.h" // IWYU pragma: keep
#endif

#include "src/metrics/metrics.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
namespace hnswlib {

// Wrapper function that adapts the new metrics API to HNSW's DISTFUNC signature
// Returns 1.0 - inner_product to convert similarity to distance
static float
InnerProductDistanceMetrics(const void *pVect1v, const void *pVect2v, const void *qty_ptr) {
    size_t dim = *static_cast<const size_t*>(qty_ptr);
    const float* vec1 = static_cast<const float*>(pVect1v);
    const float* vec2 = static_cast<const float*>(pVect2v);
    return 1.0f - valkey_search::metrics::IpF32(vec1, vec2, dim);
}

class InnerProductSpace : public SpaceInterface<float> {
    DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    InnerProductSpace(size_t dim) {
        fstdistfunc_ = InnerProductDistanceMetrics;
        dim_ = dim;
        data_size_ = dim * sizeof(float);
    }

    size_t get_data_size() {
        return data_size_;
    }

    DISTFUNC<float> get_dist_func() {
        return fstdistfunc_;
    }

    void *get_dist_func_param() {
        return &dim_;
    }

    ~InnerProductSpace() {}
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
