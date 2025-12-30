#ifndef THIRD_PARTY_HNSWLIB_SIMSIMD_H_
#define THIRD_PARTY_HNSWLIB_SIMSIMD_H_

#include <cstddef>

// Use new simd_metrics vtable dispatch instead of SimSIMD's lazy dispatch
#include "src/simd_metrics/simd_metrics.h"

inline float InnerProductDistanceSimsimd(const void *pVect1, const void *pVect2,
                                         const void *qty_ptr) {
  size_t dim = *static_cast<const size_t *>(qty_ptr);
  const float *vec1 = static_cast<const float *>(pVect1);
  const float *vec2 = static_cast<const float *>(pVect2);
  // Inner product returns dot product, we need 1 - dot for distance
  return 1.0f - metrics_ip_f32(vec1, vec2, dim);
}

inline float L2SqrSimsimd(const void *pVect1, const void *pVect2,
                          const void *qty_ptr) {
  size_t dim = *static_cast<const size_t *>(qty_ptr);
  const float *vec1 = static_cast<const float *>(pVect1);
  const float *vec2 = static_cast<const float *>(pVect2);
  return metrics_l2sq_f32(vec1, vec2, dim);
}

#endif  // THIRD_PARTY_HNSWLIB_SIMSIMD_H_
