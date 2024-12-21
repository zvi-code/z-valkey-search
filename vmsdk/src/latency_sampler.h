#ifndef VMSDK_SRC_LATENCY_SAMPLER_H_
#define VMSDK_SRC_LATENCY_SAMPLER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "third_party/hdrhistogram_c/src/hdr_histogram.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

// LatencySampler provides a mechanism for tracking latency samples in a
// histogram. It is lazily allocated so it will not take any memory unless
// samples are added.
class LatencySampler {
 public:
  LatencySampler(int64_t min_value, int64_t max_value, int precision)
      : min_value_(min_value), max_value_(max_value), precision_(precision) {}
  ~LatencySampler() {
    if (initialized_) hdr_close(histogram_);
  }

  void SubmitSample(std::unique_ptr<vmsdk::StopWatch> sample) {
    if (!sample) {
      return;
    }
    SubmitSample(sample->Duration());
  }
  void SubmitSample(absl::Duration latency) {
    absl::MutexLock lock(&histogram_lock_);
    if (!initialized_) {
      hdr_init(min_value_, max_value_, precision_, &histogram_);
      initialized_ = true;
    }
    hdr_record_value(histogram_, absl::ToInt64Nanoseconds(latency));
  }
  bool HasSamples() {
    absl::MutexLock lock(&histogram_lock_);
    return initialized_;
  }
  std::string GetStatsString() {
    absl::MutexLock lock(&histogram_lock_);
    double p50 = 0;
    double p99 = 0;
    double p999 = 0;
    if (initialized_) {
      double nano_to_micro = absl::ToDoubleMicroseconds(absl::Nanoseconds(1));
      p50 = hdr_value_at_percentile(histogram_, 50) * nano_to_micro;
      p99 = hdr_value_at_percentile(histogram_, 99) * nano_to_micro;
      p999 = hdr_value_at_percentile(histogram_, 99.9) * nano_to_micro;
    }
    return absl::StrFormat("p50=%.3f,p99=%.3f,p99.9=%.3f", p50, p99, p999);
  }

 private:
  mutable absl::Mutex histogram_lock_;
  int64_t min_value_;
  int64_t max_value_;
  int precision_;
  bool initialized_ ABSL_GUARDED_BY(histogram_lock_) = false;
  hdr_histogram *histogram_ ABSL_GUARDED_BY(histogram_lock_);
};

#define SAMPLE_EVERY_N(interval)          \
  []() -> std::unique_ptr<vmsdk::StopWatch> {      \
    thread_local uint64_t counter = 0;             \
    if (counter++ % interval == 0) {               \
      return std::make_unique<vmsdk::StopWatch>(); \
    }                                              \
    return nullptr;                                \
  }()

}  // namespace vmsdk

#endif  // VMSDK_SRC_LATENCY_SAMPLER_H_
