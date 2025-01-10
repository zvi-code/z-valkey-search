#ifndef THIRD_PARTY_HNSWLIB_IOSTREAM_H_
#define THIRD_PARTY_HNSWLIB_IOSTREAM_H_

#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

namespace hnswlib {

struct StringBufferDeleter {
  void operator()(char *buf) { free(buf); }
};
using StringBufferUniquePtr = std::unique_ptr<char[], StringBufferDeleter>;

inline StringBufferUniquePtr MakeStringBufferUniquePtr(size_t len) {
  return StringBufferUniquePtr((char *)malloc(len * sizeof(char)));
}
class VectorTracker {
 public:
  virtual ~VectorTracker() = default;
  virtual char *TrackVector(uint64_t internal_id, char *vector, size_t len) = 0;
};

// TODO: use stream operators to align with C++ STL.
// TODO: remove dependency on absl libraries.
class InputStream {
 public:
  virtual ~InputStream() = default;
  virtual absl::Status LoadSizeT(size_t &val) = 0;
  virtual absl::Status LoadUnsigned(unsigned int &val) = 0;
  virtual absl::Status LoadSigned(int &val) = 0;
  virtual absl::Status LoadDouble(double &val) = 0;
  virtual absl::StatusOr<StringBufferUniquePtr> LoadStringBuffer(
      size_t len) = 0;
};

class OutputStream {
 public:
  virtual ~OutputStream() = default;
  virtual absl::Status SaveSizeT(size_t val) = 0;
  virtual absl::Status SaveUnsigned(unsigned int val) = 0;
  virtual absl::Status SaveSigned(int val) = 0;
  virtual absl::Status SaveDouble(double val) = 0;
  virtual absl::Status SaveStringBuffer(const char *str, size_t len) = 0;
};

class FileInputStream : public InputStream {
 public:
  static absl::StatusOr<std::unique_ptr<FileInputStream>> Create(
      const std::string &location) {
    auto input = std::make_unique<std::ifstream>(location, std::ios::binary);
    if (!input->is_open()) return absl::InternalError("Cannot open file");
    return std::make_unique<FileInputStream>(std::move(input));
  }

  FileInputStream(std::unique_ptr<std::ifstream> input)
      : input_(std::move(input)) {}
  ~FileInputStream() { input_->close(); };

  absl::Status LoadSizeT(size_t &val) override { return LoadPOD(val); }
  absl::Status LoadUnsigned(unsigned int &val) override { return LoadPOD(val); }
  absl::Status LoadSigned(int &val) override { return LoadPOD(val); }
  absl::Status LoadDouble(double &val) override { return LoadPOD(val); }

  absl::StatusOr<StringBufferUniquePtr> LoadStringBuffer(
      size_t len) override {
    auto str = MakeStringBufferUniquePtr(len);
    input_->read(str.get(), len);
    if (*input_) return str;
    return absl::InternalError("Error reading string buffer from file");
  }

 private:
  template <typename T>
  absl::Status LoadPOD(T &podRef) {
    input_->read((char *)&podRef, sizeof(T));
    if (*input_) return absl::OkStatus();
    return absl::InternalError("Error reading POD from file");
  }

  std::unique_ptr<std::ifstream> input_;
};

class FileOutputStream : public OutputStream {
 public:
  static absl::StatusOr<std::unique_ptr<FileOutputStream>> Create(
      const std::string &location) {
    auto output_ = std::make_unique<std::ofstream>(location, std::ios::binary);
    if (!output_->is_open()) return absl::InternalError("Cannot open file");
    return std::make_unique<FileOutputStream>(std::move(output_));
  }

  FileOutputStream(std::unique_ptr<std::ofstream> output)
      : output_(std::move(output)) {}
  ~FileOutputStream() { output_->close(); };

  absl::Status SaveSizeT(size_t val) override { return SavePOD(val); }
  absl::Status SaveUnsigned(unsigned int val) override { return SavePOD(val); }
  absl::Status SaveSigned(int val) override { return SavePOD(val); }
  absl::Status SaveDouble(double val) override { return SavePOD(val); }

  absl::Status SaveStringBuffer(const char *str, size_t len) override {
    output_->write(str, len);
    if (*output_) return absl::OkStatus();
    return absl::InternalError("Error writing string buffer to file");
  }

 private:
  template <typename T>
  absl::Status SavePOD(const T val) {
    output_->write((char *)&val, sizeof(T));
    if (*output_) return absl::OkStatus();
    return absl::InternalError("Error writing POD to file");
  }

  std::unique_ptr<std::ofstream> output_;
};

}  // namespace hnswlib

#endif  // THIRD_PARTY_HNSWLIB_IOSTREAM_H_
