#pragma once
#include <assert.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "hnswlib.h"
#include "iostream.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
  #include "vmsdk/src/memory_allocation_overrides.h" // IWYU pragma: keep
#endif

namespace hnswlib {
template<typename dist_t>
class BruteforceSearch : public AlgorithmInterface<dist_t> {
 public:
    static const unsigned int ENCODING_VERSION = 0;

    std::unique_ptr<ChunkedArray> data_;
    size_t cur_element_count_;
    size_t vector_size_{0};
    const size_t data_ptr_size_ = sizeof(char*);
    DISTFUNC <dist_t> fstdistfunc_;
    void *dist_func_param_;
    std::mutex index_lock;
    const size_t k_elements_per_chunk{10*1024};

    std::unordered_map<labeltype, size_t > dict_external_to_internal;


    BruteforceSearch(SpaceInterface <dist_t> *s)
        : cur_element_count_(0), dist_func_param_(nullptr) {
    }


    BruteforceSearch(SpaceInterface<dist_t> *s, const std::string &location)
        : cur_element_count_(0), dist_func_param_(nullptr) {
        loadIndex(location, s);
    }


    BruteforceSearch(SpaceInterface <dist_t> *s, size_t maxElements) {
        cur_element_count_ = 0;
        vector_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();
        data_ = std::make_unique<ChunkedArray>(
                data_ptr_size_ + sizeof(labeltype),
                k_elements_per_chunk,
                maxElements);
    }


    void addPoint(const void *datapoint, labeltype label, bool replace_deleted = false) {
        int idx;
        std::unique_lock<std::mutex> lock(index_lock);
        auto search = dict_external_to_internal.find(label);
        if (search != dict_external_to_internal.end()) {
            idx = search->second;
        } else {
            if (cur_element_count_ >= data_->getCapacity()) {
                throw std::runtime_error("The number of elements exceeds the specified limit\n");
            }
            idx = cur_element_count_;
            dict_external_to_internal[label] = idx;
            cur_element_count_++;
        }
        memcpy((*data_)[idx] + data_ptr_size_, &label, sizeof(labeltype));
        *(char**)((*data_)[idx]) = (char*)datapoint;
    }

    char *getPoint(labeltype cur_external) {
      auto found = dict_external_to_internal.find(cur_external);
      if (found == dict_external_to_internal.end()) {
        return nullptr;
      }
      return *(char **)(*data_)[found->second];
    }

    void removePoint(labeltype cur_external) {
        std::unique_lock<std::mutex> lock(index_lock);

        auto found = dict_external_to_internal.find(cur_external);
        if (found == dict_external_to_internal.end()) {
            return;
        }
        // Fixing a bug - found->second value must be fetched before it's erased
        size_t cur_c = found->second;
        dict_external_to_internal.erase(found);
        if (cur_element_count_ - 1 == cur_c) {
          cur_element_count_--;
          return;
        }

        labeltype label = *((labeltype*)((*data_)[cur_element_count_-1] + data_ptr_size_));
        dict_external_to_internal[label] = cur_c;
        memcpy((*data_)[cur_c],
                (*data_)[cur_element_count_-1],
                data_ptr_size_ + sizeof(labeltype));
        cur_element_count_--;
    }


    std::priority_queue<std::pair<dist_t, labeltype >>
    searchKnn(const void *query_data, size_t k, BaseFilterFunctor* isIdAllowed = nullptr) const {
        assert(k <= cur_element_count_);
        std::priority_queue<std::pair<dist_t, labeltype >> topResults;
        if (cur_element_count_ == 0) return topResults;
        for (int i = 0; i < k; i++) {
            dist_t dist = fstdistfunc_(query_data, *(char**)(*data_)[i], dist_func_param_);
            labeltype label = *((labeltype*) ((*data_)[i] + data_ptr_size_));
            if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                topResults.emplace(dist, label);
            }
        }
        dist_t lastdist = topResults.empty() ? std::numeric_limits<dist_t>::max() : topResults.top().first;
        for (int i = k; i < cur_element_count_; i++) {
            dist_t dist = fstdistfunc_(query_data, *(char**)(*data_)[i], dist_func_param_);
            if (dist <= lastdist) {
                labeltype label = *((labeltype *) ((*data_)[i] + data_ptr_size_));
                if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                    topResults.emplace(dist, label);
                }
                if (topResults.size() > k)
                    topResults.pop();

                if (!topResults.empty()) {
                    lastdist = topResults.top().first;
                }
            }
        }
        return topResults;
    }


    void saveIndex(const std::string &location) {
      absl::StatusOr<std::unique_ptr<FileOutputStream>> output_or =
          FileOutputStream::Create(location);
      if (!output_or.ok()) {
        throw std::runtime_error(
            absl::StrCat("Error instantiating FileOutputStream ",
                         output_or.status().message()));
      }
      absl::Status status = saveIndex(*output_or.value());
      if (!status.ok()) {
        throw std::runtime_error(
            absl::StrCat("Error saving index ", status.message()));
      }
    }

    absl::Status saveIndex(OutputStream &output) {
      VMSDK_RETURN_IF_ERROR(output.saveUnsigned(ENCODING_VERSION));
      VMSDK_RETURN_IF_ERROR(output.saveSizeT(data_->getCapacity()));
      const size_t size_per_element = vector_size_ + sizeof(labeltype);
      VMSDK_RETURN_IF_ERROR(output.saveSizeT(size_per_element));
      VMSDK_RETURN_IF_ERROR(output.saveSizeT(cur_element_count_));

      // TODO: write in chunks to improve throughput
      char buf[size_per_element];
      for (int i = 0; i < cur_element_count_; i++) {
        memcpy(buf, *(char **)(*data_)[i], vector_size_);
        memcpy(buf + vector_size_, (*data_)[i] + sizeof(char*),
               sizeof(labeltype));
         VMSDK_RETURN_IF_ERROR(output.saveStringBuffer(buf, size_per_element));
      }
      return absl::OkStatus();
    }

    void loadIndex(const std::string &location, SpaceInterface<dist_t> *s) {
      absl::StatusOr<std::unique_ptr<FileInputStream>> input_or =
          FileInputStream::Create(location);
      if (!input_or.ok()) {
        throw std::runtime_error(
            absl::StrCat("Error instantiating FileInputStream ",
                         input_or.status().message()));
      }
      absl::Status status = loadIndex(*input_or.value(), s);
      if (!status.ok()) {
        throw std::runtime_error(
            absl::StrCat("Error loading index ", status.message()));
      }
    }

    absl::Status loadIndex(InputStream &input, SpaceInterface<dist_t> *s,
                           VectorTracker* vector_tracker) {
      if (data_ != nullptr) {
        data_->clear();
      }

      unsigned int encoding_version;
      VMSDK_RETURN_IF_ERROR(input.loadUnsigned(encoding_version));
      if (encoding_version != ENCODING_VERSION) {
        return absl::InvalidArgumentError(
            absl::StrCat("Unsupported Bruteforce index encoding version ",
                         encoding_version));
      }

      size_t maxelements, size_per_element;

      VMSDK_RETURN_IF_ERROR(input.loadSizeT(maxelements));
      VMSDK_RETURN_IF_ERROR(input.loadSizeT(size_per_element));
      VMSDK_RETURN_IF_ERROR(input.loadSizeT(cur_element_count_));

      vector_size_ = s->get_data_size();
      fstdistfunc_ = s->get_dist_func();
      dist_func_param_ = s->get_dist_func_param();

      if (size_per_element != s->get_data_size() + sizeof(labeltype)) {
        throw std::runtime_error("Persisted size_per_element does not match expectation.");
      }

      data_ = std::make_unique<ChunkedArray>(
              data_ptr_size_ + sizeof(labeltype),
              k_elements_per_chunk,
              maxelements
              );

      for (int i = 0; i < cur_element_count_; i++) {
         VMSDK_ASSIGN_OR_RETURN(StringBufferUniquePtr buf,
                               input.loadStringBuffer(size_per_element));
        labeltype id;
        memcpy((char *)&id,
          buf.get() + vector_size_, sizeof(labeltype));
        *(char **)(*data_)[i] =
                vector_tracker->TrackVector(id, buf.get(), vector_size_);
        memcpy((*data_)[i] + data_ptr_size_,
               (char *)&id, sizeof(labeltype));
        dict_external_to_internal[id] = i;
      }

      return absl::OkStatus();
    }

    void resizeIndex(size_t new_max_elements) {
        data_->resize(new_max_elements);
    }
};
}  // namespace hnswlib
