#pragma once

#include <assert.h>
#include <stdlib.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <list>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "hnswlib.h"
#include "iostream.h"
#include "src/metrics.h"
#include "third_party/hnswlib/index.pb.h"
#include "visited_list_pool.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define htole64(x) OSSwapHostToLittleInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)
#endif

namespace hnswlib {
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;

template <typename dist_t>
class HierarchicalNSW : public AlgorithmInterface<dist_t> {
 public:
  static const tableint MAX_LABEL_OPERATION_LOCKS = 65536;
  static const unsigned char DELETE_MARK = 0x01;
  static const unsigned int ENCODING_VERSION = 0;

  size_t max_elements_{0};
  mutable std::atomic<size_t> cur_element_count_{
      0};  // current number of elements
  size_t size_data_per_element_{0};
  size_t serialize_size_data_per_element_{0};
  size_t size_links_per_element_{0};
  mutable std::atomic<size_t> num_deleted_{0};  // number of deleted elements
  size_t M_{0};
  size_t maxM_{0};
  size_t maxM0_{0};
  size_t ef_construction_{0};
  size_t ef_{0};
  const size_t k_elements_per_chunk{10 * 1024};

  double mult_{0.0}, revSize_{0.0};
  int maxlevel_{0};

  std::unique_ptr<VisitedListPool> visited_list_pool_{nullptr};

  // Locks operations with element by label value
  mutable std::vector<std::mutex> label_op_locks_;

  std::mutex global;
  std::vector<std::mutex> link_list_locks_;

  tableint enterpoint_node_{0};

  size_t size_links_level0_{0};
  size_t offsetData_{0}, offsetLevel0_{0}, label_offset_{0};

  std::unique_ptr<ChunkedArray> data_level0_memory_;
  std::unique_ptr<ChunkedArray> linkLists_;
  std::vector<int> element_levels_;  // keeps level of each element

  size_t vector_size_{0};

  DISTFUNC<dist_t> fstdistfunc_;
  void *dist_func_param_{nullptr};

  mutable std::mutex label_lookup_lock;  // lock for label_lookup_
  std::unordered_map<labeltype, tableint> label_lookup_;

  std::default_random_engine level_generator_;
  std::default_random_engine update_probability_generator_;

  mutable std::atomic<long> metric_distance_computations{0};
  mutable std::atomic<long> metric_hops{0};

  bool allow_replace_deleted_ = false;  // flag to replace deleted elements
                                        // (marked as deleted) during insertions

  std::mutex deleted_elements_lock;  // lock for deleted_elements
  std::unordered_set<tableint>
      deleted_elements;  // contains internal ids of deleted elements

  HierarchicalNSW(SpaceInterface<dist_t> *s) {}

  HierarchicalNSW(SpaceInterface<dist_t> *s, const std::string &location,
                  bool nmslib = false, size_t max_elements = 0,
                  bool allow_replace_deleted = false)
      : allow_replace_deleted_(allow_replace_deleted) {
    LoadIndex(location, s, max_elements);
  }

  HierarchicalNSW(SpaceInterface<dist_t> *s, size_t max_elements, size_t M = 16,
                  size_t ef_construction = 200, size_t random_seed = 100,
                  bool allow_replace_deleted = false)
      : label_op_locks_(MAX_LABEL_OPERATION_LOCKS),
        link_list_locks_(max_elements),
        element_levels_(max_elements),
        allow_replace_deleted_(allow_replace_deleted) {
    max_elements_ = max_elements;
    num_deleted_ = 0;
    vector_size_ = s->get_data_size();
    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();
    if (M <= 10000) {
      M_ = M;
    } else {
      HNSWERR << "warning: M parameter exceeds 10000 which may lead to adverse "
                 "effects."
              << std::endl;
      HNSWERR << "         Cap to 10000 will be applied for the rest of the "
                 "processing."
              << std::endl;
      M_ = 10000;
    }
    maxM_ = M_;
    maxM0_ = M_ * 2;
    ef_construction_ = std::max(ef_construction, M_);
    ef_ = 10;

    level_generator_.seed(random_seed);
    update_probability_generator_.seed(random_seed + 1);

    size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
    size_data_per_element_ =
        size_links_level0_ + sizeof(char *) + sizeof(labeltype);
    serialize_size_data_per_element_ =
        size_links_level0_ + vector_size_ + sizeof(labeltype);
    offsetData_ = size_links_level0_;
    label_offset_ = size_links_level0_ + sizeof(char *);
    offsetLevel0_ = 0;

    data_level0_memory_ = std::make_unique<ChunkedArray>(
        size_data_per_element_, k_elements_per_chunk, max_elements);

    cur_element_count_ = 0;

    visited_list_pool_ =
        std::unique_ptr<VisitedListPool>(new VisitedListPool(1, max_elements));

    // initializations for special treatment of the first node
    enterpoint_node_ = -1;
    maxlevel_ = -1;

    linkLists_ = std::make_unique<ChunkedArray>(
        sizeof(void *), k_elements_per_chunk, max_elements);
    size_links_per_element_ =
        maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
    mult_ = 1 / log(1.0 * M_);
    revSize_ = 1.0 / mult_;
  }

  ~HierarchicalNSW() { clear(); }

  void clear() {
    if (data_level0_memory_ != nullptr) {
      data_level0_memory_->clear();
    }
    if (linkLists_ != nullptr) {
      for (tableint i = 0; i < element_levels_.size(); i++) {
        if (element_levels_[i] > 0 &&
            *reinterpret_cast<char **>((*linkLists_)[i]) != nullptr) {
          delete[] (*reinterpret_cast<char **>((*linkLists_)[i]));
        }
      }
      linkLists_->clear();
    }
    valkey_search::Metrics::GetStats().reclaimable_memory -=
        num_deleted_ * vector_size_;
    cur_element_count_ = 0;
    visited_list_pool_.reset(nullptr);
  }

  struct CompareByFirst {
    constexpr bool operator()(
        std::pair<dist_t, tableint> const &a,
        std::pair<dist_t, tableint> const &b) const noexcept {
      return a.first < b.first;
    }
  };

  void setEf(size_t ef) { ef_ = ef; }

  inline std::mutex &getLabelOpMutex(labeltype label) const {
    // calculate hash
    size_t lock_id = label & (MAX_LABEL_OPERATION_LOCKS - 1);
    return label_op_locks_[lock_id];
  }

  inline labeltype getExternalLabel(tableint internal_id) const {
    labeltype return_label;
    memcpy(&return_label, ((*data_level0_memory_)[internal_id] + label_offset_),
           sizeof(labeltype));
    return return_label;
  }

  inline void setExternalLabel(tableint internal_id, labeltype label) const {
    memcpy(((*data_level0_memory_)[internal_id] + label_offset_), &label,
           sizeof(labeltype));
  }

  inline labeltype *getExternalLabeLp(tableint internal_id) const {
    return (labeltype *)((*data_level0_memory_)[internal_id] + label_offset_);
  }

  inline char *getDataPtrByInternalId(tableint internal_id) const {
    return ((*data_level0_memory_)[internal_id] + offsetData_);
  }

  inline char *getDataByInternalId(tableint internal_id) const {
    auto data_ptr = (char **)(getDataPtrByInternalId(internal_id));
    return *data_ptr;
  }

  int getRandomLevel(double reverse_size) {
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    double r = -log(distribution(level_generator_)) * reverse_size;
    return (int)r;
  }

  size_t getMaxElements() { return max_elements_; }

  size_t getCurrentElementCount() { return cur_element_count_; }

  size_t getDeletedCount() { return num_deleted_; }

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
  searchBaseLayer(tableint ep_id, const void *data_point, int layer) {
    VisitedList *vl = visited_list_pool_->getFreeVisitedList();
    vl_type *visited_array = vl->mass;
    vl_type visited_array_tag = vl->curV;

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        candidateSet;

    dist_t lowerBound;
    if (!isMarkedDeleted(ep_id)) {
      dist_t dist = fstdistfunc_(data_point, getDataByInternalId(ep_id),
                                 dist_func_param_);
      top_candidates.emplace(dist, ep_id);
      lowerBound = dist;
      candidateSet.emplace(-dist, ep_id);
    } else {
      lowerBound = std::numeric_limits<dist_t>::max();
      candidateSet.emplace(-lowerBound, ep_id);
    }
    visited_array[ep_id] = visited_array_tag;

    while (!candidateSet.empty()) {
      std::pair<dist_t, tableint> curr_el_pair = candidateSet.top();
      if ((-curr_el_pair.first) > lowerBound &&
          top_candidates.size() == ef_construction_) {
        break;
      }
      candidateSet.pop();

      tableint curNodeNum = curr_el_pair.second;

      std::unique_lock<std::mutex> lock(link_list_locks_[curNodeNum]);

      int *data;  // = (int *)(linkList0_ + curNodeNum *
                  // size_links_per_element0_);
      if (layer == 0) {
        data = (int *)get_linklist0(curNodeNum);
      } else {
        data = (int *)get_linklist(curNodeNum, layer);
        //                    data = (int *) ((*linkLists_)[curNodeNum] + (layer
        //                    - 1) * size_links_per_element_);
      }
      size_t size = getListCount((linklistsizeint *)data);
      tableint *datal = (tableint *)(data + 1);
#ifdef USE_PREFETCH
      __builtin_prefetch((char *)(visited_array + *(data + 1)), 0, 3);
      __builtin_prefetch((char *)(visited_array + *(data + 1) + 64), 0, 3);
      __builtin_prefetch(getDataByInternalId(*datal), 0, 3);
      __builtin_prefetch(getDataByInternalId(*(datal + 1)), 0, 3);
#endif

      for (size_t j = 0; j < size; j++) {
        tableint candidate_id = *(datal + j);
//                    if (candidate_id == 0) continue;
#ifdef USE_PREFETCH
        if (j + 1 < size) {
          __builtin_prefetch((char *)(visited_array + *(datal + j + 1)), 0, 3);
          __builtin_prefetch(getDataByInternalId(*(datal + j + 1)), 0, 3);
        }
#endif
        if (visited_array[candidate_id] == visited_array_tag) continue;
        visited_array[candidate_id] = visited_array_tag;
        char *currObj1 = (getDataByInternalId(candidate_id));

        dist_t dist1 = fstdistfunc_(data_point, currObj1, dist_func_param_);
        if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
          candidateSet.emplace(-dist1, candidate_id);
#ifdef USE_PREFETCH
          __builtin_prefetch(getDataByInternalId(candidateSet.top().second), 0,
                             3);
#endif

          if (!isMarkedDeleted(candidate_id))
            top_candidates.emplace(dist1, candidate_id);

          if (top_candidates.size() > ef_construction_) top_candidates.pop();

          if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
        }
      }
    }
    visited_list_pool_->releaseVisitedList(vl);

    return top_candidates;
  }

  // bare_bone_search means there is no check for deletions and stop condition
  // is ignored in return of extra performance
  template <bool bare_bone_search = true, bool collect_metrics = false>
  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
  searchBaseLayerST(
      tableint ep_id, const void *data_point, size_t ef,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseSearchStopCondition<dist_t> *stop_condition = nullptr) const {
    VisitedList *vl = visited_list_pool_->getFreeVisitedList();
    vl_type *visited_array = vl->mass;
    vl_type visited_array_tag = vl->curV;

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        candidate_set;

    dist_t lowerBound;
    if (bare_bone_search ||
        (!isMarkedDeleted(ep_id) &&
         ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(ep_id))))) {
      char *ep_data = getDataByInternalId(ep_id);
      dist_t dist = fstdistfunc_(data_point, ep_data, dist_func_param_);
      lowerBound = dist;
      top_candidates.emplace(dist, ep_id);
      if (!bare_bone_search && stop_condition) {
        stop_condition->add_point_to_result(getExternalLabel(ep_id), ep_data,
                                            dist);
      }
      candidate_set.emplace(-dist, ep_id);
    } else {
      lowerBound = std::numeric_limits<dist_t>::max();
      candidate_set.emplace(-lowerBound, ep_id);
    }

    visited_array[ep_id] = visited_array_tag;

    while (!candidate_set.empty()) {
      std::pair<dist_t, tableint> current_node_pair = candidate_set.top();
      dist_t candidate_dist = -current_node_pair.first;

      bool flag_stop_search;
      if (bare_bone_search) {
        flag_stop_search = candidate_dist > lowerBound;
      } else {
        if (stop_condition) {
          flag_stop_search =
              stop_condition->should_stop_search(candidate_dist, lowerBound);
        } else {
          flag_stop_search =
              candidate_dist > lowerBound && top_candidates.size() == ef;
        }
      }
      if (flag_stop_search) {
        break;
      }
      candidate_set.pop();

      tableint current_node_id = current_node_pair.second;
      int *data = (int *)get_linklist0(current_node_id);
      size_t size = getListCount((linklistsizeint *)data);
      //                bool cur_node_deleted =
      //                isMarkedDeleted(current_node_id);
      if (collect_metrics) {
        metric_hops++;
        metric_distance_computations += size;
      }

#ifdef USE_PREFETCH
      __builtin_prefetch((char *)(visited_array + *(data + 1)), 0, 3);
      __builtin_prefetch((char *)(visited_array + *(data + 1) + 64), 0, 3);
      __builtin_prefetch((*data_level0_memory_)[(*(data + 1))] + offsetData_, 0,
                         3);
      __builtin_prefetch((char *)(data + 2), 0, 3);
#endif

      for (size_t j = 1; j <= size; j++) {
        int candidate_id = *(data + j);
//                    if (candidate_id == 0) continue;
#ifdef USE_PREFETCH
        if (j + 1 < size) {
          __builtin_prefetch((char *)(visited_array + *(data + j + 1)), 0, 3);
          __builtin_prefetch(
              (*data_level0_memory_)[(*(data + j + 1))] + offsetData_, 0, 3);
        }
#endif
        if (!(visited_array[candidate_id] == visited_array_tag)) {
          visited_array[candidate_id] = visited_array_tag;

          char *currObj1 = (getDataByInternalId(candidate_id));
          dist_t dist = fstdistfunc_(data_point, currObj1, dist_func_param_);

          bool flag_consider_candidate;
          if (!bare_bone_search && stop_condition) {
            flag_consider_candidate =
                stop_condition->should_consider_candidate(dist, lowerBound);
          } else {
            flag_consider_candidate =
                top_candidates.size() < ef || lowerBound > dist;
          }

          if (flag_consider_candidate) {
            candidate_set.emplace(-dist, candidate_id);
#ifdef USE_PREFETCH
            __builtin_prefetch(
                (*data_level0_memory_)[candidate_set.top().second] +
                    offsetLevel0_,  ///////////
                0, 3);              ////////////////////////
#endif

            if (bare_bone_search ||
                (!isMarkedDeleted(candidate_id) &&
                 ((!isIdAllowed) ||
                  (*isIdAllowed)(getExternalLabel(candidate_id))))) {
              top_candidates.emplace(dist, candidate_id);
              if (!bare_bone_search && stop_condition) {
                stop_condition->add_point_to_result(
                    getExternalLabel(candidate_id), currObj1, dist);
              }
            }

            bool flag_remove_extra = false;
            if (!bare_bone_search && stop_condition) {
              flag_remove_extra = stop_condition->should_remove_extra();
            } else {
              flag_remove_extra = top_candidates.size() > ef;
            }
            while (flag_remove_extra) {
              tableint id = top_candidates.top().second;
              top_candidates.pop();
              if (!bare_bone_search && stop_condition) {
                stop_condition->remove_point_from_result(
                    getExternalLabel(id), getDataByInternalId(id), dist);
                flag_remove_extra = stop_condition->should_remove_extra();
              } else {
                flag_remove_extra = top_candidates.size() > ef;
              }
            }

            if (!top_candidates.empty())
              lowerBound = top_candidates.top().first;
          }
        }
      }
    }

    visited_list_pool_->releaseVisitedList(vl);
    return top_candidates;
  }

  void getNeighborsByHeuristic2(
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst> &top_candidates,
      const size_t M) {
    if (top_candidates.size() < M) {
      return;
    }

    std::priority_queue<std::pair<dist_t, tableint>> queue_closest;
    std::vector<std::pair<dist_t, tableint>> return_list;
    while (top_candidates.size() > 0) {
      queue_closest.emplace(-top_candidates.top().first,
                            top_candidates.top().second);
      top_candidates.pop();
    }

    while (queue_closest.size()) {
      if (return_list.size() >= M) break;
      std::pair<dist_t, tableint> current_pair = queue_closest.top();
      dist_t dist_to_query = -current_pair.first;
      queue_closest.pop();
      bool good = true;

      for (std::pair<dist_t, tableint> second_pair : return_list) {
        dist_t curdist = fstdistfunc_(getDataByInternalId(second_pair.second),
                                      getDataByInternalId(current_pair.second),
                                      dist_func_param_);
        if (curdist < dist_to_query) {
          good = false;
          break;
        }
      }
      if (good) {
        return_list.push_back(current_pair);
      }
    }

    for (std::pair<dist_t, tableint> current_pair : return_list) {
      top_candidates.emplace(-current_pair.first, current_pair.second);
    }
  }

  linklistsizeint *get_linklist0(tableint internal_id) const {
    return (linklistsizeint *)((*data_level0_memory_)[internal_id] +
                               offsetLevel0_);
  }

  linklistsizeint *get_linklist(tableint internal_id, int level) const {
    return (linklistsizeint *)(*reinterpret_cast<char **>(
                                   (*linkLists_)[internal_id]) +
                               (level - 1) * size_links_per_element_);
  }

  linklistsizeint *get_linklist_at_level(tableint internal_id,
                                         int level) const {
    return level == 0 ? get_linklist0(internal_id)
                      : get_linklist(internal_id, level);
  }

  tableint mutuallyConnectNewElement(
      const void *data_point, tableint cur_c,
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst> &top_candidates,
      int level, bool isUpdate) {
    size_t Mcurmax = level ? maxM_ : maxM0_;
    getNeighborsByHeuristic2(top_candidates, M_);
    if (top_candidates.size() > M_)
      throw std::runtime_error(
          "Should be not be more than M_ candidates returned by the heuristic");

    std::vector<tableint> selectedNeighbors;
    selectedNeighbors.reserve(M_);
    while (top_candidates.size() > 0) {
      selectedNeighbors.push_back(top_candidates.top().second);
      top_candidates.pop();
    }

    if (selectedNeighbors.empty()) {
      throw std::runtime_error(
          "During insertion, no neighbors found to mutually connect to");
    }

    tableint next_closest_entry_point = selectedNeighbors.back();

    {
      // lock only during the update
      // because during the addition the lock for cur_c is already acquired
      std::unique_lock<std::mutex> lock(link_list_locks_[cur_c],
                                        std::defer_lock);
      if (isUpdate) {
        lock.lock();
      }
      linklistsizeint *ll_cur;
      if (level == 0)
        ll_cur = get_linklist0(cur_c);
      else
        ll_cur = get_linklist(cur_c, level);

      if (*ll_cur && !isUpdate) {
        throw std::runtime_error(
            "The newly inserted element should have blank link list");
      }
      setListCount(ll_cur, selectedNeighbors.size());
      tableint *data = (tableint *)(ll_cur + 1);
      for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
        if (data[idx] && !isUpdate)
          throw std::runtime_error("Possible memory corruption");
        if (level > element_levels_[selectedNeighbors[idx]])
          throw std::runtime_error(
              "Trying to make a link on a non-existent level");

        data[idx] = selectedNeighbors[idx];
      }
    }

    for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
      std::unique_lock<std::mutex> lock(
          link_list_locks_[selectedNeighbors[idx]]);

      linklistsizeint *ll_other;
      if (level == 0)
        ll_other = get_linklist0(selectedNeighbors[idx]);
      else
        ll_other = get_linklist(selectedNeighbors[idx], level);

      size_t sz_link_list_other = getListCount(ll_other);

      if (sz_link_list_other > Mcurmax)
        throw std::runtime_error("Bad value of sz_link_list_other");
      if (selectedNeighbors[idx] == cur_c)
        throw std::runtime_error("Trying to connect an element to itself");
      if (level > element_levels_[selectedNeighbors[idx]])
        throw std::runtime_error(
            "Trying to make a link on a non-existent level");

      tableint *data = (tableint *)(ll_other + 1);

      bool is_cur_c_present = false;
      if (isUpdate) {
        for (size_t j = 0; j < sz_link_list_other; j++) {
          if (data[j] == cur_c) {
            is_cur_c_present = true;
            break;
          }
        }
      }

      // If cur_c is already present in the neighboring connections of
      // `selectedNeighbors[idx]` then no need to modify any connections or run
      // the heuristics.
      if (!is_cur_c_present) {
        if (sz_link_list_other < Mcurmax) {
          data[sz_link_list_other] = cur_c;
          setListCount(ll_other, sz_link_list_other + 1);
        } else {
          // finding the "weakest" element to replace it with the new one
          dist_t d_max = fstdistfunc_(
              getDataByInternalId(cur_c),
              getDataByInternalId(selectedNeighbors[idx]), dist_func_param_);
          // Heuristic:
          std::priority_queue<std::pair<dist_t, tableint>,
                              std::vector<std::pair<dist_t, tableint>>,
                              CompareByFirst>
              candidates;
          candidates.emplace(d_max, cur_c);

          for (size_t j = 0; j < sz_link_list_other; j++) {
            candidates.emplace(
                fstdistfunc_(getDataByInternalId(data[j]),
                             getDataByInternalId(selectedNeighbors[idx]),
                             dist_func_param_),
                data[j]);
          }

          getNeighborsByHeuristic2(candidates, Mcurmax);

          int indx = 0;
          while (candidates.size() > 0) {
            data[indx] = candidates.top().second;
            candidates.pop();
            indx++;
          }

          setListCount(ll_other, indx);
          // Nearest K:
          /*int indx = -1;
          for (int j = 0; j < sz_link_list_other; j++) {
              dist_t d = fstdistfunc_(getDataByInternalId(data[j]),
          getDataByInternalId(rez[idx]), dist_func_param_); if (d > d_max) {
                  indx = j;
                  d_max = d;
              }
          }
          if (indx >= 0) {
              data[indx] = cur_c;
          } */
        }
      }
    }

    return next_closest_entry_point;
  }

  void resizeIndex(size_t new_max_elements) {
    if (new_max_elements < cur_element_count_)
      throw std::runtime_error(
          "Cannot resize, max element is less than the current number of "
          "elements");

    visited_list_pool_.reset(new VisitedListPool(1, new_max_elements));

    element_levels_.resize(new_max_elements);

    std::vector<std::mutex>(new_max_elements).swap(link_list_locks_);

    // Reallocate base layer
    data_level0_memory_->resize(new_max_elements);

    // Reallocate all other layers
    linkLists_->resize(new_max_elements);

    max_elements_ = new_max_elements;
  }

  size_t indexFileSize() const {
    size_t size = 0;
    size += sizeof(offsetLevel0_);
    size += sizeof(max_elements_);
    size += sizeof(cur_element_count_);
    size += sizeof(serialize_size_data_per_element_);
    size += sizeof(label_offset_);
    size += sizeof(offsetData_);
    size += sizeof(maxlevel_);
    size += sizeof(enterpoint_node_);
    size += sizeof(maxM_);

    size += sizeof(maxM0_);
    size += sizeof(M_);
    size += sizeof(mult_);
    size += sizeof(ef_construction_);

    size += cur_element_count_ * serialize_size_data_per_element_;

    for (size_t i = 0; i < cur_element_count_; i++) {
      unsigned int linkListSize =
          element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i]
                                 : 0;
      size += sizeof(linkListSize);
      size += linkListSize;
    }
    return size;
  }

  absl::Status SaveIndex(OutputStream &output) {
    data_model::HNSWIndexHeader header;
    header.set_offset_level_0(offsetLevel0_);
    header.set_max_elements(max_elements_);
    header.set_curr_element_count(cur_element_count_);
    header.set_serialize_size_data_per_element(
        serialize_size_data_per_element_);
    header.set_label_offset(label_offset_);
    header.set_offset_data(offsetData_);
    header.set_max_level(maxlevel_);
    header.set_enterpoint_node(enterpoint_node_);
    header.set_max_m(maxM_);
    header.set_max_m_0(maxM0_);
    header.set_m(M_);
    header.set_mult(mult_);
    header.set_ef_construction(ef_construction_);
    std::string serialized;
    if (!header.SerializeToString(&serialized)) {
      return absl::InternalError("Could not serialize HNSW header");
    }
    VMSDK_RETURN_IF_ERROR(
        output.SaveChunk(serialized.data(), serialized.size()));

    if (cur_element_count_ == 0) {
      return absl::OkStatus();
    }

    // Resize internal data structures to match the true max elements so
    // that the saved index is self-consistent.
    data_level0_memory_->resize(max_elements_);
    linkLists_->resize(max_elements_);

    std::vector<char> buf(serialize_size_data_per_element_);
    for (int i = 0; i < cur_element_count_; i++) {
      memcpy(buf.data(), (*data_level0_memory_)[i], size_links_level0_);
      memcpy(buf.data() + size_links_level0_,
             *(char **)((*data_level0_memory_)[i] + offsetData_), vector_size_);
      memcpy(buf.data() + size_links_level0_ + vector_size_,
             (*data_level0_memory_)[i] + label_offset_, sizeof(labeltype));
      VMSDK_RETURN_IF_ERROR(
          output.SaveChunk(buf.data(), serialize_size_data_per_element_));
    };

    for (size_t i = 0; i < cur_element_count_; i++) {
      unsigned int linkListSize =
          element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i]
                                 : 0;
      size_t size_to_serialize = htole64(static_cast<size_t>(linkListSize));
      VMSDK_RETURN_IF_ERROR(output.SaveChunk(
          reinterpret_cast<const char *>(&size_to_serialize), sizeof(size_t)));
      if (linkListSize) {
        VMSDK_RETURN_IF_ERROR(output.SaveChunk(
            *reinterpret_cast<char **>((*linkLists_)[i]), linkListSize));
      }
    }
    return absl::OkStatus();
  }

  absl::Status LoadIndex(InputStream &input, SpaceInterface<dist_t> *s,
                         size_t max_elements_i, VectorTracker *vector_tracker) {
    clear();

    VMSDK_ASSIGN_OR_RETURN(auto serialized_header, input.LoadChunk());
    auto header = std::make_unique<data_model::HNSWIndexHeader>();
    if (!header->ParseFromString(*serialized_header)) {
      return absl::InternalError("Could not deserialize HNSW header");
    }

    offsetLevel0_ = header->offset_level_0();
    max_elements_ = header->max_elements();
    cur_element_count_ = header->curr_element_count();
    serialize_size_data_per_element_ =
        header->serialize_size_data_per_element();
    label_offset_ = header->label_offset();
    offsetData_ = header->offset_data();
    maxlevel_ = header->max_level();
    enterpoint_node_ = header->enterpoint_node();
    maxM_ = header->max_m();
    maxM0_ = header->max_m_0();
    M_ = header->m();
    mult_ = header->mult();
    ef_construction_ = header->ef_construction();

    size_t max_elements = max_elements_i;
    if (max_elements < cur_element_count_) {
      max_elements = max_elements_;
    }
    max_elements_ = max_elements;

    size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);

    vector_size_ = s->get_data_size();
    size_data_per_element_ =
        size_links_level0_ + sizeof(char *) + sizeof(labeltype);
    label_offset_ = size_links_level0_ + sizeof(char *);

    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();

    data_level0_memory_ = std::make_unique<ChunkedArray>(
        size_data_per_element_, k_elements_per_chunk, max_elements);

    for (size_t i = 0; i < cur_element_count_; i++) {
      VMSDK_ASSIGN_OR_RETURN(auto chunk, input.LoadChunk());
      memcpy((*data_level0_memory_)[i], chunk->data(), size_links_level0_);
      labeltype id;
      memcpy((char *)&id, chunk->data() + offsetData_ + vector_size_,
             sizeof(labeltype));
      *(char **)((*data_level0_memory_)[i] + offsetData_) =
          vector_tracker->TrackVector(id, chunk->data() + offsetData_,
                                      vector_size_);
      memcpy((*data_level0_memory_)[i] + label_offset_, (char *)&id,
             sizeof(labeltype));
    }

    size_links_per_element_ =
        maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

    std::vector<std::mutex>(max_elements).swap(link_list_locks_);
    std::vector<std::mutex>(MAX_LABEL_OPERATION_LOCKS).swap(label_op_locks_);

    visited_list_pool_ = std::make_unique<VisitedListPool>(1, max_elements);

    linkLists_ = std::make_unique<ChunkedArray>(
        sizeof(void *), k_elements_per_chunk, max_elements);

    element_levels_ = std::vector<int>(max_elements);
    revSize_ = 1.0 / mult_;
    ef_ = 10;
    for (size_t i = 0; i < cur_element_count_; i++) {
      label_lookup_[getExternalLabel(i)] = i;
      size_t linkListSize;
      VMSDK_ASSIGN_OR_RETURN(auto size_chunk, input.LoadChunk());
      memcpy(&linkListSize, size_chunk->data(), sizeof(size_t));
      linkListSize = le64toh(linkListSize);
      if (linkListSize == 0) {
        element_levels_[i] = 0;
        *reinterpret_cast<char **>((*linkLists_)[i]) = nullptr;
      } else {
        element_levels_[i] = linkListSize / size_links_per_element_;
        VMSDK_ASSIGN_OR_RETURN(auto link_list_chunk, input.LoadChunk());
        *reinterpret_cast<char **>((*linkLists_)[i]) =
            new char[link_list_chunk->size()];
        memcpy(*reinterpret_cast<char **>((*linkLists_)[i]),
               link_list_chunk->data(), link_list_chunk->size());
      }
    }

    for (size_t i = 0; i < cur_element_count_; i++) {
      if (isMarkedDeleted(i)) {
        num_deleted_ += 1;
        valkey_search::Metrics::GetStats().reclaimable_memory += vector_size_;
        if (allow_replace_deleted_) {
          deleted_elements.insert(i);
        }
      }
    }
    return absl::OkStatus();
  }

  char *getPoint(labeltype label) const {
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
      return nullptr;
    }
    return getDataByInternalId(search->second);
  }

  template <typename data_t>
  std::vector<data_t> getDataByLabel(labeltype label) const {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    char *data_ptrv = getDataByInternalId(internalId);
    size_t dim = *((size_t *)dist_func_param_);
    std::vector<data_t> data(dim);
    memcpy(data.data(), data_ptrv, dim * sizeof(data_t));
    return data;
  }

  /*
   * Marks an element with the given label deleted, does NOT really change the
   * current graph.
   */
  void markDelete(labeltype label) {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    markDeletedInternal(internalId);
  }

  /*
   * Uses the last 16 bits of the memory for the linked list size to store the
   * mark, whereas maxM0_ has to be limited to the lower 16 bits, however, still
   * large enough in almost all cases.
   */
  void markDeletedInternal(tableint internalId) {
    assert(internalId < cur_element_count_);
    if (!isMarkedDeleted(internalId)) {
      unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
      *ll_cur |= DELETE_MARK;
      num_deleted_ += 1;
      valkey_search::Metrics::GetStats().reclaimable_memory += vector_size_;
      if (allow_replace_deleted_) {
        std::unique_lock<std::mutex> lock_deleted_elements(
            deleted_elements_lock);
        deleted_elements.insert(internalId);
      }
    } else {
      throw std::runtime_error(
          "The requested to delete element is already deleted");
    }
  }

  /*
   * Removes the deleted mark of the node, does NOT really change the current
   * graph.
   *
   * Note: the method is not safe to use when replacement of deleted elements is
   * enabled, because elements marked as deleted can be completely removed by
   * addPoint
   */
  void unmarkDelete(labeltype label) {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    unmarkDeletedInternal(internalId);
  }

  /*
   * Remove the deleted mark of the node.
   */
  void unmarkDeletedInternal(tableint internalId) {
    assert(internalId < cur_element_count_);
    if (isMarkedDeleted(internalId)) {
      unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
      *ll_cur &= ~DELETE_MARK;
      num_deleted_ -= 1;
      valkey_search::Metrics::GetStats().reclaimable_memory -= vector_size_;
      if (allow_replace_deleted_) {
        std::unique_lock<std::mutex> lock_deleted_elements(
            deleted_elements_lock);
        deleted_elements.erase(internalId);
      }
    } else {
      throw std::runtime_error(
          "The requested to undelete element is not deleted");
    }
  }

  /*
   * Checks the first 16 bits of the memory to see if the element is marked
   * deleted.
   */
  bool isMarkedDeleted(tableint internalId) const {
    unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
    return *ll_cur & DELETE_MARK;
  }

  unsigned short int getListCount(linklistsizeint *ptr) const {
    return *((unsigned short int *)ptr);
  }

  void setListCount(linklistsizeint *ptr, unsigned short int size) const {
    *((unsigned short int *)(ptr)) = *((unsigned short int *)&size);
  }

  /*
   * Adds point. Updates the point if it is already in the index.
   * If replacement of deleted elements is enabled: replaces previously deleted
   * point if any, updating it with new point
   */
  void addPoint(const void *data_point, labeltype label,
                bool replace_deleted = false) {
    if ((allow_replace_deleted_ == false) && (replace_deleted == true)) {
      throw std::runtime_error(
          "Replacement of deleted elements is disabled in constructor");
    }

    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));
    if (!replace_deleted) {
      addPoint(data_point, label, -1);
      return;
    }
    // check if there is vacant place
    tableint internal_id_replaced;
    std::unique_lock<std::mutex> lock_deleted_elements(deleted_elements_lock);
    bool is_vacant_place = !deleted_elements.empty();
    if (is_vacant_place) {
      internal_id_replaced = *deleted_elements.begin();
      deleted_elements.erase(internal_id_replaced);
    }
    lock_deleted_elements.unlock();

    // if there is no vacant place then add or update point
    // else add point to vacant place
    if (!is_vacant_place) {
      addPoint(data_point, label, -1);
    } else {
      // we assume that there are no concurrent operations on deleted element
      labeltype label_replaced = getExternalLabel(internal_id_replaced);
      setExternalLabel(internal_id_replaced, label);

      std::unique_lock<std::mutex> lock_table(label_lookup_lock);
      label_lookup_.erase(label_replaced);
      label_lookup_[label] = internal_id_replaced;
      lock_table.unlock();

      unmarkDeletedInternal(internal_id_replaced);
      updatePoint(data_point, internal_id_replaced, 1.0);
    }
  }

  void updatePoint(const void *dataPoint, tableint internalId,
                   float updateNeighborProbability) {
    // update the feature vector associated with existing point with new vector
    auto data_ptr = (const char **)(getDataPtrByInternalId(internalId));
    *data_ptr = static_cast<const char *>(dataPoint);

    int maxLevelCopy = maxlevel_;
    tableint entryPointCopy = enterpoint_node_;
    // If point to be updated is entry point and graph just contains single
    // element then just return.
    if (entryPointCopy == internalId && cur_element_count_ == 1) return;

    int elemLevel = element_levels_[internalId];
    std::uniform_real_distribution<float> distribution(0.0, 1.0);
    for (int layer = 0; layer <= elemLevel; layer++) {
      std::unordered_set<tableint> sCand;
      std::unordered_set<tableint> sNeigh;
      std::vector<tableint> listOneHop =
          getConnectionsWithLock(internalId, layer);
      if (listOneHop.size() == 0) continue;

      sCand.insert(internalId);

      for (auto &&elOneHop : listOneHop) {
        sCand.insert(elOneHop);

        if (distribution(update_probability_generator_) >
            updateNeighborProbability)
          continue;

        sNeigh.insert(elOneHop);

        std::vector<tableint> listTwoHop =
            getConnectionsWithLock(elOneHop, layer);
        for (auto &&elTwoHop : listTwoHop) {
          sCand.insert(elTwoHop);
        }
      }

      for (auto &&neigh : sNeigh) {
        // if (neigh == internalId)
        //     continue;

        std::priority_queue<std::pair<dist_t, tableint>,
                            std::vector<std::pair<dist_t, tableint>>,
                            CompareByFirst>
            candidates;
        size_t size =
            sCand.find(neigh) == sCand.end()
                ? sCand.size()
                : sCand.size() - 1;  // sCand guaranteed to have size >= 1
        size_t elementsToKeep = std::min(ef_construction_, size);
        for (auto &&cand : sCand) {
          if (cand == neigh) continue;

          dist_t distance =
              fstdistfunc_(getDataByInternalId(neigh),
                           getDataByInternalId(cand), dist_func_param_);
          if (candidates.size() < elementsToKeep) {
            candidates.emplace(distance, cand);
          } else {
            if (distance < candidates.top().first) {
              candidates.pop();
              candidates.emplace(distance, cand);
            }
          }
        }

        // Retrieve neighbours using heuristic and set connections.
        getNeighborsByHeuristic2(candidates, layer == 0 ? maxM0_ : maxM_);

        {
          std::unique_lock<std::mutex> lock(link_list_locks_[neigh]);
          linklistsizeint *ll_cur;
          ll_cur = get_linklist_at_level(neigh, layer);
          size_t candSize = candidates.size();
          setListCount(ll_cur, candSize);
          tableint *data = (tableint *)(ll_cur + 1);
          for (size_t idx = 0; idx < candSize; idx++) {
            data[idx] = candidates.top().second;
            candidates.pop();
          }
        }
      }
    }

    repairConnectionsForUpdate(dataPoint, entryPointCopy, internalId, elemLevel,
                               maxLevelCopy);
  }

  void repairConnectionsForUpdate(const void *dataPoint,
                                  tableint entryPointInternalId,
                                  tableint dataPointInternalId,
                                  int dataPointLevel, int maxLevel) {
    tableint currObj = entryPointInternalId;
    if (dataPointLevel < maxLevel) {
      dist_t curdist = fstdistfunc_(dataPoint, getDataByInternalId(currObj),
                                    dist_func_param_);
      for (int level = maxLevel; level > dataPointLevel; level--) {
        bool changed = true;
        while (changed) {
          changed = false;
          unsigned int *data;
          std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
          data = get_linklist_at_level(currObj, level);
          int size = getListCount(data);
          tableint *datal = (tableint *)(data + 1);
#ifdef USE_PREFETCH
          __builtin_prefetch(getDataByInternalId(*datal), 0, 3);
#endif
          for (int i = 0; i < size; i++) {
#ifdef USE_PREFETCH
            if (i + 1 < size) {
              __builtin_prefetch(getDataByInternalId(*(datal + i + 1)), 1, 3);
            }
#endif
            tableint cand = datal[i];
            dist_t d = fstdistfunc_(dataPoint, getDataByInternalId(cand),
                                    dist_func_param_);
            if (d < curdist) {
              curdist = d;
              currObj = cand;
              changed = true;
            }
          }
        }
      }
    }

    if (dataPointLevel > maxLevel)
      throw std::runtime_error(
          "Level of item to be updated cannot be bigger than max level");

    for (int level = dataPointLevel; level >= 0; level--) {
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst>
          topCandidates = searchBaseLayer(currObj, dataPoint, level);

      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst>
          filteredTopCandidates;
      while (topCandidates.size() > 0) {
        if (topCandidates.top().second != dataPointInternalId)
          filteredTopCandidates.push(topCandidates.top());

        topCandidates.pop();
      }

      // Since element_levels_ is being used to get `dataPointLevel`, there
      // could be cases where `topCandidates` could just contains entry point
      // itself. To prevent self loops, the `topCandidates` is filtered and thus
      // can be empty.
      if (filteredTopCandidates.size() > 0) {
        bool epDeleted = isMarkedDeleted(entryPointInternalId);
        if (epDeleted) {
          filteredTopCandidates.emplace(
              fstdistfunc_(dataPoint, getDataByInternalId(entryPointInternalId),
                           dist_func_param_),
              entryPointInternalId);
          if (filteredTopCandidates.size() > ef_construction_)
            filteredTopCandidates.pop();
        }

        currObj = mutuallyConnectNewElement(dataPoint, dataPointInternalId,
                                            filteredTopCandidates, level, true);
      }
    }
  }

  std::vector<tableint> getConnectionsWithLock(tableint internalId, int level) {
    std::unique_lock<std::mutex> lock(link_list_locks_[internalId]);
    unsigned int *data = get_linklist_at_level(internalId, level);
    int size = getListCount(data);
    std::vector<tableint> result(size);
    tableint *ll = (tableint *)(data + 1);
    memcpy(result.data(), ll, size * sizeof(tableint));
    return result;
  }

  tableint addPoint(const void *data_point, labeltype label, int level) {
    tableint cur_c = 0;
    {
      // Checking if the element with the same label already exists
      // if so, updating it *instead* of creating a new element.
      std::unique_lock<std::mutex> lock_table(label_lookup_lock);
      auto search = label_lookup_.find(label);
      if (search != label_lookup_.end()) {
        tableint existingInternalId = search->second;
        if (allow_replace_deleted_) {
          if (isMarkedDeleted(existingInternalId)) {
            throw std::runtime_error(
                "Can't use addPoint to update deleted elements if replacement "
                "of deleted elements is enabled.");
          }
        }
        lock_table.unlock();

        if (isMarkedDeleted(existingInternalId)) {
          unmarkDeletedInternal(existingInternalId);
        }
        updatePoint(data_point, existingInternalId, 1.0);

        return existingInternalId;
      }

      if (cur_element_count_ >= max_elements_) {
        throw std::runtime_error(
            "The number of elements exceeds the specified limit");
      }

      cur_c = cur_element_count_;
      cur_element_count_++;
      label_lookup_[label] = cur_c;
    }

    std::unique_lock<std::mutex> templock(global);
    int maxlevelcopy = maxlevel_;
    std::unique_lock<std::mutex> lock_el(link_list_locks_[cur_c]);
    int curlevel = getRandomLevel(mult_);
    if (level > 0) curlevel = level;
    if (curlevel <= maxlevelcopy) {
      templock.unlock();
    }
    element_levels_[cur_c] = curlevel;
    tableint currObj = enterpoint_node_;
    tableint enterpoint_copy = enterpoint_node_;

    memset((*data_level0_memory_)[cur_c] + offsetLevel0_, 0,
           size_data_per_element_);

    // Initialisation of the data and label
    memcpy(getExternalLabeLp(cur_c), &label, sizeof(labeltype));
    auto data_ptr = (const char **)(getDataPtrByInternalId(cur_c));
    *data_ptr = static_cast<const char *>(data_point);

    if (curlevel) {
      *reinterpret_cast<char **>((*linkLists_)[cur_c]) =
          new char[size_links_per_element_ * curlevel + 1];
      if (*reinterpret_cast<char **>((*linkLists_)[cur_c]) == nullptr)
        throw std::runtime_error(
            "Not enough memory: addPoint failed to allocate linklist");
      memset(*reinterpret_cast<char **>((*linkLists_)[cur_c]), 0,
             size_links_per_element_ * curlevel + 1);
    }

    if ((signed)currObj != -1) {
      if (curlevel < maxlevelcopy) {
        dist_t curdist = fstdistfunc_(data_point, getDataByInternalId(currObj),
                                      dist_func_param_);
        for (int level = maxlevelcopy; level > curlevel; level--) {
          bool changed = true;
          while (changed) {
            changed = false;
            unsigned int *data;
            std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
            data = get_linklist(currObj, level);
            int size = getListCount(data);

            tableint *datal = (tableint *)(data + 1);
            for (int i = 0; i < size; i++) {
              tableint cand = datal[i];
              if (cand < 0 || cand > max_elements_)
                throw std::runtime_error("cand error");
              dist_t d = fstdistfunc_(data_point, getDataByInternalId(cand),
                                      dist_func_param_);
              if (d < curdist) {
                curdist = d;
                currObj = cand;
                changed = true;
              }
            }
          }
        }
      }

      bool epDeleted = isMarkedDeleted(enterpoint_copy);
      for (int level = std::min(curlevel, maxlevelcopy); level >= 0; level--) {
        if (level > maxlevelcopy || level < 0)  // possible?
          throw std::runtime_error("Level error");

        std::priority_queue<std::pair<dist_t, tableint>,
                            std::vector<std::pair<dist_t, tableint>>,
                            CompareByFirst>
            top_candidates = searchBaseLayer(currObj, data_point, level);
        if (epDeleted) {
          top_candidates.emplace(
              fstdistfunc_(data_point, getDataByInternalId(enterpoint_copy),
                           dist_func_param_),
              enterpoint_copy);
          if (top_candidates.size() > ef_construction_) top_candidates.pop();
        }
        currObj = mutuallyConnectNewElement(data_point, cur_c, top_candidates,
                                            level, false);
      }
    } else {
      // Do nothing for the first element
      enterpoint_node_ = 0;
      maxlevel_ = curlevel;
    }

    // Releasing lock for the maximum level
    if (curlevel > maxlevelcopy) {
      enterpoint_node_ = cur_c;
      maxlevel_ = curlevel;
    }
    return cur_c;
  }
  std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const void *query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr) const {
    return searchKnn(query_data, k, std::nullopt, isIdAllowed);
  }

  std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const void *query_data, size_t k, std::optional<size_t> ef_runtime,
      BaseFilterFunctor *isIdAllowed = nullptr) const {
    std::priority_queue<std::pair<dist_t, labeltype>> result;
    if (cur_element_count_ == 0) return result;

    tableint currObj = enterpoint_node_;
    dist_t curdist = fstdistfunc_(
        query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);

    for (int level = maxlevel_; level > 0; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        unsigned int *data;

        data = (unsigned int *)get_linklist(currObj, level);
        int size = getListCount(data);
        metric_hops++;
        metric_distance_computations += size;

        tableint *datal = (tableint *)(data + 1);
        for (int i = 0; i < size; i++) {
          tableint cand = datal[i];
          if (cand < 0 || cand > max_elements_)
            throw std::runtime_error("cand error");
          dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand),
                                  dist_func_param_);

          if (d < curdist) {
            curdist = d;
            currObj = cand;
            changed = true;
          }
        }
      }
    }

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    bool bare_bone_search = !num_deleted_ && !isIdAllowed;
    if (bare_bone_search) {
      top_candidates = searchBaseLayerST<true>(
          currObj, query_data, std::max(ef_runtime.value_or(ef_), k),
          isIdAllowed);
    } else {
      top_candidates = searchBaseLayerST<false>(
          currObj, query_data, std::max(ef_runtime.value_or(ef_), k),
          isIdAllowed);
    }

    while (top_candidates.size() > k) {
      top_candidates.pop();
    }
    while (top_candidates.size() > 0) {
      std::pair<dist_t, tableint> rez = top_candidates.top();
      result.push(std::pair<dist_t, labeltype>(rez.first,
                                               getExternalLabel(rez.second)));
      top_candidates.pop();
    }
    return result;
  }

  std::vector<std::pair<dist_t, labeltype>> searchStopConditionClosest(
      const void *query_data, BaseSearchStopCondition<dist_t> &stop_condition,
      BaseFilterFunctor *isIdAllowed = nullptr) const {
    std::vector<std::pair<dist_t, labeltype>> result;
    if (cur_element_count_ == 0) return result;

    tableint currObj = enterpoint_node_;
    dist_t curdist = fstdistfunc_(
        query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);

    for (int level = maxlevel_; level > 0; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        unsigned int *data;

        data = (unsigned int *)get_linklist(currObj, level);
        int size = getListCount(data);
        metric_hops++;
        metric_distance_computations += size;

        tableint *datal = (tableint *)(data + 1);
        for (int i = 0; i < size; i++) {
          tableint cand = datal[i];
          if (cand < 0 || cand > max_elements_)
            throw std::runtime_error("cand error");
          dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand),
                                  dist_func_param_);

          if (d < curdist) {
            curdist = d;
            currObj = cand;
            changed = true;
          }
        }
      }
    }

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    top_candidates = searchBaseLayerST<false>(currObj, query_data, 0,
                                              isIdAllowed, &stop_condition);

    size_t sz = top_candidates.size();
    result.resize(sz);
    while (!top_candidates.empty()) {
      result[--sz] = top_candidates.top();
      top_candidates.pop();
    }

    stop_condition.filter_results(result);

    return result;
  }

  void checkIntegrity() {
    int connections_checked = 0;
    std::vector<int> inbound_connections_num(cur_element_count_, 0);
    for (int i = 0; i < cur_element_count_; i++) {
      for (int l = 0; l <= element_levels_[i]; l++) {
        linklistsizeint *ll_cur = get_linklist_at_level(i, l);
        int size = getListCount(ll_cur);
        tableint *data = (tableint *)(ll_cur + 1);
        std::unordered_set<tableint> s;
        for (int j = 0; j < size; j++) {
          assert(data[j] < cur_element_count_);
          assert(data[j] != i);
          inbound_connections_num[data[j]]++;
          s.insert(data[j]);
          connections_checked++;
        }
        assert(s.size() == size);
      }
    }
    if (cur_element_count_ > 1) {
      int min1 = inbound_connections_num[0], max1 = inbound_connections_num[0];
      for (int i = 0; i < cur_element_count_; i++) {
        assert(inbound_connections_num[i] > 0);
        min1 = std::min(inbound_connections_num[i], min1);
        max1 = std::max(inbound_connections_num[i], max1);
      }
      std::cout << "Min inbound: " << min1 << ", Max inbound:" << max1 << "\n";
    }
    std::cout << "integrity ok, checked " << connections_checked
              << " connections\n";
  }
};
}  // namespace hnswlib
