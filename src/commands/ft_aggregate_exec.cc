/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search {
namespace aggregate {

expr::Value Attribute::GetValue(expr::Expression::EvalContext& ctx,
                                const expr::Expression::Record& record) const {
  auto rec = reinterpret_cast<const Record&>(record);
  return rec.fields_.at(record_index_);
};

expr::Expression::EvalContext ctx;

std::ostream& operator<<(std::ostream& os, const RecordSet& rs) {
  os << "<RecordSet> " << rs.size() << "\n";
  for (size_t i = 0; i < rs.size(); ++i) {
    os << i << ": ";
    rs[i]->Dump(os, rs.agg_params_);
    os << "\n";
  }
  os << "</RecordSet>\n";
  return os;
}

void Record::Dump(std::ostream& os,
                  const AggregateParameters* agg_params) const {
  os << '[';
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (!fields_[i].IsNil()) {
      os << ' ';
      if (agg_params) {
        os << agg_params->record_info_by_index_[i] << ':';
      } else {
        os << '?' << i << '?';
      }
      os << fields_[i];
    }
  }
  if (!extra_fields_.empty()) {
    os << " Extra:" << extra_fields_.size() << ' ';
    for (const auto& [field, value] : extra_fields_) {
      os << " " << field << ":" << value;
    }
  }
  os << ']';
}

absl::Status Limit::Execute(RecordSet& records) const {
  DBG << "Executing LIMIT with offset: " << offset_ << " and limit: " << limit_
      << "\n";
  for (auto i = 0; i < offset_ && !records.empty(); ++i) {
    records.pop_front();
  }
  while (records.size() > limit_) {
    records.pop_back();
  }
  return absl::OkStatus();
}

void SetField(Record& record, Attribute& dest, expr::Value value) {
  if (record.fields_.size() <= dest.record_index_) {
    record.fields_.resize(dest.record_index_ + 1);
  }
  record.fields_[dest.record_index_] = value;
}

absl::Status Apply::Execute(RecordSet& records) const {
  DBG << "Executing APPLY with expr: " << *expr_ << "\n";
  for (auto& r : records) {
    SetField(*r, *name_, expr_->Evaluate(ctx, *r));
  }
  return absl::OkStatus();
}

absl::Status Filter::Execute(RecordSet& records) const {
  DBG << "Executing FILTER with expr: " << *expr_ << "\n";
  RecordSet filtered(records.agg_params_);
  while (!records.empty()) {
    auto r = records.pop_front();
    auto result = expr_->Evaluate(ctx, *r);
    if (result.IsTrue()) {
      filtered.push_back(std::move(r));
    }
  }
  records.swap(filtered);
  return absl::OkStatus();
}

template <typename T>
struct SortFunctor {
  const absl::InlinedVector<SortBy::SortKey, 4>* sortkeys_;
  bool operator()(const T& l, const T& r) const {
    for (auto& sk : *sortkeys_) {
      auto lvalue = sk.expr_->Evaluate(ctx, *l);
      auto rvalue = sk.expr_->Evaluate(ctx, *r);
      auto cmp = expr::Compare(lvalue, rvalue);
      switch (cmp) {
        case expr::Ordering::kEQUAL:
        case expr::Ordering::kUNORDERED:
          continue;
        case expr::Ordering::kLESS:
          return sk.direction_ == SortBy::Direction::kASC;
        case expr::Ordering::kGREATER:
          return sk.direction_ == SortBy::Direction::kDESC;
      }
    }
    return false;
  }
};

absl::Status SortBy::Execute(RecordSet& records) const {
  DBG << "Executing SORTBY with sortkeys: " << sortkeys_.size() << "\n";
  if (records.size() > max_) {
    // Sadly std::priority_queue can't operate on unique_ptr's. so we need an
    // extra copy
    SortFunctor<Record*> sorter{&sortkeys_};
    std::priority_queue<Record*, std::vector<Record*>, SortFunctor<Record*>>
        heap(sorter);
    for (auto i = 0; i < max_; ++i) {
      heap.push(records.pop_front().release());
    }
    while (!records.empty()) {
      heap.push(records.pop_front().release());
      auto top = RecordPtr(heap.top());  // no leak....
      heap.pop();
    }
    while (!heap.empty()) {
      records.emplace_front(RecordPtr(heap.top()));
      heap.pop();
    }
  } else {
    SortFunctor<RecordPtr> sorter{&sortkeys_};
    std::stable_sort(records.begin(), records.end(), sorter);
  }
  return absl::OkStatus();
}

absl::Status GroupBy::Execute(RecordSet& records) const {
  DBG << "Executing GROUPBY with groups: " << groups_.size()
      << " and reducers: " << reducers_.size() << "\n";
  absl::flat_hash_map<GroupKey,
                      absl::InlinedVector<std::unique_ptr<ReducerInstance>, 4>>
      groups;
  size_t record_field_count = 0;
  while (!records.empty()) {
    auto record = records.pop_front();
    if (record_field_count == 0) {
      record_field_count = record->fields_.size();
    } else {
      CHECK(record_field_count == record->fields_.size());
    }
    GroupKey k;
    // todo: How do we handle keys that have a missing attribute in the key??
    // Skip them?
    for (auto& g : groups_) {
      k.keys_.emplace_back(g->GetValue(ctx, *record));
    }
    DBG << "Record: " << *record << " GroupKey: " << k << "\n";
    auto [group_it, inserted] = groups.try_emplace(std::move(k));
    if (inserted) {
      DBG << "Was inserted, now have " << groups.size() << " groups\n";
      for (auto& reducer : reducers_) {
        group_it->second.emplace_back(reducer.info_->make_instance());
      }
    }
    for (auto i = 0; i < reducers_.size(); ++i) {
      absl::InlinedVector<expr::Value, 4> args;
      for (auto& nargs : reducers_[i].args_) {
        args.emplace_back(nargs->Evaluate(ctx, *record));
      }
      group_it->second[i]->ProcessRecord(args);
    }
  }
  for (auto& group : groups) {
    DBG << "Making record for group " << group.first << "\n";
    RecordPtr record = std::make_unique<Record>(record_field_count);
    CHECK(groups_.size() == group.first.keys_.size());
    for (auto i = 0; i < groups_.size(); ++i) {
      SetField(*record, *groups_[i], group.first.keys_[i]);
    }
    CHECK(reducers_.size() == group.second.size());
    for (auto i = 0; i < reducers_.size(); ++i) {
      SetField(*record, *reducers_[i].output_, group.second[i]->GetResult());
    }
    DBG << "Record (" << records.size() << ") is : " << *record << "\n";
    records.push_back(std::move(record));
  }
  return absl::OkStatus();
}

class Count : public GroupBy::ReducerInstance {
  size_t count_{0};
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    count_++;
  }
  expr::Value GetResult() const override { return expr::Value(double(count_)); }
};

class Min : public GroupBy::ReducerInstance {
  expr::Value min_;
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    if (values[0].IsNil()) {
      return;
    }
    if (min_.IsNil()) {
      DBG << "First Value Min is " << values[0] << "\n";
      min_ = values[0];
    } else if (min_ > values[0]) {
      DBG << " New Min: " << values[0] << "\n";
      min_ = values[0];
    } else {
      DBG << "Not new Min: " << values[0] << "\n";
    }
  }
  expr::Value GetResult() const override { return min_; }
};

class Max : public GroupBy::ReducerInstance {
  expr::Value max_;
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    if (values[0].IsNil()) {
      return;
    }
    if (max_.IsNil()) {
      max_ = values[0];
    } else if (max_ < values[0]) {
      max_ = values[0];
    }
  }
  expr::Value GetResult() const override { return max_; }
};

class Sum : public GroupBy::ReducerInstance {
  double sum_{0};
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    auto val = values[0].AsDouble();
    if (val) {
      sum_ += *val;
    }
  }
  expr::Value GetResult() const override { return expr::Value(sum_); }
};

class Avg : public GroupBy::ReducerInstance {
  double sum_{0};
  size_t count_{0};
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    auto val = values[0].AsDouble();
    if (val) {
      sum_ += *val;
      count_++;
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(count_ ? sum_ / count_ : 0.0);
  }
};

class Stddev : public GroupBy::ReducerInstance {
  double sum_{0}, sq_sum_{0};
  size_t count_{0};
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    auto val = values[0].AsDouble();
    if (val) {
      sum_ += *val;
      sq_sum_ += (*val) * (*val);
      count_++;
    }
  }
  expr::Value GetResult() const override {
    if (count_ <= 1) {
      return expr::Value(0.0);
    } else {
      double variance = (sq_sum_ - (sum_ * sum_) / count_) / (count_ - 1);
      return expr::Value(std::sqrt(variance));
    }
  }
};

class CountDistinct : public GroupBy::ReducerInstance {
  absl::flat_hash_set<expr::Value> values_;
  void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) override {
    if (!values[0].IsNil()) {
      values_.insert(values[0]);
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(double(values_.size()));
  }
};

template <typename T>
std::unique_ptr<GroupBy::ReducerInstance> MakeReducer() {
  return std::unique_ptr<GroupBy::ReducerInstance>(std::make_unique<T>());
}

absl::flat_hash_map<std::string, GroupBy::ReducerInfo> GroupBy::reducerTable{
    {"AVG", GroupBy::ReducerInfo{"AVG", 1, 1, &MakeReducer<Avg>}},
    {"COUNT", GroupBy::ReducerInfo{"COUNT", 0, 0, &MakeReducer<Count>}},
    {"COUNT_DISTINCT",
     GroupBy::ReducerInfo{"COUNT_DISTINCT", 1, 1, &MakeReducer<CountDistinct>}},
    {"MIN", GroupBy::ReducerInfo{"MIN", 1, 1, &MakeReducer<Min>}},
    {"MAX", GroupBy::ReducerInfo{"MAX", 1, 1, &MakeReducer<Max>}},
    {"STDDEV", GroupBy::ReducerInfo{"STDDEV", 1, 1, &MakeReducer<Stddev>}},
    {"SUM", GroupBy::ReducerInfo{"SUM", 1, 1, &MakeReducer<Sum>}},
};

}  // namespace aggregate
}  // namespace valkey_search
