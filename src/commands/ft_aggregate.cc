/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate.h"

#include <algorithm>
#include <ranges>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "ft_search_parser.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/commands/ft_create_parser.h"
#include "src/commands/ft_search.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/fanout.h"
#include "src/query/response_generator.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/debug.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search {
namespace aggregate {

struct RealIndexInterface : public IndexInterface {
  std::shared_ptr<IndexSchema> schema_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const override {
    VMSDK_ASSIGN_OR_RETURN(auto indexer, schema_->GetIndex(s));
    return indexer->GetIndexerType();
  }
  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const override {
    return schema_->GetIdentifier(alias);
  }
  absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const override {
    return schema_->GetAlias(identifier);
  }
  RealIndexInterface(std::shared_ptr<IndexSchema> schema) : schema_(schema) {}
};

absl::Status ManipulateReturnsClause(AggregateParameters &params) {
  // Figure out what fields actually need to be returned by the aggregation
  // operation. And modify the common search returns list accordingly
  DBG << "Manipulating returns clause for: " << params.index_schema_name
      << "\n";
  CHECK(!params.no_content);
  if (params.loadall_) {
    DBG << "**LOADALL**\n";
    CHECK(params.return_attributes.empty());
  } else if (params.loads_.empty()) {
    // Nothing, don't load anything
    params.no_content = true;
  } else {
    DBG << "LOADING: ";
    for (const auto &load : params.loads_) {
      DBG << " " << load;
      //
      // Skip loading of the score and the key, we always get those...
      //
      if (load == "__key") {
        DBG << " *skipped*";
        params.load_key = true;
        continue;
      }
      if (load == vmsdk::ToStringView(params.score_as.get())) {
        DBG << " *skipping score*";
        continue;
      }
      VMSDK_ASSIGN_OR_RETURN(auto indexer, params.index_schema->GetIndex(load));
      auto field_type = indexer->GetIndexerType();
      auto schema_identifier = params.index_schema->GetIdentifier(load);
      if (schema_identifier.ok()) {
        DBG << " (alias: " << *schema_identifier << ", " << load << ")";
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(*schema_identifier),
            .attribute_alias = vmsdk::MakeUniqueValkeyString(load),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(*schema_identifier, load, field_type);
      } else {
        DBG << " " << load;
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(load),
            .attribute_alias = vmsdk::UniqueValkeyString(),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(load, load, indexes::IndexerType::kNone);
      }
    }
    DBG << "\n";
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<AggregateParameters>> ParseCommand(
    ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
    const SchemaManager &schema_manager) {
  static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  vmsdk::ArgsIterator itr{argv, argc};
  std::string index_schema_name;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_schema_name));
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  RealIndexInterface index_interface(index_schema);
  auto params = std::make_unique<AggregateParameters>(
      options::GetDefaultTimeoutMs().GetValue(), &index_interface);
  DBG << "AggregateParameters created for index: " << index_schema_name << " @"
      << (void *)params.get() << "\n";
  params->index_schema_name = std::move(index_schema_name);
  params->index_schema = std::move(index_schema);

  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, params->parse_vars.query_string));

  VMSDK_RETURN_IF_ERROR(PreParseQueryString(*params));
  // Ensure that key is first value if it gets included...
  CHECK(params->AddRecordAttribute("__key", "__key",
                                   indexes::IndexerType::kNone) == 0);
  auto score_sv = vmsdk::ToStringView(params->score_as.get());
  CHECK(params->AddRecordAttribute(score_sv, score_sv,
                                   indexes::IndexerType::kNone) == 1);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*params, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }

  if (params->dialect < 2 || params->dialect > 4) {
    return absl::InvalidArgumentError("Only Dialects 2, 3 and 4 are supported");
  }

  params->limit.number =
      std::numeric_limits<uint64_t>::max();  // Override default of 10 from
                                             // search

  VMSDK_RETURN_IF_ERROR(PostParseQueryString(*params));
  VMSDK_RETURN_IF_ERROR(ManipulateReturnsClause(*params));

  DBG << "At end of parse: " << *params << "\n";
  params->parse_vars.ClearAtEndOfParse();
  return std::move(params);
}

bool ReplyWithValue(ValkeyModuleCtx *ctx,
                    data_model::AttributeDataType data_type,
                    std::string_view name, indexes::IndexerType field_type,
                    const expr::Value &value, int dialect) {
  if (value.IsNil()) {
    return false;
  } else {
    DBG << "ReplyWithValue " << name << "\n";
    if (data_type == data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
      ValkeyModule_ReplyWithSimpleString(ctx, name.data());
      auto value_sv = value.AsStringView();
      ValkeyModule_ReplyWithStringBuffer(ctx, value_sv.data(), value_sv.size());
      DBG << "HASH: " << name << ":" << value_sv << "\n";
    } else {
      char double_storage[50];
      std::string_view value_view;
      if (name == "$") {
        value_view = value.AsStringView();
        DBG << "Overriding for field name of $ " << int(field_type) << "\n";
        DBG << "Input: " << value.AsStringView() << "\n";
      } else {
        switch (field_type) {
          case indexes::IndexerType::kTag:
          case indexes::IndexerType::kNone: {
            value_view = value.AsStringView();
            DBG << "JSON kTag: " << value_view << "\n";
            break;
          }
          case indexes::IndexerType::kNumeric: {
            auto dble = value.AsDouble();
            if (!dble) {
              return false;
            }
            auto double_size = snprintf(double_storage, sizeof(double_storage),
                                        "%.11g", *dble);
            value_view = std::string_view(double_storage, double_size);
            DBG << "JSON kNumeric:" << value_view << "\n";
            break;
          }
          default:
            DBG << "Unsupported field type for reply: " << int(field_type)
                << "\n";
            assert("Unsupported field type" == nullptr);
        }
      }
      ValkeyModule_ReplyWithSimpleString(ctx, name.data());
      if (dialect == 2) {
        ValkeyModule_ReplyWithStringBuffer(ctx, value_view.data(),
                                           value_view.size());
      } else {
        std::string s;
        s = '[';
        s += value_view;
        s += ']';
        DBG << "Dialect != 2: " << s << "\n";
        ValkeyModule_ReplyWithStringBuffer(ctx, s.data(), s.size());
      }
    }
  }
  return true;
}

absl::Status SendReplyInner(ValkeyModuleCtx *ctx,
                            std::deque<indexes::Neighbor> &neighbors,
                            AggregateParameters &parameters) {
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
  if (!identifier.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return identifier.status();
  }

  query::ProcessNeighborsForReply(
      ctx, parameters.index_schema->GetAttributeDataType(), neighbors,
      parameters, identifier.value());

  size_t key_index = 0, scores_index = 0;
  if (parameters.load_key) {
    key_index = parameters.AddRecordAttribute("__key", "__key",
                                              indexes::IndexerType::kNone);
  }
  if (parameters.IsVectorQuery()) {
    auto score_sv = vmsdk::ToStringView(parameters.score_as.get());
    scores_index = parameters.AddRecordAttribute(score_sv, score_sv,
                                                 indexes::IndexerType::kNone);
  }

  //
  //  1. Process the collected Neighbors into Aggregate Records.
  //
  auto data_type = parameters.index_schema->GetAttributeDataType().ToProto();
  RecordSet records(&parameters);
  // Todo: fix this  for (auto &n : neighbors) {
  for (auto &n : neighbors) {
    auto rec =
        std::make_unique<Record>(parameters.record_indexes_by_alias_.size());
    DBG << "Neighbor: " << n << " Empty Record:" << *rec << "\n";
    if (parameters.load_key) {
      rec->fields_.at(key_index) = expr::Value(n.external_id.get()->Str());
    }
    if (/* todo: parameters.addscores_ */ true) {
      rec->fields_.at(scores_index) = expr::Value(n.distance);
    }
    // For the fields that were fetched, stash them into the RecordSet
    if (n.attribute_contents.has_value() && !parameters.no_content) {
      for (auto &[name, records_map_value] : *n.attribute_contents) {
        auto value = vmsdk::ToStringView(records_map_value.value.get());
        size_t record_index;
        bool found_index = false;
        if (auto by_alias = parameters.record_indexes_by_alias_.find(name);
            by_alias != parameters.record_indexes_by_alias_.end()) {
          record_index = by_alias->second;
          found_index = true;
          assert(record_index < rec->field_.size());
        } else if (auto by_identifier =
                       parameters.record_indexes_by_identifier_.find(name);
                   by_identifier !=
                   parameters.record_indexes_by_identifier_.end()) {
          record_index = by_identifier->second;
          found_index = true;
          assert(record_index < rec->field_.size());
        }
        if (found_index) {
          // Need to find the field type
          indexes::IndexerType field_type =
              parameters.record_info_by_index_[record_index].data_type_;
          DBG << "Attribute_contents: " << name << " : " << value
              << " Index:" << record_index << " FieldType:" << int(field_type)
              << "\n";
          switch (field_type) {
            case indexes::IndexerType::kNumeric: {
              auto numeric_value = vmsdk::To<double>(value);
              if (numeric_value.ok()) {
                rec->fields_[record_index] = expr::Value(numeric_value.value());
              } else {
                // Skip this field, it contains an invalid number....
                // todo Prove that skipping this field is the right thing to
                // do
              }
              break;
            }
            default:
              if (data_type ==
                  data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
                rec->fields_[record_index] = expr::Value(value);
              } else {
                auto v = vmsdk::JsonUnquote(value);
                if (v) {
                  DBG << "De-quoting:\n"
                      << value << "\nBecame:\n"
                      << *v << "\n";
                  rec->fields_[record_index] = expr::Value(std::move(*v));
                } else {
                  goto drop_record;
                }
              }
              break;
          }
          // DBG << "After set record is " << *rec << "\n";
        } else {
          DBG << "Attribute_contents: " << name << " : " << value
              << " Extra:\n";
          rec->extra_fields_.push_back(
              std::make_pair(std::string(name), expr::Value(value)));
        }
      }
    }
    records.push_back(std::move(rec));
  drop_record:;
  }
  DBG << "After Record Fetch\n" << records << "\n";
  //
  //  2. Perform the aggregation stages
  //
  for (auto &stage : parameters.stages_) {
    // Todo Check for timeout
    VMSDK_RETURN_IF_ERROR(stage->Execute(records));
  }
  DBG << ">> Finished stages\n" << records;

  //
  //  3. Generate the result
  //
  ValkeyModule_ReplyWithArray(ctx, 1 + records.size());
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(records.size()));
  while (!records.empty()) {
    auto rec = records.pop_front();
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    //
    // First the referenced fields
    //
    size_t array_count = 0;
    CHECK(rec->fields_.size() <= parameters.record_info_by_index_.size());
    for (size_t i = 0; i < rec->fields_.size(); ++i) {
      if (ReplyWithValue(
              ctx, parameters.index_schema->GetAttributeDataType().ToProto(),
              parameters.record_info_by_index_[i].identifier_,
              parameters.record_info_by_index_[i].data_type_, rec->fields_[i],
              parameters.dialect)) {
        array_count += 2;
      }
    }
    //
    // Now the unreferenced ones
    //
    for (const auto &[name, value] : rec->extra_fields_) {
      if (ReplyWithValue(
              ctx, parameters.index_schema->GetAttributeDataType().ToProto(),
              name, indexes::IndexerType::kNone, value, parameters.dialect)) {
        array_count += 2;
      }
    }
    DBG << " (Total length) " << array_count << "\n";
    ValkeyModule_ReplySetArrayLength(ctx, array_count);
  }
  return absl::OkStatus();
}

void SendAggReply(ValkeyModuleCtx *ctx,
                  std::deque<indexes::Neighbor> &neighbors,
                  AggregateParameters &parameters) {
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
  auto result = SendReplyInner(ctx, neighbors, parameters);
  if (!result.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, result.message().data());
  }
}

}  // namespace aggregate

CONTROLLED_BOOLEAN(AggForceReplicasOnly, false);

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  auto status = [&]() {
    auto &schema_manager = SchemaManager::Instance();
    VMSDK_ASSIGN_OR_RETURN(
        auto parameters,
        aggregate::ParseCommand(ctx, argv + 1, argc - 1, schema_manager));
    parameters->index_schema->ProcessMultiQueue();
    bool inside_multi =
        (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_MULTI) != 0;
    if (ABSL_PREDICT_FALSE(!ValkeySearch::Instance().SupportParallelQueries() ||
                           inside_multi)) {
      VMSDK_ASSIGN_OR_RETURN(
          auto neighbors,
          query::Search(*parameters, query::SearchMode::kLocal));
      SendAggReply(ctx, neighbors, *parameters);
      return absl::OkStatus();
    }
    vmsdk::BlockedClient blocked_client(ctx, async::Reply, async::Timeout,
                                        async::Free, 0);
    blocked_client.MeasureTimeStart();
    auto on_done_callback = [blocked_client = std::move(blocked_client)](
                                auto &neighbors, auto parameters) mutable {
      auto result = std::make_unique<async::Result>(async::Result{
          .neighbors = std::move(neighbors),
          .parameters = std::move(parameters),
      });
      blocked_client.SetReplyPrivateData(result.release());
    };

    if (ValkeySearch::Instance().UsingCoordinator() &&
        ValkeySearch::Instance().IsCluster() && !parameters->local_only) {
      auto mode = /* !vmsdk::IsReadOnly(ctx) ? query::fanout::kPrimaries ? */
          AggForceReplicasOnly.GetValue()
              ? query::fanout::FanoutTargetMode::kReplicasOnly
              : query::fanout::FanoutTargetMode::kRandom;
      auto search_targets = query::fanout::GetSearchTargetsForFanout(ctx, mode);
      return query::fanout::PerformSearchFanoutAsync(
          ctx, search_targets,
          ValkeySearch::Instance().GetCoordinatorClientPool(),
          std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
          std::move(on_done_callback));
    }
    return query::SearchAsync(
        std::move(parameters), ValkeySearch::Instance().GetReaderThreadPool(),
        std::move(on_done_callback), query::SearchMode::kLocal);
    CHECK(false);
  }();
  if (!status.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
  }
  return status;
}

}  // namespace valkey_search
