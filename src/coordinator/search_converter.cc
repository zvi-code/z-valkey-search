/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::coordinator {

absl::StatusOr<std::unique_ptr<query::Predicate>> GRPCPredicateToPredicate(
    const Predicate& predicate, std::shared_ptr<IndexSchema> index_schema,
    absl::flat_hash_set<std::string>& attribute_identifiers) {
  switch (predicate.predicate_case()) {
    case Predicate::kTag: {
      VMSDK_ASSIGN_OR_RETURN(
          auto index,
          index_schema->GetIndex(predicate.tag().attribute_alias()));
      if (index->GetIndexerType() != indexes::IndexerType::kTag) {
        return absl::InvalidArgumentError(
            absl::StrCat("`", predicate.tag().attribute_alias(),
                         "` is not indexed as a tag field"));
      }
      VMSDK_ASSIGN_OR_RETURN(
          auto identifier,
          index_schema->GetIdentifier(predicate.tag().attribute_alias()));
      attribute_identifiers.insert(identifier);
      auto tag_index = dynamic_cast<indexes::Tag*>(index.get());
      VMSDK_ASSIGN_OR_RETURN(
          auto parsed_tags,
          tag_index->ParseSearchTags(predicate.tag().raw_tag_string(),
                                     tag_index->GetSeparator()));
      auto tag_predicate = std::make_unique<query::TagPredicate>(
          tag_index, predicate.tag().attribute_alias(), identifier,
          predicate.tag().raw_tag_string(), parsed_tags);
      return tag_predicate;
    }
    case Predicate::kNumeric: {
      VMSDK_ASSIGN_OR_RETURN(
          auto index,
          index_schema->GetIndex(predicate.numeric().attribute_alias()));
      if (index->GetIndexerType() != indexes::IndexerType::kNumeric) {
        return absl::InvalidArgumentError(
            absl::StrCat("`", predicate.numeric().attribute_alias(),
                         "` is not indexed as a numeric field"));
      }
      VMSDK_ASSIGN_OR_RETURN(
          auto identifier,
          index_schema->GetIdentifier(predicate.numeric().attribute_alias()));
      attribute_identifiers.insert(identifier);
      auto numeric_index = dynamic_cast<indexes::Numeric*>(index.get());
      auto numeric_predicate = std::make_unique<query::NumericPredicate>(
          numeric_index, predicate.numeric().attribute_alias(), identifier,
          predicate.numeric().start(), predicate.numeric().is_inclusive_start(),
          predicate.numeric().end(), predicate.numeric().is_inclusive_end());
      return numeric_predicate;
    }
    case Predicate::kAnd: {
      VMSDK_ASSIGN_OR_RETURN(
          auto lhs_predicate,
          GRPCPredicateToPredicate(predicate.and_().lhs(), index_schema,
                                   attribute_identifiers));
      VMSDK_ASSIGN_OR_RETURN(
          auto rhs_predicate,
          GRPCPredicateToPredicate(predicate.and_().rhs(), index_schema,
                                   attribute_identifiers));
      return std::make_unique<query::ComposedPredicate>(
          std::move(lhs_predicate), std::move(rhs_predicate),
          query::LogicalOperator::kAnd);
    }
    case Predicate::kOr: {
      VMSDK_ASSIGN_OR_RETURN(
          auto lhs_predicate,
          GRPCPredicateToPredicate(predicate.or_().lhs(), index_schema,
                                   attribute_identifiers));
      VMSDK_ASSIGN_OR_RETURN(
          auto rhs_predicate,
          GRPCPredicateToPredicate(predicate.or_().rhs(), index_schema,
                                   attribute_identifiers));
      return std::make_unique<query::ComposedPredicate>(
          std::move(lhs_predicate), std::move(rhs_predicate),
          query::LogicalOperator::kOr);
    }
    case Predicate::kNegate: {
      VMSDK_ASSIGN_OR_RETURN(
          auto predicate,
          GRPCPredicateToPredicate(predicate.negate().predicate(), index_schema,
                                   attribute_identifiers));
      return std::make_unique<query::NegatePredicate>(std::move(predicate));
    }
    case Predicate::PREDICATE_NOT_SET:
      return absl::InvalidArgumentError("Predicate not set");
  }
  CHECK(false);
}

absl::StatusOr<std::unique_ptr<query::VectorSearchParameters>>
GRPCSearchRequestToParameters(const SearchIndexPartitionRequest& request, grpc::CallbackServerContext *context) {
  auto parameters = std::make_unique<query::VectorSearchParameters>(request.timeout_ms(), context);
  parameters->index_schema_name = request.index_schema_name();
  parameters->attribute_alias = request.attribute_alias();
  VMSDK_ASSIGN_OR_RETURN(
      parameters->index_schema,
      SchemaManager::Instance().GetIndexSchema(0, request.index_schema_name()));
  if (request.has_score_as()) {
    parameters->score_as = vmsdk::MakeUniqueValkeyString(request.score_as());
  } else {
    VMSDK_ASSIGN_OR_RETURN(parameters->score_as,
                           parameters->index_schema->DefaultReplyScoreAs(
                               request.attribute_alias()));
  }
  parameters->query = request.query();
  parameters->dialect = request.dialect();
  parameters->k = request.k();
  parameters->ef = request.ef();
  parameters->limit = query::LimitParameter{request.limit().first_index(),
                                            request.limit().number()};
  parameters->no_content = request.no_content();
  if (request.has_root_filter_predicate()) {
    VMSDK_ASSIGN_OR_RETURN(
        parameters->filter_parse_results.root_predicate,
        GRPCPredicateToPredicate(
            request.root_filter_predicate(), parameters->index_schema,
            parameters->filter_parse_results.filter_identifiers));
  }
  for (auto& return_parameter : request.return_parameters()) {
    parameters->return_attributes.emplace_back(query::ReturnAttribute(
        vmsdk::MakeUniqueValkeyString(return_parameter.identifier()),
        vmsdk::MakeUniqueValkeyString(return_parameter.alias())));
  }
  return parameters;
}

std::unique_ptr<Predicate> PredicateToGRPCPredicate(
    const query::Predicate& predicate) {
  switch (predicate.GetType()) {
    case query::PredicateType::kTag: {
      auto tag_predicate = dynamic_cast<const query::TagPredicate*>(&predicate);
      auto tag_predicate_proto = std::make_unique<Predicate>();
      tag_predicate_proto->mutable_tag()->set_attribute_alias(
          tag_predicate->GetAlias());
      tag_predicate_proto->mutable_tag()->set_raw_tag_string(
          tag_predicate->GetTagString());
      return tag_predicate_proto;
    }
    case query::PredicateType::kNumeric: {
      auto numeric_predicate =
          dynamic_cast<const query::NumericPredicate*>(&predicate);
      auto numeric_predicate_proto = std::make_unique<Predicate>();
      numeric_predicate_proto->mutable_numeric()->set_attribute_alias(
          std::string(numeric_predicate->GetAlias()));
      numeric_predicate_proto->mutable_numeric()->set_start(
          numeric_predicate->GetStart());
      numeric_predicate_proto->mutable_numeric()->set_is_inclusive_start(
          numeric_predicate->IsStartInclusive());
      numeric_predicate_proto->mutable_numeric()->set_end(
          numeric_predicate->GetEnd());
      numeric_predicate_proto->mutable_numeric()->set_is_inclusive_end(
          numeric_predicate->IsEndInclusive());
      return numeric_predicate_proto;
    }
    case query::PredicateType::kComposedAnd: {
      auto and_predicate_proto = std::make_unique<Predicate>();
      auto composed_and_predicate =
          dynamic_cast<const query::ComposedPredicate*>(&predicate);
      and_predicate_proto->mutable_and_()->set_allocated_lhs(
          PredicateToGRPCPredicate(*composed_and_predicate->GetLhsPredicate())
              .release());
      and_predicate_proto->mutable_and_()->set_allocated_rhs(
          PredicateToGRPCPredicate(*composed_and_predicate->GetRhsPredicate())
              .release());
      return and_predicate_proto;
    }
    case query::PredicateType::kComposedOr: {
      auto or_predicate_proto = std::make_unique<Predicate>();
      auto composed_or_predicate =
          dynamic_cast<const query::ComposedPredicate*>(&predicate);
      or_predicate_proto->mutable_or_()->set_allocated_lhs(
          PredicateToGRPCPredicate(*composed_or_predicate->GetLhsPredicate())
              .release());
      or_predicate_proto->mutable_or_()->set_allocated_rhs(
          PredicateToGRPCPredicate(*composed_or_predicate->GetRhsPredicate())
              .release());
      return or_predicate_proto;
    }
    case query::PredicateType::kNegate: {
      auto negate_predicate_proto = std::make_unique<Predicate>();
      auto negate_predicate =
          dynamic_cast<const query::NegatePredicate*>(&predicate);
      negate_predicate_proto->mutable_negate()->set_allocated_predicate(
          PredicateToGRPCPredicate(*negate_predicate->GetPredicate())
              .release());
      return negate_predicate_proto;
    }
    case query::PredicateType::kNone: {
      return nullptr;
    }
  }
  CHECK(false);
}

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::VectorSearchParameters& parameters) {
  auto request = std::make_unique<SearchIndexPartitionRequest>();
  request->set_index_schema_name(parameters.index_schema_name);
  request->set_attribute_alias(parameters.attribute_alias);
  request->set_score_as(vmsdk::ToStringView(parameters.score_as.get()));
  request->set_query(parameters.query);
  request->set_dialect(parameters.dialect);
  request->set_k(parameters.k);
  if (parameters.ef.has_value()) {
    request->set_ef(parameters.ef.value());
  }
  request->mutable_limit()->set_first_index(parameters.limit.first_index);
  request->mutable_limit()->set_number(parameters.limit.number);
  request->set_timeout_ms(parameters.timeout_ms);
  request->set_no_content(parameters.no_content);
  if (parameters.filter_parse_results.root_predicate != nullptr) {
    request->set_allocated_root_filter_predicate(
        PredicateToGRPCPredicate(
            *parameters.filter_parse_results.root_predicate)
            .release());
  } else {
    request->clear_root_filter_predicate();
  }
  for (const auto& return_attribute : parameters.return_attributes) {
    auto return_parameter = request->add_return_parameters();
    return_parameter->set_identifier(
        vmsdk::ToStringView(return_attribute.identifier.get()));
    return_parameter->set_alias(
        vmsdk::ToStringView(return_attribute.alias.get()));
  }
  return request;
}

}  // namespace valkey_search::coordinator
