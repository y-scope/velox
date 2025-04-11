/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/clp/search_lib/ClpCursor.h"

#include <filesystem>
#include <stdexcept>

#include <spdlog/spdlog.h>

#include "clp_s/search/EvaluateTimestampIndex.hpp"
#include "clp_s/search/ast/ConvertToExists.hpp"
#include "clp_s/search/ast/EmptyExpr.hpp"
#include "clp_s/search/ast/NarrowTypes.hpp"
#include "clp_s/search/ast/OrOfAndForm.hpp"
#include "clp_s/search/ast/SearchUtils.hpp"
#include "clp_s/search/kql/kql.hpp"

using namespace clp_s;
using namespace clp_s::search;
using namespace clp_s::search::ast;

namespace facebook::velox::connector::clp::search_lib {
ClpCursor::ClpCursor(InputSource inputSource, const std::string& archivePath)
    : errorCode_(ErrorCode::QueryNotInitialized),
      inputSource_(inputSource),
      archivePath_(std::move(archivePath)) {}

ErrorCode ClpCursor::loadArchive(const std::vector<Field>& outputColumns) {
  auto networkAuthOption = inputSource_ == InputSource::Filesystem
      ? NetworkAuthOption{.method = AuthMethod::None}
      : NetworkAuthOption{.method = AuthMethod::S3PresignedUrlV4};

  archiveReader_->open(
      get_path_object_for_raw_path(archivePath_), networkAuthOption);
  auto timestampDict = archiveReader_->get_timestamp_dictionary();
  auto schemaTree = archiveReader_->get_schema_tree();
  auto schemaMap = archiveReader_->get_schema_map();

  EvaluateTimestampIndex timestampIndex(timestampDict);
  if (clp_s::EvaluatedValue::False == timestampIndex.run(expr_)) {
    SPDLOG_DEBUG("No matching timestamp ranges for query '{}'", query_);
    return ErrorCode::InvalidTimestampRange;
  }

  auto schemaMatch = std::make_shared<SchemaMatch>(schemaTree, schemaMap);
  if (expr_ = schemaMatch->run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    SPDLOG_ERROR("No matching schemas for query '{}'", query_);
    return ErrorCode::SchemaNotFound;
  }

  projection_ = std::make_shared<Projection>(
      outputColumns.empty() ? ReturnAllColumns : ReturnSelectedColumns);
  try {
    for (auto const& column : outputColumns) {
      std::vector<std::string> descriptorTokens;
      std::string descriptorNamespace;
      if (false ==
          tokenize_column_descriptor(
              column.name, descriptorTokens, descriptorNamespace)) {
        SPDLOG_ERROR("Can not tokenize invalid column: \"{}\"", column);
        return ErrorCode::InternalError;
      }

      auto columnDescriptor = ColumnDescriptor::create_from_escaped_tokens(
          descriptorTokens, descriptorNamespace);
      switch (column.type) {
        case ColumnType::String:
          columnDescriptor->set_matching_types(
              LiteralType::ClpStringT | LiteralType::VarStringT |
              LiteralType::EpochDateT);
          break;
        case ColumnType::Integer:
          columnDescriptor->set_matching_types(LiteralType::IntegerT);
          break;
        case ColumnType::Float:
          columnDescriptor->set_matching_types(LiteralType::FloatT);
          break;
        case ColumnType::Boolean:
          columnDescriptor->set_matching_types(LiteralType::BooleanT);
          break;
        case ColumnType::Array:
          columnDescriptor->set_matching_types(LiteralType::ArrayT);
          break;
        default:
          break;
      }

      projection_->add_column(columnDescriptor);
    }
  } catch (TraceableException& e) {
    SPDLOG_ERROR("{}", e.what());
    return ErrorCode::InternalError;
  }
  projection_->resolve_columns(schemaTree);
  archiveReader_->set_projection(projection_);

  archiveReader_->read_metadata();

  matchedSchemas_.clear();
  for (auto schemaId : archiveReader_->get_schema_ids()) {
    if (schemaMatch->schema_matched(schemaId)) {
      matchedSchemas_.push_back(schemaId);
    }
  }

  if (matchedSchemas_.empty()) {
    return ErrorCode::SchemaNotFound;
  }

  EvaluateTimestampIndex timestamp_index(timestampDict);
  if (EvaluatedValue::False == timestamp_index.run(expr_)) {
    SPDLOG_INFO("No matching timestamp ranges for query '{}'", query_);
    return ErrorCode::InvalidTimestampRange;
  }

  archiveReader_->read_variable_dictionary();
  archiveReader_->read_log_type_dictionary();
  archiveReader_->read_array_dictionary();

  currentSchemaIndex_ = 0;
  currentSchemaTableLoaded_ = false;
  return ErrorCode::Success;
}

ErrorCode ClpCursor::preprocessQuery() {
  auto queryStream = std::istringstream(query_);
  expr_ = kql::parse_kql_expression(queryStream);
  if (nullptr == expr_) {
    SPDLOG_ERROR("Failed to parse query '{}'", query_);
    return ErrorCode::InvalidQuerySyntax;
  }

  if (std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    SPDLOG_DEBUG("Query '{}' is logically false", query_);
    return ErrorCode::LogicalError;
  }

  OrOfAndForm standardizePass;
  if (expr_ = standardizePass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    SPDLOG_DEBUG("Query '{}' is logically false", query_);
    return ErrorCode::LogicalError;
  }

  NarrowTypes narrowPass;
  if (expr_ = narrowPass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    SPDLOG_DEBUG("Query '{}' is logically false", query_);
    return ErrorCode::LogicalError;
  }

  ConvertToExists convertPass;
  if (expr_ = convertPass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    SPDLOG_DEBUG("Query '{}' is logically false", query_);
    return ErrorCode::LogicalError;
  }

  return ErrorCode::Success;
}

ErrorCode ClpCursor::executeQuery(
    const std::string& query,
    const std::vector<Field>& outputColumns) {
  query_ = query;
  outputColumns_ = outputColumns;
  return preprocessQuery();
}

ErrorCode ClpCursor::fetch_next(
    size_t numRows,
    std::vector<size_t> filteredRows) {
  if (ErrorCode::Success != errorCode_) {
    return errorCode_;
  }

  if (false == currentArchiveLoaded_) {
    errorCode_ = loadArchive(outputColumns_);
    if (ErrorCode::Success != errorCode_) {
      return errorCode_;
    }

    archiveReader_->open_packed_streams();
    currentArchiveLoaded_ = true;
    queryRunner_ = std::make_shared<ClpQueryRunner>(
        schemaMatch_, expr_, archiveReader_, false, projection_);
    queryRunner_->populate_string_queries();
  }

  while (currentSchemaIndex_ < matchedSchemas_.size()) {
    if (false == currentSchemaTableLoaded_) {
      currentSchemaId_ = matchedSchemas_[currentSchemaIndex_];
      queryRunner_->setup_schema(currentSchemaId_);
      queryRunner_->populate_searched_wildcard_columns();
      if (auto expressionValue = queryRunner_->constant_propagate();
          EvaluatedValue::False != expressionValue) {
        queryRunner_->add_wildcard_columns_to_searched_columns();

        auto& reader =
            archiveReader_->read_schema_table(currentSchemaId_, false, false);
        reader.initialize_filter_with_column_map(queryRunner_.get());

        errorCode_ = ErrorCode::Success;
        currentSchemaTableLoaded_ = true;
      } else {
        currentSchemaIndex_ += 1;
        currentSchemaTableLoaded_ = false;
        errorCode_ = ErrorCode::DictionaryNotFound;
        continue;
      }
    }

    queryRunner_->fetchNext(numRows, filteredRows);
    if (false == filteredRows.empty()) {
      return ErrorCode::Success;
    }

    currentSchemaIndex_ += 1;
    currentSchemaTableLoaded_ = false;
  }

  return {};
}

} // namespace facebook::velox::connector::clp::search_lib
