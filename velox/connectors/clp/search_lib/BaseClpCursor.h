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

#pragma once

#include <string>
#include <vector>

#include "archive/ClpQueryRunner.h"
#include "connectors/clp/ClpConnectorSplit.h"

namespace clp_s {
enum class InputSource : uint8_t;
class ArchiveReader;
class BaseColumnReader;
} // namespace clp_s

namespace clp_s::search {
class Projection;
class SchemaMatch;
} // namespace clp_s::search

namespace clp_s::search::ast {
class Expression;
} // namespace clp_s::search::ast

namespace facebook::velox::connector::clp::search_lib {

enum class ErrorCode {
  DictionaryNotFound,
  InternalError,
  InvalidQuerySyntax,
  InvalidTimestampRange,
  LogicalError,
  QueryNotInitialized,
  SchemaNotFound,
  Success
};

enum class ColumnType {
  Array,
  Boolean,
  Float,
  Integer,
  String,
  Timestamp,
  Unknown = -1
};

struct Field {
  ColumnType type;
  std::string name;
};

/// A query execution interface that manages the lifecycle of a query on a CLP-S
/// archive, including parsing and validating the query, loading the relevant
/// schemas and archives, applying filters, and iterating over the results. It
/// abstracts away the low-level details of archive access and schema matching
/// while supporting projection and batch-oriented retrieval of filtered rows.
class BaseClpCursor {
 public:
  explicit BaseClpCursor(
      clp_s::InputSource inputSource,
      std::string_view splitPath)
      : errorCode_(ErrorCode::QueryNotInitialized),
        inputSource_(inputSource),
        splitPath_(std::string(splitPath)),
        splitType_(ClpConnectorSplit::SplitType::kArchive) {}
  virtual ~BaseClpCursor() = default;

  /// Executes a query. This function parses, validates, and prepares the given
  /// query for execution.
  ///
  /// @param query The KQL query to execute.
  /// @param outputColumns A vector specifying the columns to be included in the
  /// query result.
  void executeQuery(
      const std::string& query,
      const std::vector<Field>& outputColumns);

  /// Fetches the next set of rows from the cursor. If the split and schema
  /// are not yet loaded, this function will perform the necessary loading.
  ///
  /// @param numRows The maximum number of rows to fetch.
  /// @param filteredRowIndices A vector of row indices that match the filter.
  /// @return The number of rows scanned.
  virtual uint64_t fetchNext(
      uint64_t numRows,
      const std::shared_ptr<std::vector<uint64_t>>& filteredRowIndices) = 0;

  /// Retrieves the projected columns.
  ///
  /// @return A vector of BaseColumnReader pointers representing the projected
  /// columns.
  virtual const std::vector<clp_s::BaseColumnReader*>& getProjectedColumns()
      const = 0;

  /// Get the type of the split that the cursor is processing.
  ///
  /// @return The split type.
  ClpConnectorSplit::SplitType getSplitType() const {
    return splitType_;
  }

 protected:
  ///
  /// @return The error code.
  virtual ErrorCode loadSplit() = 0;

  ErrorCode errorCode_;

  clp_s::InputSource inputSource_{clp_s::InputSource::Filesystem};
  std::string splitPath_;
  ClpConnectorSplit::SplitType splitType_;
  std::string query_;
  std::vector<Field> outputColumns_;

  bool currentSplitLoaded_{false};

  std::shared_ptr<clp_s::search::ast::Expression> expr_;

 private:
  /// Preprocesses the query, performing parsing, validation, and optimization.
  ///
  /// @return The error code.
  ErrorCode preprocessQuery();
};

} // namespace facebook::velox::connector::clp::search_lib
