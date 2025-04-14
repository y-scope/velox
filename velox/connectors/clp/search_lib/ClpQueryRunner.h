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

#include <stack>

#include <simdjson.h>

#include "clp_s/SchemaReader.hpp"
#include "clp_s/search/Projection.hpp"
#include "clp_s/search/QueryRunner.hpp"
#include "clp_s/search/SchemaMatch.hpp"
#include "clp_s/search/ast/Expression.hpp"

#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::clp::search_lib {
class ClpQueryRunner : public clp_s::search::QueryRunner {
 public:
  ClpQueryRunner(
      std::shared_ptr<clp_s::search::SchemaMatch> match,
      std::shared_ptr<clp_s::search::ast::Expression> expr,
      std::shared_ptr<clp_s::ArchiveReader> archive_reader,
      bool ignore_case,
      std::shared_ptr<clp_s::search::Projection> projection)
      : clp_s::search::QueryRunner(*match, expr, archive_reader, ignore_case),
        projection_(projection) {}

  /**
   * Initializes the filter with schema information and column readers.
   * @param schemaReader A pointer to the SchemaReader
   * @param columnMap An unordered map associating column IDs with
   * BaseColumnReader pointers.
   */
  void init(
      clp_s::SchemaReader* schemaReader,
      std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& columnMap)
      override;

  /**
   * Fetches the next set of rows from the cursor.
   * @param numRows The maximum number of rows to fetch.
   * @param filteredRows A vector to store the row indices that match the
   * filter.
   */
  void fetchNext(size_t numRows, std::vector<size_t>& filteredRows);

  /**
   * @return A reference to the vector of BaseColumnReader pointers that
   * represent the columns involved in the scanning operation.
   */
  std::vector<clp_s::BaseColumnReader*>& getProjectedColumns() {
    return projectedColumns_;
  }

 private:
  std::shared_ptr<clp_s::search::ast::Expression> expr_;
  std::shared_ptr<clp_s::SchemaTree> schemaTree_;
  std::shared_ptr<clp_s::search::Projection> projection_;
  std::vector<clp_s::BaseColumnReader*> projectedColumns_;

  uint64_t curMessage_{};
  uint64_t numMessages_{};
};
} // namespace facebook::velox::connector::clp::search_lib
