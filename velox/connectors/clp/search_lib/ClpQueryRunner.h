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
   * Initializes the filter with a map
   * @param schema_reader
   * @param column_map
   */
  void init(
      clp_s::SchemaReader* schema_reader,
      std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& column_map)
      override;

  /**
   * Fetches the next set of rows from the cursor.
   * @param num_rows The number of rows to fetch.
   * @param filteredRows A vector to store the row indices that match the
   * filter.
   * @return A vector of row indices that match the filter.
   */
  void fetchNext(size_t num_rows, std::vector<size_t>& filteredRows);

 private:
  std::shared_ptr<clp_s::search::ast::Expression> expr_;
  std::shared_ptr<clp_s::SchemaTree> schemaTree_;
  std::shared_ptr<clp_s::search::Projection> projection_;
  std::vector<clp_s::BaseColumnReader*> projectedColumns_;
  std::vector<clp_s::NodeType> projectedTypes_;

  uint64_t curMessage_{};
  uint64_t numMessages_{};
};
} // namespace facebook::velox::connector::clp::search_lib
