#pragma once

#include <string>
#include <vector>

#include "clp_s/ArchiveReader.hpp"
#include "clp_s/search/SchemaMatch.hpp"
#include "clp_s/search/ast/Expression.hpp"

#include "velox/connectors/clp/search_lib/ClpQueryRunner.h"

namespace facebook::velox::connector::clp::search_lib {

enum class ErrorCode {
  Success,
  QueryNotInitialized,
  InvalidQuerySyntax,
  SchemaNotFound,
  LogicalError,
  DictionaryNotFound,
  InvalidTimestampRange,
  InternalError
};

enum class ColumnType { String, Integer, Float, Array, Boolean, Unknown };

struct Field {
  ColumnType type;
  std::string name;
};

class ClpCursor {
 public:
  // Constructors
  explicit ClpCursor(clp_s::InputSource inputSource, std::string& archivePath);

  /**
   * Executes a query. This functions tries to find the first schema table with
   * potential matches
   * @param query The query to execute.
   * @param outputColumns The columns to output.
   * @return The error code.
   */
  ErrorCode executeQuery(
      const std::string& query,
      const std::vector<Field>& outputColumns);

  /**
   * Fetches the next set of rows from the cursor.
   * @param numRows The number of rows to fetch.
   * @return A vector of row indices that match the filter.
   */
  ErrorCode ClpCursor::fetch_next(size_t numRows, std::vector<size_t>);

 private:
  /**
   * Preprocesses the query to.
   */
  ErrorCode preprocessQuery();

  /**
   * Loads the archive at the current index.
   * @param outputColumns The columns to output.
   * @return The error code.
   */
  ErrorCode loadArchive(const std::vector<Field>& outputColumns);

  ErrorCode errorCode_;

  clp_s::InputSource inputSource_{clp_s::InputSource::Filesystem};
  std::string archivePath_;
  std::string query_;
  std::vector<Field> outputColumns_;
  std::vector<int32_t> matchedSchemas_;
  size_t currentSchemaIndex_{0};
  int32_t currentSchemaId_{-1};
  bool currentSchemaTableLoaded_{false};
  bool currentArchiveLoaded_{false};

  std::shared_ptr<clp_s::search::ast::Expression> expr_;
  std::shared_ptr<clp_s::search::SchemaMatch> schemaMatch_;
  std::shared_ptr<ClpQueryRunner> queryRunner_;
  std::shared_ptr<clp_s::search::Projection> projection_;
  std::shared_ptr<clp_s::ArchiveReader> archiveReader_;
};
} // namespace facebook::velox::connector::clp::search_lib
