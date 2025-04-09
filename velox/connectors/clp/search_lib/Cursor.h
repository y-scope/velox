#pragma once

#include <string>
#include <vector>

#include "clp_s/ArchiveReader.hpp"
#include "clp_s/SchemaTree.hpp"
#include "clp_s/TraceableException.hpp"
#include "clp_s/search/Expression.hpp"
#include "clp_s/search/Projection.hpp"
#include "clp_s/search/SchemaMatch.hpp"

#include "velox/connectors/clp/search_lib/QueryRunner.h"

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

enum class ArchiveReadStage {
  None,
  Opened,
  DictionariesRead,
  TablesInitialized,
};

class Cursor {
 public:
  // Types
  class OperationFailed : public clp_s::TraceableException {
   public:
    // Constructors
    OperationFailed(
        clp_s::ErrorCode error_code,
        char const* const filename,
        int line_number)
        : TraceableException(error_code, filename, line_number) {}
  };

  // Constructors
  explicit Cursor(
      clp_s::InputSource input_source,
      std::vector<std::string> archive_paths,
      bool m_ignore_case);

  /**
   * Executes a query. This functions tries to find the first schema table with
   * potential matches
   * @param query The query to execute.
   * @param output_columns The columns to output.
   * @return The error code.
   */
  ErrorCode execute_query(
      std::string& query,
      std::vector<Field>& output_columns);

  /**
   * Fetches the next set of rows from the cursor.
   * @param num_rows The number of rows to fetch.
   * @param column_vectors The column vectors to fill.
   * @return The number of rows fetched.
   */
  size_t fetch_next(
      size_t num_rows,
      std::vector<facebook::velox::VectorPtr>& column_vectors);

  size_t fetch_next(size_t num_rows, std::vector<ColumnData>& column_vectors);

 private:
  /**
   * Moves to the next archive in the list of archives.
   */
  void move_to_next_archive();

  /**
   * Preprocesses the query to.
   */
  ErrorCode preprocess_query();

  /**
   * Loads the archive at the current index.
   */
  ErrorCode load_archive();

  ErrorCode m_error_code;
  bool m_ignore_case;

  clp_s::InputSource m_input_source{clp_s::InputSource::Filesystem};
  std::vector<std::string> m_archive_paths;
  size_t m_current_archive_index{};
  size_t m_end_archive_index{};
  bool m_completed_archive_cycles{};
  std::vector<int32_t> m_matched_schemas;
  size_t m_current_schema_index{};
  size_t m_end_schema_index{};
  bool m_completed_schema_cycles{};
  int32_t m_current_schema_id{};

  ArchiveReadStage m_archive_read_stage{ArchiveReadStage::None};

  std::shared_ptr<clp_s::search::Expression> m_expr;
  std::shared_ptr<clp_s::search::Projection> m_projection;
  std::string m_query;
  std::vector<Field> m_output_columns;

  bool m_current_schema_table_loaded{};

  std::shared_ptr<clp_s::search::SchemaMatch> m_schema_match;

  std::shared_ptr<QueryRunner> m_query_runner;

  clp_s::EvaluatedValue m_expression_value{clp_s::EvaluatedValue::Unknown};

  clp_s::ArchiveReader m_archive_reader;
  clp_s::SchemaReader* m_schema_reader{};
  std::shared_ptr<clp_s::SchemaTree> m_schema_tree;
  std::shared_ptr<clp_s::ReaderUtils::SchemaMap> m_schema_map;
  std::shared_ptr<clp_s::VariableDictionaryReader> m_var_dict;
  std::shared_ptr<clp_s::LogTypeDictionaryReader> m_log_dict;
  std::shared_ptr<clp_s::LogTypeDictionaryReader> m_array_dict;
  std::shared_ptr<clp_s::TimestampDictionaryReader> m_timestamp_dict;
};
} // namespace facebook::velox::connector::clp::search_lib
