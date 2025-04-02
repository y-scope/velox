#pragma once

#include <stack>

#include <simdjson.h>

#include "clp_s/SchemaReader.hpp"
#include "clp_s/search/Expression.hpp"
#include "clp_s/search/Output.hpp"
#include "clp_s/search/Projection.hpp"
#include "clp_s/search/SchemaMatch.hpp"
#include "clp_s/search/clp_search/Query.hpp"

#include "velox/vector/FlatVector.h"

using ColumnData = std::variant<
    std::vector<int64_t>,
    std::vector<bool>,
    std::vector<std::string>,
    std::vector<double>>;

namespace facebook::velox::connector::clp::search_lib {
class QueryRunner : public clp_s::search::Output {
 public:
  QueryRunner(
  std::shared_ptr<clp_s::search::SchemaMatch> match,
      std::shared_ptr<clp_s::search::Expression> expr,
      std::shared_ptr<clp_s::ArchiveReader> archive_reader,
      bool ignore_case,
      std::shared_ptr<clp_s::ReaderUtils::SchemaMap> schemas,
      std::shared_ptr<clp_s::SchemaTree> m_schema_tree,
      std::shared_ptr<clp_s::search::Projection> projection,
      std::shared_ptr<clp_s::VariableDictionaryReader> m_var_dict,
      std::shared_ptr<clp_s::LogTypeDictionaryReader> m_log_dict)
      : clp_s::search::Output(match, expr, archive_reader, nullptr, ignore_case),
        m_projection(projection),
        m_var_dict(m_var_dict),
        m_log_dict(m_log_dict) {}

  /**
   * Set the schema to filter
   * @param schema
   */
  void set_schema(int32_t schema) {
    m_schema = schema;
    m_cur_message = 0;

    m_expr_clp_query.clear();
    m_expr_var_match_map.clear();
    m_expr = m_match->get_query_for_schema(m_schema)->copy();
    m_wildcard_to_searched_basic_columns.clear();
    m_wildcard_columns.clear();
  }

  /**
   * Initializes the filter with a map
   * @param schema_reader
   * @param schema_id
   * @param column_map
   */
  void init(
      clp_s::SchemaReader* schema_reader,
      int32_t schema_id,
      std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& column_map)
      override;

  /**
   * Filter the message
   * @param cur_message
   * @return
   */
  bool filter(uint64_t cur_message) override {
    return false;
  }

  /**
   * Populates the string queries
   */
  void populate_string_queries() {
    populate_string_queries(m_expr);
  }

  /**
   * Populates searched wildcard columns
   */
  void populate_searched_wildcard_columns() {
    populate_searched_wildcard_columns(m_expr);
  }

  /**
   * Constant propagates an expression
   * @param expr
   * @param schema_id
   * @return EvaluatedValue::True if the expression evaluates to true,
   * EvaluatedValue::False if the expression evaluates to false,
   * EvaluatedValue::Unknown otherwise
   */
  clp_s::EvaluatedValue constant_propagate() {
      m_expression_value = constant_propagate(m_expr);
      return m_expression_value;
  }

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
  enum class ExpressionType { And, Or, Filter };

  std::shared_ptr<clp_s::search::Expression> m_expr;
  std::shared_ptr<clp_s::search::SchemaMatch> m_match;

  bool m_ignore_case;

  // variables for the current schema being filtered
  int32_t m_schema;
  std::shared_ptr<clp_s::ReaderUtils::SchemaMap> m_schemas;
  std::shared_ptr<clp_s::SchemaTree> m_schema_tree;
  clp_s::SchemaReader* m_schema_reader;

  std::shared_ptr<clp_s::search::Projection> m_projection;
  std::vector<clp_s::BaseColumnReader*> m_projected_columns;
  std::vector<clp_s::NodeType> m_projected_types;

  std::shared_ptr<clp_s::VariableDictionaryReader> m_var_dict;
  std::shared_ptr<clp_s::LogTypeDictionaryReader> m_log_dict;

  std::map<std::string, std::optional<clp_s::search::clp_search::Query>>
      m_string_query_map;
  std::map<std::string, std::unordered_set<int64_t>> m_string_var_match_map;
  std::unordered_map<
      clp_s::search::Expression*,
      clp_s::search::clp_search::Query*>
      m_expr_clp_query;
  std::unordered_map<clp_s::search::Expression*, std::unordered_set<int64_t>*>
      m_expr_var_match_map;
  std::unordered_map<int32_t, std::vector<clp_s::ClpStringColumnReader*>>
      m_clp_string_readers;
  std::unordered_map<int32_t, std::vector<clp_s::VariableStringColumnReader*>>
      m_var_string_readers;
  std::unordered_map<int32_t, clp_s::DateStringColumnReader*>
      m_datestring_readers;
  std::unordered_map<int32_t, std::vector<clp_s::BaseColumnReader*>>
      m_basic_readers;
  std::unordered_map<int32_t, std::string> m_extracted_unstructured_arrays;
  uint64_t m_cur_message;
  uint64_t m_num_messages;
  clp_s::EvaluatedValue m_expression_value;

  std::vector<clp_s::search::ColumnDescriptor*> m_wildcard_columns;
  std::map<clp_s::search::ColumnDescriptor*, std::set<int32_t>>
      m_wildcard_to_searched_basic_columns;
  clp_s::search::LiteralTypeBitmask m_wildcard_type_mask{0};

  std::stack<
      std::pair<ExpressionType, clp_s::search::OpList::iterator>,
      std::vector<std::pair<ExpressionType, clp_s::search::OpList::iterator>>>
      m_expression_state;

  simdjson::ondemand::parser m_array_parser;
  std::string m_array_search_string;
  bool m_maybe_string, m_maybe_number;

  std::unordered_map<size_t, size_t> m_array_offsets;

  /**
   * Gets a message and populate it to the vector
   * @param message_index
   * @param vector_index
   * @param column_vectors
   */
  void get_message(
      uint64_t message_index,
      uint64_t vector_index,
      std::vector<facebook::velox::VectorPtr>& column_vectors);

  /**
   * Gets a message and populate it to the vector
   * @param message_index
   * @param vector_index
   * @param column_vectors
   */
  void get_message(
      uint64_t message_index,
      uint64_t vector_index,
      std::vector<ColumnData>& column_vectors);
};
} // namespace facebook::velox::connector::clp::search_lib
