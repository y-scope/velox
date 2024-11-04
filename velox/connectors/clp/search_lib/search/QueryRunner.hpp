#ifndef CLP_S_SEARCH_QUERYRUNNER_HPP
#define CLP_S_SEARCH_QUERYRUNNER_HPP

#include <stack>

#include <simdjson.h>

#include "../SchemaReader.hpp"
#include "../Utils.hpp"
#include "Expression.hpp"
#include "SchemaMatch.hpp"
#include "StringLiteral.hpp"
#include "clp_search/Query.hpp"

using namespace clp_s::search::clp_search;
using namespace simdjson;

using ColumnData = std::variant<
    std::vector<int64_t>,
    std::vector<bool>,
    std::vector<std::string>,
    std::vector<double>>;

namespace clp_s::search {
class QueryRunner {
 public:
  QueryRunner(
      std::shared_ptr<Expression> expr,
      std::shared_ptr<SchemaMatch> match,
      bool ignore_case,
      std::shared_ptr<ReaderUtils::SchemaMap> schemas,
      std::shared_ptr<SchemaTree> m_schema_tree,
      std::shared_ptr<VariableDictionaryReader> m_var_dict,
      std::shared_ptr<LogTypeDictionaryReader> m_log_dict)
      : m_expr(expr),
        m_match(match),
        m_ignore_case(ignore_case),
        m_schemas(schemas),
        m_schema_tree(m_schema_tree),
        m_var_dict(m_var_dict),
        m_log_dict(m_log_dict),
        m_schema(-1) {}

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
   * Initializes the filter
   * @param schema_reader
   * @param column_readers
   */
  void initialize_filter(
      SchemaReader* schema_reader,
      std::vector<BaseColumnReader*> const& column_readers);

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
  EvaluatedValue constant_propagate() {
    m_expression_value = constant_propagate(m_expr);
    return m_expression_value;
  }

  /**
   * Adds wildcard columns to searched columns
   */
  void add_wildcard_columns_to_searched_columns();

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

  std::shared_ptr<Expression> m_expr;
  std::shared_ptr<SchemaMatch> m_match;

  bool m_ignore_case;

  // variables for the current schema being filtered
  int32_t m_schema;
  SchemaReader* m_schema_reader;

  std::shared_ptr<SchemaTree> m_schema_tree;
  std::shared_ptr<VariableDictionaryReader> m_var_dict;
  std::shared_ptr<LogTypeDictionaryReader> m_log_dict;

  std::shared_ptr<ReaderUtils::SchemaMap> m_schemas;

  std::map<std::string, std::optional<Query>> m_string_query_map;
  std::map<std::string, std::unordered_set<int64_t>> m_string_var_match_map;
  std::unordered_map<Expression*, Query*> m_expr_clp_query;
  std::unordered_map<Expression*, std::unordered_set<int64_t>*>
      m_expr_var_match_map;
  std::unordered_map<int32_t, std::vector<ClpStringColumnReader*>>
      m_clp_string_readers;
  std::unordered_map<int32_t, std::vector<VariableStringColumnReader*>>
      m_var_string_readers;
  std::unordered_map<int32_t, DateStringColumnReader*> m_datestring_readers;
  std::unordered_map<int32_t, std::vector<BaseColumnReader*>> m_basic_readers;
  std::unordered_map<int32_t, std::string> m_extracted_unstructured_arrays;
  uint64_t m_cur_message;
  uint64_t m_num_messages;
  EvaluatedValue m_expression_value;

  std::vector<ColumnDescriptor*> m_wildcard_columns;
  std::map<ColumnDescriptor*, std::set<int32_t>>
      m_wildcard_to_searched_basic_columns;
  LiteralTypeBitmask m_wildcard_type_mask{0};

  std::stack<
      std::pair<ExpressionType, OpList::iterator>,
      std::vector<std::pair<ExpressionType, OpList::iterator>>>
      m_expression_state;

  simdjson::ondemand::parser m_array_parser;
  std::string m_array_search_string;
  bool m_maybe_string, m_maybe_number;

  /**
   * Evaluates an expression
   * @param expr
   * @param schema
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate(Expression* expr, int32_t schema);

  /**
   * Evaluates a filter expression
   * @param expr
   * @param schema
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_filter(FilterExpr* expr, int32_t schema);

  /**
   * Evaluates a wildcard filter expression
   * @param expr
   * @param schema
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_wildcard_filter(FilterExpr* expr, int32_t schema);

  /**
   * Evaluates a int filter expression
   * @param op
   * @param column_id
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_int_filter(
      FilterOperation op,
      int32_t column_id,
      std::shared_ptr<Literal> const& operand);

  /**
   * Evaluates a int filter expression
   * @param op
   * @param value
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  static bool
  evaluate_int_filter_core(FilterOperation op, int64_t value, int64_t operand);

  /**
   * Evaluates a float filter expression
   * @param op
   * @param column_id
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_float_filter(
      FilterOperation op,
      int32_t column_id,
      std::shared_ptr<Literal> const& operand);

  /**
   * Evaluates the core of a float filter expression
   * @param op
   * @param value
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  static bool
  evaluate_float_filter_core(FilterOperation op, double value, double operand);

  /**
   * Evaluates a clp string filter expression
   * @param op
   * @param q
   * @param readers
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_clp_string_filter(
      FilterOperation op,
      Query* q,
      std::vector<ClpStringColumnReader*> const& readers) const;

  /**
   * Evaluates a var string filter expression
   * @param op
   * @param reader
   * @param matching_vars
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_var_string_filter(
      FilterOperation op,
      std::vector<VariableStringColumnReader*> const& readers,
      std::unordered_set<int64_t>* matching_vars) const;

  /**
   * Evaluates a epoch date string filter expression
   * @param op
   * @param reader
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_epoch_date_filter(
      FilterOperation op,
      DateStringColumnReader* reader,
      std::shared_ptr<Literal>& operand);

  /**
   * Evaluates an array filter expression
   * @param op
   * @param unresolved_tokens
   * @param value
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_array_filter(
      FilterOperation op,
      DescriptorList const& unresolved_tokens,
      std::string& value,
      std::shared_ptr<Literal> const& operand);

  /**
   * Evaluates a filter expression on a single value for precise array search.
   * @param item
   * @param op
   * @param unresolved_tokens
   * @param cur_idx
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  inline bool evaluate_array_filter_value(
      ondemand::value& item,
      FilterOperation op,
      DescriptorList const& unresolved_tokens,
      size_t cur_idx,
      std::shared_ptr<Literal> const& operand) const;

  /**
   * Evaluates a filter expression on an array (top level or nested) for precise
   * array search.
   * @param array
   * @param op
   * @param unresolved_tokens
   * @param cur_idx
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_array_filter_array(
      ondemand::array& array,
      FilterOperation op,
      DescriptorList const& unresolved_tokens,
      size_t cur_idx,
      std::shared_ptr<Literal> const& operand) const;

  /**
   * Evaluates a filter expression on an object inside of an array for precise
   * array search.
   * @param object
   * @param op
   * @param unresolved_tokens
   * @param cur_idx
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_array_filter_object(
      ondemand::object& object,
      FilterOperation op,
      DescriptorList const& unresolved_tokens,
      size_t cur_idx,
      std::shared_ptr<Literal> const& operand) const;

  /**
   * Evaluates a wildcard array filter expression
   * @param op
   * @param value
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_wildcard_array_filter(
      FilterOperation op,
      std::string& value,
      std::shared_ptr<Literal> const& operand);

  /**
   * The implementation of evaluate_wildcard_array_filter
   * @param array
   * @param op
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_wildcard_array_filter(
      ondemand::array& array,
      FilterOperation op,
      std::shared_ptr<Literal> const& operand) const;

  /**
   * The implementation of evaluate_wildcard_array_filter
   * @param object
   * @param op
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_wildcard_array_filter(
      ondemand::object& object,
      FilterOperation op,
      std::shared_ptr<Literal> const& operand) const;

  /**
   * Evaluates a bool filter expression
   * @param op
   * @param column_id
   * @param operand
   * @return true if the expression evaluates to true, false otherwise
   */
  bool evaluate_bool_filter(
      FilterOperation op,
      int32_t column_id,
      std::shared_ptr<Literal> const& operand);

  /**
   * Populates the string queries
   * @param expr
   */
  void populate_string_queries(std::shared_ptr<Expression> const& expr);

  /**
   * Constant propagates an expression
   * @param expr
   * @param schema_id
   * @return EvaluatedValue::True if the expression evaluates to true,
   * EvaluatedValue::False if the expression evaluates to false,
   * EvaluatedValue::Unknown otherwise
   */
  EvaluatedValue constant_propagate(std::shared_ptr<Expression> const& expr);

  /**
   * Populates searched wildcard columns
   * @param expr
   */
  void populate_searched_wildcard_columns(
      std::shared_ptr<Expression> const& expr);

  /**
   * Gets the cached decompressed structured array for the current message
   * stored in the column column_id. Decompressing array fields can be
   * expensive, so this interface allows us to decompress lazily, and decompress
   * the field only once.
   *
   * Note: the string is returned by reference to allow our array search code to
   * adjust the string so that we have enough padding for simdjson.
   * @param column_id
   * @return the string representing the unstructured array stored in the column
   * column_id
   */
  std::string& get_cached_decompressed_unstructured_array(int32_t column_id);
};
} // namespace clp_s::search
#endif // CLP_S_SEARCH_QUERYRUNNER_HPP
