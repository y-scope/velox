#include "velox/connectors/clp/search_lib/QueryRunner.h"

#include "clp/type_utils.hpp"
#include "clp_s/Utils.hpp"
#include "clp_s/search/AndExpr.hpp"
#include "clp_s/search/FilterExpr.hpp"
#include "clp_s/search/Literal.hpp"
#include "clp_s/search/OrExpr.hpp"
#include "clp_s/search/SearchUtils.hpp"
#include "clp_s/search/clp_search/EncodedVariableInterpreter.hpp"
#include "clp_s/search/clp_search/Grep.hpp"

#include "velox/vector/ComplexVector.h"

#define eval(op, a, b) \
  (((op) == FilterOperation::EQ) ? ((a) == (b)) : ((a) != (b)))

using namespace clp_s;
using namespace clp_s::search;
using namespace clp_s::search::clp_search;
using namespace simdjson;

namespace facebook::velox::connector::clp::search_lib {
void QueryRunner::init(
    clp_s::SchemaReader* schema_reader,
    int32_t schema_id,
    std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& column_map) {
  // Extract below to a function
  // ----------------------------------------------------------------
  m_schema_reader = schema_reader;
  m_num_messages = schema_reader->get_num_messages();
  m_clp_string_readers.clear();
  m_var_string_readers.clear();
  m_datestring_readers.clear();
  m_basic_readers.clear();
  // ----------------------------------------------------------------

  auto matching_nodes_list = m_projection->get_matching_nodes_list();
  for (const auto& node_ids : matching_nodes_list) {
    if (node_ids.empty()) {
      m_projected_columns.push_back(nullptr);
      m_projected_types.push_back(NodeType::Unknown);
      continue;
    }

    // Try to find a matching column in m_column_map
    bool found_reader = false;
    for (const auto node_id : node_ids) {
      auto column_it = column_map.find(node_id);
      if (column_it != column_map.end()) {
        m_projected_columns.push_back(column_it->second);
        m_projected_types.push_back(
            m_schema_tree->get_node(node_id).get_type());
        found_reader = true;
        break;
      }
    }

    if (!found_reader) {
      m_projected_columns.push_back(nullptr);
      m_projected_types.push_back(NodeType::Unknown);
    }
  }

  for (auto& [column_id, column_reader] : column_map) {
    // extract below to a function
    // ----------------------------------------------------------------
    if ((0 !=
         (m_wildcard_type_mask &
          node_to_literal_type(
              m_schema_tree->get_node(column_id).get_type()))) ||
        m_match->schema_searches_against_column(m_schema, column_id)) {
      ClpStringColumnReader* clp_reader =
          dynamic_cast<ClpStringColumnReader*>(column_reader);
      VariableStringColumnReader* var_reader =
          dynamic_cast<VariableStringColumnReader*>(column_reader);
      DateStringColumnReader* date_reader =
          dynamic_cast<DateStringColumnReader*>(column_reader);
      if (nullptr != clp_reader &&
          clp_reader->get_type() == NodeType::ClpString) {
        m_clp_string_readers[column_id].push_back(clp_reader);
      } else if (
          nullptr != var_reader &&
          var_reader->get_type() == NodeType::VarString) {
        m_var_string_readers[column_id].push_back(var_reader);
      } else if (nullptr != date_reader) {
        // Datestring readers with a given column ID are guaranteed not to
        // repeat
        m_datestring_readers.emplace(column_id, date_reader);
      } else {
        m_basic_readers[column_id].push_back(column_reader);
      }
    }
    // ----------------------------------------------------------------
  }
}

size_t QueryRunner::fetch_next(
    size_t num_rows,
    std::vector<facebook::velox::VectorPtr>& column_vectors) {
  size_t num_rows_fetched = 0;
  while (m_cur_message < m_num_messages) {
    m_extracted_unstructured_arrays.clear();
    if (evaluate(m_expr.get(), m_schema)) {
      get_message(m_cur_message, num_rows_fetched, column_vectors);
      num_rows_fetched += 1;
      if (num_rows_fetched >= column_vectors[0]->size()) {
        int a = 1;
      }
    }
    m_cur_message += 1;
    if (num_rows_fetched >= num_rows) {
      break;
    }
  }
  return num_rows_fetched;
}

size_t QueryRunner::fetch_next(
    size_t num_rows,
    std::vector<ColumnData>& column_vectors) {
  size_t num_rows_fetched = 0;
  while (m_cur_message < m_num_messages) {
    m_extracted_unstructured_arrays.clear();
    if (evaluate(m_expr.get(), m_schema)) {
      get_message(m_cur_message, num_rows_fetched, column_vectors);
      num_rows_fetched += 1;
    }
    m_cur_message += 1;
    if (num_rows_fetched >= num_rows) {
      break;
    }
  }
  return num_rows_fetched;
}

void QueryRunner::get_message(
    uint64_t message_index,
    uint64_t vector_index,
    std::vector<facebook::velox::VectorPtr>& column_vectors) {
  for (size_t i = 0; i < column_vectors.size(); ++i) {
    switch (m_projected_types[i]) {
      case NodeType::Integer: {
        auto vector = column_vectors[i]->asFlatVector<int64_t>();
        vector->set(
            vector_index,
            std::get<int64_t>(
                m_projected_columns[i]->extract_value(message_index)));
        vector->setNull(vector_index, false);
        break;
      }
      case NodeType::Float: {
        auto float_vector = column_vectors[i]->asFlatVector<double>();
        float_vector->set(
            vector_index,
            std::get<double>(
                m_projected_columns[i]->extract_value(message_index)));
        float_vector->setNull(vector_index, false);
        break;
      }
      case NodeType::Boolean: {
        auto bool_vector = column_vectors[i]->asFlatVector<bool>();
        bool_vector->set(
            vector_index,
            std::get<uint8_t>(
                m_projected_columns[i]->extract_value(message_index)) != 0);
        bool_vector->setNull(vector_index, false);
        break;
      }
      case NodeType::ClpString:
      case NodeType::VarString: {
        auto string_vector =
            column_vectors[i]->asFlatVector<facebook::velox::StringView>();
        auto string_value = std::get<std::string>(
            m_projected_columns[i]->extract_value(message_index));
        string_vector->set(
            vector_index, facebook::velox::StringView(string_value));
        string_vector->setNull(vector_index, false);
        break;
      }
      case NodeType::UnstructuredArray: {
        auto array_vector =
            std::dynamic_pointer_cast<facebook::velox::ArrayVector>(
                column_vectors[i]);
        if (vector_index == 0) {
          m_array_offsets[i] = 0;
        }

        auto array_begin_offset = m_array_offsets[i];
        auto array_end_offset = array_begin_offset;
        auto elements = array_vector->elements()
                            ->asFlatVector<facebook::velox::StringView>();
        std::vector<std::string_view> arrayElements;
        auto json_string = std::get<std::string>(
            m_projected_columns[i]->extract_value(message_index));
        auto obj = m_array_parser.iterate(json_string);

        for (auto arrayElement : obj.get_array()) {
          // Get each array element as a string
          auto elementStringWithQuotes =
              simdjson::to_json_string(arrayElement).value();
          auto elementString = elementStringWithQuotes.substr(
              1, elementStringWithQuotes.size() - 2);
          arrayElements.emplace_back(elementString);
        }
        elements->resize(array_end_offset + arrayElements.size());

        for (auto& arrayElement : arrayElements) {
          // Set the element in the array vector
          elements->set(
              array_end_offset++, facebook::velox::StringView(arrayElement));
        }
        m_array_offsets[i] = array_end_offset;
        array_vector->setOffsetAndSize(
            vector_index,
            array_begin_offset,
            array_end_offset - array_begin_offset);
        array_vector->setNull(vector_index, false);
        break;
      }
      case NodeType::NullValue:
      case NodeType::Unknown:
      default:
        column_vectors[i]->setNull(vector_index, true);
        break;
    }
  }
}

void QueryRunner::get_message(
    uint64_t message_index,
    uint64_t vector_index,
    std::vector<ColumnData>& column_vectors) {
  for (size_t i = 0; i < column_vectors.size(); ++i) {
    switch (m_projected_types[i]) {
      case NodeType::Integer: {
        auto& vector = std::get<std::vector<int64_t>>(column_vectors[i]);
        vector[vector_index] = std::get<int64_t>(
            m_projected_columns[i]->extract_value(message_index));
        break;
      }
      case NodeType::Float: {
        auto& vector = std::get<std::vector<double>>(column_vectors[i]);
        vector[vector_index] = std::get<double>(
            m_projected_columns[i]->extract_value(message_index));
        break;
      }
      case NodeType::Boolean: {
        auto& vector = std::get<std::vector<bool>>(column_vectors[i]);
        vector[vector_index] =
            std::get<uint8_t>(
                m_projected_columns[i]->extract_value(message_index)) != 0;
        break;
      }
      case NodeType::ClpString:
      case NodeType::VarString:
      case NodeType::UnstructuredArray: {
        auto& vector = std::get<std::vector<std::string>>(column_vectors[i]);
        vector[vector_index] = std::get<std::string>(
            m_projected_columns[i]->extract_value(message_index));
        break;
      }
      default:
        break;
    }
  }
}

} // namespace facebook::velox::connector::clp::search_lib
