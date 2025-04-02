#include "velox/connectors/clp/search_lib/Cursor.h"

#include <filesystem>
#include <stdexcept>

#include <spdlog/spdlog.h>

#include "clp_s/search/ConvertToExists.hpp"
#include "clp_s/search/EmptyExpr.hpp"
#include "clp_s/search/EvaluateTimestampIndex.hpp"
#include "clp_s/search/NarrowTypes.hpp"
#include "clp_s/search/OrOfAndForm.hpp"
#include "clp_s/search/SearchUtils.hpp"
#include "clp_s/search/kql/kql.hpp"

using namespace clp_s;
using namespace clp_s::search;

namespace facebook::velox::connector::clp::search_lib {
Cursor::Cursor(
    InputSource input_source,
    std::vector<std::string> archive_paths,
    bool ignore_case)
    : m_error_code(ErrorCode::QueryNotInitialized),
      m_ignore_case(ignore_case),
      m_input_source(input_source),
      m_archive_paths(std::move(archive_paths)),
      m_current_schema_id(-1) {
}

void Cursor::move_to_next_archive() {
  m_archive_reader.close();
  m_current_archive_index =
      (m_current_archive_index + 1) % m_archive_paths.size();
  m_completed_archive_cycles = m_current_archive_index == m_end_archive_index;
  m_archive_read_stage = ArchiveReadStage::None;
  m_current_schema_id = -1;
  m_current_schema_table_loaded = false;
}

ErrorCode Cursor::load_archive() {
  if (m_archive_read_stage < ArchiveReadStage::Opened) {
    auto archive_path = m_archive_paths[m_current_archive_index];
    auto networkAuthOption = m_input_source == InputSource::Filesystem
        ? NetworkAuthOption{.method = AuthMethod::None}
        : NetworkAuthOption{.method = AuthMethod::S3PresignedUrlV4};

    m_archive_reader.open(
        clp_s::get_path_object_for_raw_path(archive_path), networkAuthOption);
    m_timestamp_dict = m_archive_reader.get_timestamp_dictionary();
    m_archive_reader.read_metadata();
    m_schema_tree = m_archive_reader.get_schema_tree();
    m_schema_map = m_archive_reader.get_schema_map();
    m_archive_read_stage = ArchiveReadStage::Opened;
  }

  EvaluateTimestampIndex timestamp_index(m_timestamp_dict);
  if (clp_s::EvaluatedValue::False == timestamp_index.run(m_expr)) {
    SPDLOG_INFO("No matching timestamp ranges for query '{}'", m_query);
    return ErrorCode::InvalidTimestampRange;
  }

  // Narrow against schemas
  m_schema_match = std::make_shared<SchemaMatch>(m_schema_tree, m_schema_map);
  if (m_expr = m_schema_match->run(m_expr);
      std::dynamic_pointer_cast<EmptyExpr>(m_expr)) {
    SPDLOG_INFO("No matching schemas for query '{}'", m_query);
    return ErrorCode::SchemaNotFound;
  }

  // Handle projection
  m_projection = std::make_shared<Projection>(
      m_output_columns.empty() ? ReturnAllColumns : ReturnSelectedColumns);
  try {
    for (auto const& column : m_output_columns) {
      std::vector<std::string> descriptor_tokens;
      std::string descriptor_namespace;
      if (false ==
          tokenize_column_descriptor(
              column.name, descriptor_tokens, descriptor_namespace)) {
        SPDLOG_ERROR("Can not tokenize invalid column: \"{}\"", column);
        return ErrorCode::InternalError;
      }

      auto column_descriptor = ColumnDescriptor::create_from_escaped_tokens(
          descriptor_tokens, descriptor_namespace);
      switch (column.type) {
        case ColumnType::String:
          column_descriptor->set_matching_types(
              LiteralType::ClpStringT | LiteralType::VarStringT |
              LiteralType::EpochDateT);
          break;
        case ColumnType::Integer:
          column_descriptor->set_matching_types(LiteralType::IntegerT);
          break;
        case ColumnType::Float:
          column_descriptor->set_matching_types(LiteralType::FloatT);
          break;
        case ColumnType::Boolean:
          column_descriptor->set_matching_types(LiteralType::BooleanT);
          break;
        case ColumnType::Array:
          column_descriptor->set_matching_types(LiteralType::ArrayT);
          break;
        default:
          break;
      }

      m_projection->add_column(column_descriptor);
    }
  } catch (TraceableException& e) {
    SPDLOG_ERROR("{}", e.what());
    return ErrorCode::InternalError;
  }
  m_projection->resolve_columns(m_schema_tree);
  m_archive_reader.set_projection(m_projection);

  m_matched_schemas.clear();
  for (auto schema_id : m_archive_reader.get_schema_ids()) {
    if (m_schema_match->schema_matched(schema_id)) {
      m_matched_schemas.push_back(schema_id);
    }
  }

  if (m_matched_schemas.empty()) {
    return ErrorCode::SchemaNotFound;
  }

  // Read dictionaries and table metadata
  if (m_archive_read_stage < ArchiveReadStage::DictionariesRead) {
    m_var_dict = m_archive_reader.read_variable_dictionary();
    m_log_dict = m_archive_reader.read_log_type_dictionary();
    m_array_dict = m_archive_reader.read_array_dictionary();
    m_archive_read_stage = ArchiveReadStage::DictionariesRead;
  }

  m_current_schema_index = m_end_schema_index = 0;
  m_completed_schema_cycles = false;
  return ErrorCode::Success;
}

ErrorCode Cursor::preprocess_query() {
  auto query_stream = std::istringstream(m_query);
  m_expr = kql::parse_kql_expression(query_stream);
  if (nullptr == m_expr) {
    SPDLOG_ERROR("Failed to parse query '{}'", m_query);
    return ErrorCode::InvalidQuerySyntax;
  }

  if (std::dynamic_pointer_cast<EmptyExpr>(m_expr)) {
    SPDLOG_ERROR("Query '{}' is logically false", m_query);
    return ErrorCode::LogicalError;
  }

  OrOfAndForm standardize_pass;
  if (m_expr = standardize_pass.run(m_expr);
      std::dynamic_pointer_cast<EmptyExpr>(m_expr)) {
    SPDLOG_ERROR("Query '{}' is logically false", m_query);
    return ErrorCode::LogicalError;
  }

  NarrowTypes narrow_pass;
  if (m_expr = narrow_pass.run(m_expr);
      std::dynamic_pointer_cast<EmptyExpr>(m_expr)) {
    SPDLOG_ERROR("Query '{}' is logically false", m_query);
    return ErrorCode::LogicalError;
  }

  ConvertToExists convert_pass;
  if (m_expr = convert_pass.run(m_expr);
      std::dynamic_pointer_cast<EmptyExpr>(m_expr)) {
    SPDLOG_ERROR("Query '{}' is logically false", m_query);
    return ErrorCode::LogicalError;
  }

  return ErrorCode::Success;
}

ErrorCode Cursor::execute_query(
    std::string& query,
    std::vector<Field>& output_columns) {
  m_output_columns = output_columns;
  m_query = query;
  m_completed_archive_cycles = false;

  m_error_code = preprocess_query();
  if (m_error_code != ErrorCode::Success) {
    return m_error_code;
  }

  m_end_archive_index = m_current_archive_index;
  while (false == m_completed_archive_cycles) {
    m_error_code = load_archive();
    if (ErrorCode::InternalError == m_error_code) {
      return m_error_code;
    }

    if (ErrorCode::Success != m_error_code) {
      move_to_next_archive();
      continue;
    }

    m_query_runner = std::make_shared<QueryRunner>(
        m_expr,
        m_schema_match,
        m_archive_reader,
        m_ignore_case,
        m_schema_map,
        m_schema_tree,
        m_projection,
        m_var_dict,
        m_log_dict);

    // clear the stage from last run
    m_query_runner->populate_string_queries();

    // probably have another class for query evaluation and filter
    while (false == m_completed_schema_cycles) {
      m_current_schema_id = m_matched_schemas[m_current_schema_index];

      m_query_runner->set_schema(m_current_schema_id);

      m_query_runner->populate_searched_wildcard_columns();

      m_expression_value = m_query_runner->constant_propagate();

      if (m_expression_value != EvaluatedValue::False) {
        m_query_runner->add_wildcard_columns_to_searched_columns();
        if (m_archive_read_stage < ArchiveReadStage::TablesInitialized) {
          m_archive_reader.open_packed_streams();
          m_archive_read_stage = ArchiveReadStage::TablesInitialized;
        }

        auto& reader = m_archive_reader.read_schema_table(
            m_current_schema_id, false, false);
        reader.initialize_filter_with_column_map(m_query_runner.get());
        m_error_code = ErrorCode::Success;
        m_current_schema_table_loaded = true;
        break;
      }

      m_current_schema_index =
          (m_current_schema_index + 1) % m_matched_schemas.size();
      m_completed_schema_cycles = m_current_schema_index == m_end_schema_index;
      m_error_code = ErrorCode::DictionaryNotFound;
      m_current_schema_table_loaded = false;
    }

    if (m_expression_value != EvaluatedValue::False) {
      break;
    }

    move_to_next_archive();
  }

  return m_error_code;
}

size_t Cursor::fetch_next(
    size_t num_rows,
    std::vector<facebook::velox::VectorPtr>& column_vectors) {
  if (m_error_code != ErrorCode::Success) {
    return 0;
  }
  while (false == m_completed_archive_cycles) {
    while (false == m_completed_schema_cycles) {
      // whether the schema table is loaded
      if (false == m_current_schema_table_loaded) {
        m_current_schema_id = m_matched_schemas[m_current_schema_index];
        m_query_runner->set_schema(m_current_schema_id);
        m_query_runner->populate_searched_wildcard_columns();
        m_expression_value = m_query_runner->constant_propagate();

        if (m_expression_value != EvaluatedValue::False) {
          m_query_runner->add_wildcard_columns_to_searched_columns();

          if (m_archive_read_stage < ArchiveReadStage::TablesInitialized) {
            m_archive_reader.open_packed_streams();
            m_archive_read_stage = ArchiveReadStage::TablesInitialized;
          }
          auto& reader = m_archive_reader.read_schema_table(
              m_current_schema_id, false, false);
          reader.initialize_filter_with_column_map(m_query_runner.get());

          m_error_code = ErrorCode::Success;
          m_current_schema_table_loaded = true;
        } else {
          m_current_schema_index =
              (m_current_schema_index + 1) % m_matched_schemas.size();
          m_error_code = ErrorCode::DictionaryNotFound;
          continue;
        }
      }

      if (auto num_rows_fetched =
              m_query_runner->fetch_next(num_rows, column_vectors);
          num_rows_fetched > 0) {
        return num_rows_fetched;
      }

      m_current_schema_index =
          (m_current_schema_index + 1) % m_matched_schemas.size();
      m_completed_schema_cycles = m_current_schema_index == m_end_schema_index;
      m_current_schema_table_loaded = false;
    }

    move_to_next_archive();
    while (false == m_completed_archive_cycles) {
      m_error_code = load_archive();

      if (ErrorCode::Success == m_error_code) {
        m_query_runner = std::make_shared<QueryRunner>(
            m_expr,
            m_schema_match,
            m_ignore_case,
            m_schema_map,
            m_schema_tree,
            m_projection,
            m_var_dict,
            m_log_dict);
        m_query_runner->populate_string_queries();
        break;
      }
      move_to_next_archive();
    }
  }
  return 0;
}

size_t Cursor::fetch_next(
    size_t num_rows,
    std::vector<ColumnData>& column_vectors) {
  if (m_error_code != ErrorCode::Success) {
    return 0;
  }

  while (false == m_completed_archive_cycles) {
    while (false == m_completed_schema_cycles) {
      // whether the schema table is loaded
      if (false == m_current_schema_table_loaded) {
        m_current_schema_id = m_matched_schemas[m_current_schema_index];
        m_query_runner->set_schema(m_current_schema_id);
        m_query_runner->populate_searched_wildcard_columns();
        m_expression_value = m_query_runner->constant_propagate();

        if (m_expression_value != EvaluatedValue::False) {
          m_query_runner->add_wildcard_columns_to_searched_columns();

          if (m_archive_read_stage < ArchiveReadStage::TablesInitialized) {
            m_archive_reader.open_packed_streams();
            m_archive_read_stage = ArchiveReadStage::TablesInitialized;
          }
          auto& reader = m_archive_reader.read_schema_table(
              m_current_schema_id, false, false);
          reader.initialize_filter_with_column_map(m_query_runner.get());
          m_error_code = ErrorCode::Success;
          m_current_schema_table_loaded = true;
        } else {
          m_current_schema_index =
              (m_current_schema_index + 1) % m_matched_schemas.size();
          m_error_code = ErrorCode::DictionaryNotFound;
          continue;
        }
      }

      if (auto num_rows_fetched =
              m_query_runner->fetch_next(num_rows, column_vectors);
          num_rows_fetched > 0) {
        return num_rows_fetched;
      }

      m_current_schema_index =
          (m_current_schema_index + 1) % m_matched_schemas.size();
      m_completed_schema_cycles = m_current_schema_index == m_end_schema_index;
      m_current_schema_table_loaded = false;
    }

    move_to_next_archive();
    while (false == m_completed_archive_cycles) {
      m_error_code = load_archive();

      if (ErrorCode::Success == m_error_code) {
        m_query_runner = std::make_shared<QueryRunner>(
            m_expr,
            m_schema_match,
            m_ignore_case,
            m_schema_map,
            m_schema_tree,
            m_projection,
            m_var_dict,
            m_log_dict);
        m_query_runner->populate_string_queries();
        break;
      }
      move_to_next_archive();
    }
  }
  return 0;
}
} // namespace facebook::velox::connector::clp::search_lib
