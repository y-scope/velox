#include "velox/connectors/clp/search_lib/KVIRCursor.h"

#include <cstdint>
#include <exception>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include <system_error>

#include <spdlog/spdlog.h>

#include "velox/connectors/clp/search_lib/Cursor.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/type/StringView.h"
#include "clp/ffi/ir_stream/Deserializer.hpp"
#include "clp_s/InputConfig.hpp"
#include "clp_s/ReaderUtils.hpp"
#include "clp_s/search/kql/kql.hpp"

namespace facebook::velox::connector::clp::search_lib {
namespace {
auto get_literal_types_for_column_type(ColumnType type) -> clp_s::search::ast::LiteralTypeBitmask {
  switch (type) {
  case ColumnType::String:
    return clp_s::search::ast::LiteralType::ClpStringT | clp_s::search::ast::VarStringT;
  case ColumnType::Integer:
    return clp_s::search::ast::LiteralType::IntegerT;
  case ColumnType::Float:
    return clp_s::search::ast::LiteralType::FloatT;
  case ColumnType::Array:
    return clp_s::search::ast::LiteralType::ArrayT;
  case ColumnType::Boolean:
    return clp_s::search::ast::LiteralType::BooleanT;
  default:
    return clp_s::search::ast::LiteralType::UnknownT;
  }
}
} // namespace

KVIRCursor::KVIRCursor(
    clp_s::InputSource input_source,
    std::vector<std::string> kvir_paths,
    bool ignore_case)
    : input_source_(input_source),
      kvir_paths_(std::move(kvir_paths)),
      ignore_case_(ignore_case) {}

ErrorCode KVIRCursor::execute_query(
    std::string const& query,
    std::vector<Field>const& output_columns) {
  current_kvir_path_ = std::nullopt;
  projected_fields_ = output_columns;
  projected_clp_typed_fields_.clear();
  for (auto it = projected_fields_.begin(); projected_fields_.end() != it; ++it) {
    projected_clp_typed_fields_.emplace_back(std::make_pair(it->name, get_literal_types_for_column_type(it->type)));
  }

  auto query_stream = std::istringstream(query);
  expr_ = clp_s::search::kql::parse_kql_expression(query_stream);
  if (nullptr == expr_) {
      return ErrorCode::InvalidQuerySyntax;
  }

  return load_next_kvir_stream();
}

size_t KVIRCursor::fetch_next(
    size_t num_rows,
    std::vector<facebook::velox::VectorPtr>& column_vectors) {
  if (false == current_kvir_path_.has_value() || false == kvir_deserializer_.has_value()) {
    return 0ULL;
  }
  if (kvir_paths_.end() == current_kvir_path_.value()) {
    return 0ULL;
  }

  size_t num_rows_fetched{0ULL};
  while (num_rows_fetched < num_rows && kvir_deserializer_.has_value()) {
    auto const result{kvir_deserializer_.value().deserialize_next_ir_unit(kvir_decompressor_)};
    if (result.has_error() && std::errc::no_message != result.error()) {
        if (ErrorCode::Success != load_next_kvir_stream()) {
            return num_rows_fetched;
        }
        continue;
    }
    if (result.value() == ::clp::ffi::ir_stream::IrUnitType::EndOfStream) {
        if (ErrorCode::Success != load_next_kvir_stream()) {
            return num_rows_fetched;
        }
        continue;
    }
    if (result.value() == ::clp::ffi::ir_stream::IrUnitType::LogEvent) {
        auto const& ir_unit_handler = kvir_deserializer_.value().get_ir_unit_handler();
        marshal_row(num_rows_fetched, column_vectors, ir_unit_handler);
        ++num_rows_fetched;
    }
  }
  return num_rows_fetched;
}

void KVIRCursor::marshal_row(size_t row_number, std::vector<facebook::velox::VectorPtr>& column_vectors, IrUnitHandler const& handler) {
  auto const& log_event_option = handler.get_deserialized_log_event();
  if (false == log_event_option.has_value()) {
    return;
  }
  auto const& log_event = log_event_option.value();
  auto const& ordered_resolved_ids = handler.get_ordered_resolved_ids();
  auto const& auto_gen_pairs = log_event.get_auto_gen_node_id_value_pairs();
  auto const& user_gen_pairs = log_event.get_user_gen_node_id_value_pairs();
  for (size_t i = 0; i < column_vectors.size(); ++i) {
    std::optional<::clp::ffi::Value> const *value_ptr{nullptr};
    for (auto const& [node_id, is_auto_gen] : ordered_resolved_ids[i]) {
        if (is_auto_gen) {
            if (auto it = auto_gen_pairs.find(node_id); auto_gen_pairs.end() != it) {
                value_ptr = &it->second;
                break;
            }
        } else {
            if (auto it = user_gen_pairs.find(node_id); user_gen_pairs.end() != it) {
                value_ptr = &it->second;
                break;
            }
        }
    }
    if (nullptr == value_ptr || false == value_ptr->has_value()) {
        column_vectors[i]->setNull(row_number, true);
        continue;
    }

    auto const& value = value_ptr->value();
    switch(projected_fields_[i].type) {
        case ColumnType::String: {
            auto string_vector = column_vectors[i]->asFlatVector<facebook::velox::StringView>();
            column_vectors[i]->setNull(row_number, false);
            if (value.is<std::string>()) {
                string_vector->set(row_number, facebook::velox::StringView(value.get_immutable_view<std::string>()));
            } else if (value.is<::clp::ir::EightByteEncodedTextAst>()) {
                auto decode_result = value.get_immutable_view<::clp::ir::EightByteEncodedTextAst>().decode_and_unparse();
                if (false == decode_result.has_value()) {
                    column_vectors[i]->setNull(row_number, true);
                } else {
                    string_vector->set(row_number, facebook::velox::StringView(decode_result.value()));
                }
            } else {
                auto decode_result = value.get_immutable_view<::clp::ir::FourByteEncodedTextAst>().decode_and_unparse();
                if (false == decode_result.has_value()) {
                    column_vectors[i]->setNull(row_number, true);
                } else {
                    string_vector->set(row_number, facebook::velox::StringView(decode_result.value()));
                }
            }
            break;
        }
        case ColumnType::Integer: {
            auto int_vector = column_vectors[i]->asFlatVector<int64_t>();
            int_vector->set(row_number, value.get_immutable_view<::clp::ffi::value_int_t>());
            column_vectors[i]->setNull(row_number, false);
            break;
        }
        case ColumnType::Float: {
            auto float_vector = column_vectors[i]->asFlatVector<double>();
            float_vector->set(row_number, value.get_immutable_view<::clp::ffi::value_float_t>());
            column_vectors[i]->setNull(row_number, false);
            break;
        }
        case ColumnType::Array: {
            auto array_vector = std::dynamic_pointer_cast<facebook::velox::ArrayVector>(column_vectors[i]);
            column_vectors[i]->setNull(row_number, false);
            std::string json_string;
            if (value.is<::clp::ir::EightByteEncodedTextAst>()) {
                auto decode_result = value.get_immutable_view<::clp::ir::EightByteEncodedTextAst>().decode_and_unparse();
                if (false == decode_result.has_value()) {
                    column_vectors[i]->setNull(row_number, true);
                    json_string = std::move(decode_result.value());
                    break;
                }
            } else {
                auto decode_result = value.get_immutable_view<::clp::ir::FourByteEncodedTextAst>().decode_and_unparse();
                if (false == decode_result.has_value()) {
                    column_vectors[i]->setNull(row_number, true);
                    break;
                }
                json_string = std::move(decode_result.value());
            }

            size_t num_elements{0ULL};
            auto elements = array_vector->elements()->asFlatVector<facebook::velox::StringView>();
            auto obj = array_parser_.iterate(json_string);
            std::vector<std::string_view> raw_elements;
            for (auto array_element : obj.get_array()) {
                auto raw_element = simdjson::to_json_string(array_element).value();
                raw_elements.emplace_back(raw_element);
            }
            elements->resize(raw_elements.size());
            for (auto& raw_element : raw_elements) {
                elements->set(num_elements++, facebook::velox::StringView(raw_element));
            }
            array_vector->setOffsetAndSize(row_number, 0ULL, num_elements);
            array_vector->setNull(row_number, false);
            break;
        }
        case ColumnType::Boolean: {
            auto bool_vector = column_vectors[i]->asFlatVector<bool>();
            bool_vector->set(row_number, value.get_immutable_view<::clp::ffi::value_bool_t>());
            column_vectors[i]->setNull(row_number, false);
            break;
        }
        default:
            column_vectors[i]->setNull(row_number, true);
            break;
    }
  }
}


ErrorCode KVIRCursor::load_next_kvir_stream() {
    kvir_deserializer_ = std::nullopt;
    kvir_decompressor_.close();

    if (false == current_kvir_path_.has_value()) {
        current_kvir_path_.emplace(kvir_paths_.begin());
    } else if (current_kvir_path_.has_value() && kvir_paths_.end() == current_kvir_path_.value()) {
        return ErrorCode::Success;
    } else {
        ++current_kvir_path_.value();
    }

    auto const archive_path_it = current_kvir_path_.value();
    if (kvir_paths_.end() == archive_path_it) {
        return ErrorCode::Success;
    }

    auto archivePath = clp_s::Path{.source = input_source_, .path = *archive_path_it};
    auto networkAuthOption = clp_s::NetworkAuthOption{};
    if (clp_s::InputSource::Filesystem != input_source_) {
        networkAuthOption.method = clp_s::AuthMethod::S3PresignedUrlV4;
    }
    kvir_reader_ = clp_s::ReaderUtils::try_create_reader(archivePath, networkAuthOption);
    if (nullptr == kvir_reader_) {
        SPDLOG_ERROR("Failed to open kv-ir stream \"{}\" for reading.", *archive_path_it);
        return ErrorCode::InternalError;
    }

    try {
        kvir_decompressor_.open(*kvir_reader_, 64 * 1024);
    } catch (std::exception const& e) {
        SPDLOG_ERROR("Failed to open kv-ir stream \"{}\" for decompression: {}.", *archive_path_it, e.what());
        return ErrorCode::InternalError;
    }
    
    auto result{::clp::ffi::ir_stream::Deserializer<IrUnitHandler>::create(kvir_decompressor_, IrUnitHandler{projected_fields_}, expr_->copy(), projected_clp_typed_fields_, false == ignore_case_)};
    if (result.has_error()) {
        SPDLOG_ERROR("Failed to open kv-ir stream \"{}\" for deserialization: {}.", *archive_path_it, result.error().message());
        return ErrorCode::InternalError;
    }

    kvir_deserializer_.emplace(std::move(result.value()));
    return ErrorCode::Success;
}
} // namespace facebook::velox::connector::clp::search_lib