#pragma once

#include <simdjson.h>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "Cursor.h"
#include "clp/ReaderInterface.hpp"
#include "clp/ffi/KeyValuePairLogEvent.hpp"
#include "clp/ffi/SchemaTree.hpp"
#include "clp/ffi/Value.hpp"
#include "clp/ffi/ir_stream/Deserializer.hpp"
#include "clp/ffi/ir_stream/IrUnitType.hpp"
#include "clp/ffi/ir_stream/decoding_methods.hpp"
#include "clp/ir/EncodedTextAst.hpp"
#include "clp/streaming_compression/zstd/Decompressor.hpp"
#include "clp/time_types.hpp"
#include "clp_s/InputConfig.hpp"
#include "clp_s/search/ast/Expression.hpp"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::connector::clp::search_lib {

/**
 * Class that implements `clp::ffi::ir_stream::IrUnitHandlerInterface` for
 * Key-Value IR decompression.
 */
class IrUnitHandler {
 public:
  using id_list =
      std::vector<std::pair<::clp::ffi::SchemaTree::Node::id_t, bool>>;

  explicit IrUnitHandler(std::vector<Field> const& projected_columns) {
    ordered_resolved_ids_.resize(projected_columns.size());
    for (size_t i = 0; i < projected_columns.size(); ++i) {
      projected_column_to_idx_.emplace(projected_columns.at(i).name, i);
    }
  }

  [[nodiscard]] auto handle_log_event(
      ::clp::ffi::KeyValuePairLogEvent&& log_event)
      -> ::clp::ffi::ir_stream::IRErrorCode {
    deserialized_log_event_.emplace(std::move(log_event));
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
  }

  [[nodiscard]] static auto handle_utc_offset_change(
      [[maybe_unused]] ::clp::UtcOffset utc_offset_old,
      [[maybe_unused]] ::clp::UtcOffset utc_offset_new)
      -> ::clp::ffi::ir_stream::IRErrorCode {
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
  }

  [[nodiscard]] auto handle_schema_tree_node_insertion(
      [[maybe_unused]] bool is_auto_generated,
      [[maybe_unused]] ::clp::ffi::SchemaTree::NodeLocator
          schema_tree_node_locator,
      [[maybe_unused]] std::shared_ptr<::clp::ffi::SchemaTree const> const&
          schema_tree) -> ::clp::ffi::ir_stream::IRErrorCode {
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
  }

  [[nodiscard]] auto handle_projection_resolution(
      [[maybe_unused]] bool is_auto_generated,
      [[maybe_unused]] ::clp::ffi::SchemaTree::Node::id_t node_id,
      [[maybe_unused]] std::string const& key_name)
      -> ::clp::ffi::ir_stream::IRErrorCode {
    if (auto it = projected_column_to_idx_.find(key_name);
        projected_column_to_idx_.end() != it) {
      ordered_resolved_ids_.at(it->second)
          .emplace_back(std::make_pair(node_id, is_auto_generated));
      return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
    }
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Corrupted_IR;
  }

  [[nodiscard]] auto handle_end_of_stream()
      -> ::clp::ffi::ir_stream::IRErrorCode {
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
  }

  [[nodiscard]] auto get_deserialized_log_event() const
      -> std::optional<::clp::ffi::KeyValuePairLogEvent> const& {
    return deserialized_log_event_;
  }

  [[nodiscard]] auto get_ordered_resolved_ids() const
      -> std::vector<id_list> const& {
    return ordered_resolved_ids_;
  }

 private:
  std::map<std::string, size_t> projected_column_to_idx_;
  std::vector<id_list> ordered_resolved_ids_;
  std::optional<::clp::ffi::KeyValuePairLogEvent> deserialized_log_event_;
};

class KVIRCursor {
 public:
  // Constructors
  explicit KVIRCursor(
      clp_s::InputSource input_source,
      std::vector<std::string> kvir_paths,
      bool ignore_case);

  /**
   * Prepares a query for execution.
   * @param query The query to execute.
   * @param output_columns The columns to retrieve from matching results.
   * @return ErrorCode::Success on success, and the relevant ErrorCode on
   * failure.
   */
  ErrorCode execute_query(
      std::string const& query,
      std::vector<Field> const& output_columns);

  /**
   * Fetches the next set of rows from the cursor.
   * @param num_rows The number of rows to fetch.
   * @param column_vectors The column vectors to fill.
   * @return The number of rows fetched.
   */
  size_t fetch_next(
      size_t num_rows,
      std::vector<facebook::velox::VectorPtr>& column_vectors);

 private:
  /**
   * Loads the next kv-ir stream to prepare it for search.
   *
   * On success kvir_deserializer_, kvir_decompressor_, kvir_reader_, and
   * current_kvir_path_ are properly initialized. On failure kvir_deserializer_
   * is set to `std::nullopt`.
   *
   * kvir_deserializer_ is also set to std::nullopt once all paths have been
   * searched.
   *
   * @return ErrorCode::Success on success and the relevant ErrorCode on
   * failure.
   */
  ErrorCode load_next_kvir_stream();

  /**
   * Marshals a single row of results.
   * @param row_number
   * @param column_vectors
   * @param handler
   */
  void marshal_row(
      size_t row_number,
      std::vector<facebook::velox::VectorPtr>& column_vectors,
      IrUnitHandler const& handler);

  clp_s::InputSource input_source_{clp_s::InputSource::Filesystem};
  std::vector<std::string> kvir_paths_;
  bool ignore_case_{};
  std::optional<std::vector<std::string>::iterator> current_kvir_path_{};
  std::vector<Field> projected_fields_;
  std::vector<std::pair<std::string, clp_s::search::ast::LiteralTypeBitmask>>
      projected_clp_typed_fields_;
  std::shared_ptr<clp_s::search::ast::Expression> expr_;
  std::shared_ptr<::clp::ReaderInterface> kvir_reader_;
  ::clp::streaming_compression::zstd::Decompressor kvir_decompressor_;
  std::optional<::clp::ffi::ir_stream::Deserializer<IrUnitHandler>>
      kvir_deserializer_{};
  simdjson::ondemand::parser array_parser_;
};

} // namespace facebook::velox::connector::clp::search_lib
