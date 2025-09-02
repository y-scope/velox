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

#include "ffi/ir_stream/Deserializer.hpp"

namespace facebook::velox::connector::clp::search_lib {

class ClpVeloxIrUnitHandler {
 public:
  ClpVeloxIrUnitHandler() {
    autoGenNodeIdNameMap =
        std::unordered_map<::clp::ffi::SchemaTree::Node::id_t, std::string>{};
    autoGenNodeNameIdMap =
        std::unordered_map<std::string, ::clp::ffi::SchemaTree::Node::id_t>{};
    userGenNodeIdNameMap =
        std::unordered_map<::clp::ffi::SchemaTree::Node::id_t, std::string>{};
    userGenNodeNameIdMap =
        std::unordered_map<std::string, ::clp::ffi::SchemaTree::Node::id_t>{};
  }

  // Destructor
  ~ClpVeloxIrUnitHandler() = default;

  // Methods implementing `IrUnitHandlerInterface`
  [[nodiscard]] auto handle_log_event(
      ::clp::ffi::KeyValuePairLogEvent log_event)
      -> ::clp::ffi::ir_stream::IRErrorCode;

  [[nodiscard]] auto handle_utc_offset_change(
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
          schema_tree) -> ::clp::ffi::ir_stream::IRErrorCode;

  [[nodiscard]] auto handle_end_of_stream()
      -> ::clp::ffi::ir_stream::IRErrorCode {
    return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
  }

  ::clp::ffi::SchemaTree::Node::id_t findNodeIdByName(
      std::string_view name) const;

 private:
  std::unordered_map<::clp::ffi::SchemaTree::Node::id_t, std::string>
      autoGenNodeIdNameMap;
  std::unordered_map<std::string, ::clp::ffi::SchemaTree::Node::id_t>
      autoGenNodeNameIdMap;
  std::unordered_map<::clp::ffi::SchemaTree::Node::id_t, std::string>
      userGenNodeIdNameMap;
  std::unordered_map<std::string, ::clp::ffi::SchemaTree::Node::id_t>
      userGenNodeNameIdMap;
};

} // namespace facebook::velox::connector::clp::search_lib
