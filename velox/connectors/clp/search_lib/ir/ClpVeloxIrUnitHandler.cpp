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

#include "velox/connectors/clp/search_lib/ir/ClpVeloxIrUnitHandler.h"

#include "clp_s/SchemaTree.hpp"
#include "common/base/Exceptions.h"

namespace facebook::velox::connector::clp::search_lib {

auto ClpVeloxIrUnitHandler::handle_log_event(
    ::clp::ffi::KeyValuePairLogEvent log_event)
    -> ::clp::ffi::ir_stream::IRErrorCode {
  return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
}

auto ClpVeloxIrUnitHandler::handle_schema_tree_node_insertion(
    bool is_auto_generated,
    ::clp::ffi::SchemaTree::NodeLocator schema_tree_node_locator,
    std::shared_ptr<::clp::ffi::SchemaTree const> const& schema_tree)
    -> ::clp::ffi::ir_stream::IRErrorCode {
  auto parentNodeId = schema_tree_node_locator.get_parent_id();
  auto selfNodeId = static_cast<::clp::ffi::SchemaTree::Node::id_t>(
      schema_tree->get_size() - 1);
  if (is_auto_generated) {
    std::string actualNodeName;
    if (schema_tree->get_node(parentNodeId).is_root()) {
      actualNodeName = schema_tree_node_locator.get_key_name();
    } else {
      actualNodeName = fmt::format(
          "{}.{}",
          autoGenNodeIdNameMap[parentNodeId],
          schema_tree_node_locator.get_key_name());
    }
    autoGenNodeIdNameMap[selfNodeId] = actualNodeName;
    autoGenNodeNameIdMap[actualNodeName] = selfNodeId;
  } else {
    std::string actualNodeName;
    if (schema_tree->get_node(parentNodeId).is_root()) {
      actualNodeName = schema_tree_node_locator.get_key_name();
    } else {
      actualNodeName = fmt::format(
          "{}.{}",
          userGenNodeIdNameMap[parentNodeId],
          schema_tree_node_locator.get_key_name());
    }
    userGenNodeIdNameMap[selfNodeId] = actualNodeName;
    userGenNodeNameIdMap[actualNodeName] = selfNodeId;
  }
  return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
}

::clp::ffi::SchemaTree::Node::id_t ClpVeloxIrUnitHandler::findNodeIdByName(
    std::string_view name) const {
  auto name_str = std::string(name);
  if (0 != autoGenNodeNameIdMap.count(name_str)) {
    return autoGenNodeNameIdMap.at(name_str);
  }
  if (0 != userGenNodeNameIdMap.count(name_str)) {
    return userGenNodeNameIdMap.at(name_str);
  }
  VELOX_USER_FAIL(fmt::format("No field named: {}", name_str));
}

} // namespace facebook::velox::connector::clp::search_lib
