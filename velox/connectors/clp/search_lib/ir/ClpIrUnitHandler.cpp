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

#include "velox/connectors/clp/search_lib/ir/ClpIrUnitHandler.h"

#include "clp_s/SchemaTree.hpp"
#include "common/base/Exceptions.h"

namespace facebook::velox::connector::clp::search_lib {

auto ClpIrUnitHandler::handle_log_event(
    ::clp::ffi::KeyValuePairLogEvent log_event,
    size_t log_event_idx) -> ::clp::ffi::ir_stream::IRErrorCode {
  filteredLogEvents_->push_back(
      std::make_unique<::clp::ffi::KeyValuePairLogEvent>(std::move(log_event)));
  return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
}

auto ClpIrUnitHandler::handle_schema_tree_node_insertion(
    bool is_auto_generated,
    ::clp::ffi::SchemaTree::NodeLocator schema_tree_node_locator,
    std::shared_ptr<::clp::ffi::SchemaTree const> const& schema_tree)
    -> ::clp::ffi::ir_stream::IRErrorCode {
  return ::clp::ffi::ir_stream::IRErrorCode::IRErrorCode_Success;
}

void ClpIrUnitHandler::clearFilteredLogEvents() {
  filteredLogEvents_->clear();
}

} // namespace facebook::velox::connector::clp::search_lib
