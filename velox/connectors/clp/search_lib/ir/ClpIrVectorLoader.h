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

#include <simdjson.h>

#include "connectors/clp/search_lib/BaseClpCursor.h"
#include "ffi/ir_stream/Deserializer.hpp"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::connector::clp::search_lib {

class ClpIrVectorLoader : public VectorLoader {
 public:
  ClpIrVectorLoader(
      bool isResolved,
      ColumnType nodeType,
      ::clp::ffi::SchemaTree::Node::id_t nodeId,
      const std::shared_ptr<
          const std::vector<std::unique_ptr<::clp::ffi::KeyValuePairLogEvent>>>&
          filteredLogEvents)
      : isResolved_(isResolved),
        nodeType_(nodeType),
        nodeId_(nodeId),
        filteredLogEvents_(filteredLogEvents) {}

 private:
  simdjson::ondemand::parser arrayParser_;

  bool isResolved_;
  ColumnType nodeType_;
  ::clp::ffi::SchemaTree::Node::id_t nodeId_;
  std::shared_ptr<
      const std::vector<std::unique_ptr<::clp::ffi::KeyValuePairLogEvent>>>
      filteredLogEvents_;

  void loadInternal(
      RowSet rows,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override;
};

} // namespace facebook::velox::connector::clp::search_lib
