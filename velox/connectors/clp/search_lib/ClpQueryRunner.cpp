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

#include "velox/connectors/clp/search_lib/ClpQueryRunner.h"

#include "clp_s/search/ast/SearchUtils.hpp"
#include "clp_s/search/clp_search/Grep.hpp"

#include "velox/vector/ComplexVector.h"

using namespace clp_s;
using namespace clp_s::search;
using namespace clp_s::search::clp_search;

namespace facebook::velox::connector::clp::search_lib {
void ClpQueryRunner::init(
    clp_s::SchemaReader* schemaReader,
    std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& columnMap) {
  numMessages_ = schemaReader->get_num_messages();
  clear_readers();

  projectedColumns_.clear();
  auto matching_nodes_list = projection_->get_ordered_matching_nodes();
  for (const auto& node_ids : matching_nodes_list) {
    if (node_ids.empty()) {
      projectedColumns_.push_back(nullptr);
      continue;
    }

    // Try to find a matching column in m_column_map
    bool found_reader = false;
    for (const auto node_id : node_ids) {
      auto column_it = columnMap.find(node_id);
      if (column_it != columnMap.end()) {
        projectedColumns_.push_back(column_it->second);
        found_reader = true;
        break;
      }
    }

    if (!found_reader) {
      projectedColumns_.push_back(nullptr);
    }
  }

  for (auto& [column_id, column_reader] : columnMap) {
    initialize_reader(column_id, column_reader);
  }
}

void ClpQueryRunner::fetchNext(
    size_t numRows,
    std::vector<size_t>& filteredRows) {
  size_t num_rows_fetched = 0;
  while (curMessage_ < numMessages_) {
    if (filter(curMessage_)) {
      filteredRows.emplace_back(curMessage_);
      num_rows_fetched += 1;
    }
    curMessage_ += 1;
    if (num_rows_fetched >= numRows) {
      break;
    }
  }
}

} // namespace facebook::velox::connector::clp::search_lib
