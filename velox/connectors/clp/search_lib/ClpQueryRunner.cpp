#include "velox/connectors/clp/search_lib/ClpQueryRunner.h"

#include "clp_s/search/ast/SearchUtils.hpp"
#include "clp_s/search/clp_search/Grep.hpp"

#include "velox/vector/ComplexVector.h"

using namespace clp_s;
using namespace clp_s::search;
using namespace clp_s::search::clp_search;

namespace facebook::velox::connector::clp::search_lib {
void ClpQueryRunner::init(
    clp_s::SchemaReader* schema_reader,
    std::unordered_map<int32_t, clp_s::BaseColumnReader*> const& column_map) {
  numMessages_ = schema_reader->get_num_messages();
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
      auto column_it = column_map.find(node_id);
      if (column_it != column_map.end()) {
        projectedColumns_.push_back(column_it->second);
        found_reader = true;
        break;
      }
    }

    if (!found_reader) {
      projectedColumns_.push_back(nullptr);
    }
  }

  for (auto& [column_id, column_reader] : column_map) {
    initialize_reader(column_id, column_reader);
  }
}

void ClpQueryRunner::fetchNext(
    size_t num_rows,
    std::vector<size_t>& filteredRows) {
  size_t num_rows_fetched = 0;
  while (curMessage_ < numMessages_) {
    if (filter(curMessage_)) {
      filteredRows.emplace_back(curMessage_);
      num_rows_fetched += 1;
    }
    curMessage_ += 1;
    if (num_rows_fetched >= num_rows) {
      break;
    }
  }
}

} // namespace facebook::velox::connector::clp::search_lib
