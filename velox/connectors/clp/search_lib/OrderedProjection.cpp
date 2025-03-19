#include "velox/connectors/clp/search_lib/OrderedProjection.h"
#include <algorithm>

#include <spdlog/spdlog.h>

#include "clp_s/search/SearchUtils.hpp"
#include "velox/connectors/clp/search_lib/Cursor.h"

using namespace clp_s;
using namespace clp_s::search;

namespace facebook::velox::connector::clp::search_lib {
void OrderedProjection::add_ordered_column(
    std::shared_ptr<ColumnDescriptor> column) {
  if (column->is_unresolved_descriptor()) {
    throw OperationFailed(ErrorCodeBadParam, __FILE__, __LINE__);
  }
  if (ProjectionMode::ReturnAllColumns == m_projection_mode) {
    throw OperationFailed(ErrorCodeUnsupported, __FILE__, __LINE__);
  }
  if (m_selected_columns.end() !=
      std::find_if(
          m_selected_columns.begin(),
          m_selected_columns.end(),
          [column](auto const& rhs) -> bool { return *column == *rhs; })) {
    // no duplicate columns in projection
    throw OperationFailed(ErrorCodeBadParam, __FILE__, __LINE__);
  }
  m_selected_columns.push_back(column);
}

void OrderedProjection::add_ordered_column(
    const std::shared_ptr<ColumnDescriptor>& column,
    ColumnType node_type) {
  if (column->is_unresolved_descriptor()) {
    throw OperationFailed(ErrorCodeBadParam, __FILE__, __LINE__);
  }
  if (ProjectionMode::ReturnAllColumns == m_projection_mode) {
    throw OperationFailed(ErrorCodeUnsupported, __FILE__, __LINE__);
  }
  switch (node_type) {
    case ColumnType::String:
      column->set_matching_types(
          LiteralType::ClpStringT | LiteralType::VarStringT |
          LiteralType::EpochDateT);
      break;
    case ColumnType::Integer:
      column->set_matching_types(LiteralType::IntegerT);
      break;
    case ColumnType::Float:
      column->set_matching_types(LiteralType::FloatT);
      break;
    case ColumnType::Boolean:
      column->set_matching_types(LiteralType::BooleanT);
      break;
    case ColumnType::Array:
      column->set_matching_types(LiteralType::ArrayT);
      break;
  }
  m_selected_columns.push_back(column);
}

void OrderedProjection::resolve_ordered_columns(
    const std::shared_ptr<SchemaTree>& tree) {
  for (auto& column : m_selected_columns) {
    resolve_column(tree, column);
  }
}

void OrderedProjection::resolve_column(
    const std::shared_ptr<SchemaTree>& tree,
    const std::shared_ptr<ColumnDescriptor>& column) {
  /**
   * Ideally we would reuse the code from SchemaMatch for resolving columns, but
   * unfortunately we can not.
   *
   * The main reason is that here we don't want to allow projection to travel
   * inside unstructured objects -- it may be possible to support such a thing
   * in the future, but it poses some extra challenges (e.g. deciding what to do
   * when projecting repeated elements in a structure).
   *
   * It would be possible to create code that can handle our use-case and
   * SchemaMatch's use-case in an elegant way, but it's a significant refactor.
   * In particular, if we extend our column type system to be one-per-token
   * instead of one-per-column we can make it so that intermediate tokens will
   * not match certain kinds of MPT nodes (like the node for structured arrays).
   *
   * In light of that we implement a simple version of column resolution here
   * that does exactly what we need.
   */
  std::vector<int32_t> local_matching_node_list;
  auto cur_node_id =
      tree->get_object_subtree_node_id_for_namespace(column->get_namespace());
  auto it = column->descriptor_begin();
  while (it != column->descriptor_end()) {
    bool matched_any{false};
    auto cur_it = it++;
    bool last_token = it == column->descriptor_end();
    auto const& cur_node = tree->get_node(cur_node_id);
    for (int32_t child_node_id : cur_node.get_children_ids()) {
      auto const& child_node = tree->get_node(child_node_id);

      // Intermediate nodes must be objects
      if (false == last_token && child_node.get_type() != NodeType::Object) {
        continue;
      }

      if (child_node.get_key_name() != cur_it->get_token()) {
        continue;
      }

      matched_any = true;
      if (last_token &&
          column->matches_type(node_to_literal_type(child_node.get_type()))) {
        m_matching_nodes.insert(child_node_id);
        local_matching_node_list.push_back(child_node_id);
      } else if (false == last_token) {
        cur_node_id = child_node_id;
        break;
      }
    }

    if (false == matched_any) {
      break;
    }
  }

  m_matching_nodes_list.push_back(local_matching_node_list);
}
} // namespace facebook::velox::connector::clp::search_lib
