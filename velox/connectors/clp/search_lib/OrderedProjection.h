#pragma once

#include "clp_s/search/Projection.hpp"

namespace facebook::velox::connector::clp::search_lib {

enum class ColumnType;

class OrderedProjection : public clp_s::search::Projection {
 public:
  class OperationFailed : public clp_s::TraceableException {
   public:
    // Constructors
    OperationFailed(
        clp_s::ErrorCode error_code,
        char const* const filename,
        int line_number)
        : TraceableException(error_code, filename, line_number) {}
  };

  explicit OrderedProjection(clp_s::search::ProjectionMode mode)
      : clp_s::search::Projection(mode), m_projection_mode{mode} {}

  /**
   * Adds a column to the set of columns that should be included in the
   * projected results
   * @param column
   * @throws OperationFailed if `column` contains a wildcard
   * @throws OperationFailed if this instance of Projection is in mode
   * ReturnAllColumns
   * @throws OperationFailed if `column` is identical to a previously added
   * column
   */
  void add_ordered_column(
      std::shared_ptr<clp_s::search::ColumnDescriptor> column);

  /**
   * Adds a column to the set of columns that should be included in the
   * projected results
   * @param column
   * @param node_type
   * @throws OperationFailed if `column` contains a wildcard
   * @throws OperationFailed if this instance of Projection is in mode
   * ReturnAllColumns
   * @throws OperationFailed if `column` is identical to a previously added
   * column
   */
  void add_ordered_column(
      const std::shared_ptr<clp_s::search::ColumnDescriptor>& column,
      ColumnType node_type);

  /**
   * Resolves all columns for the purpose of projection. This key resolution
   * implementation is more limited than the one in schema matching. In
   * particular, this version of key resolution only allows resolving keys that
   * do not contain wildcards and does not allow resolving to objects within
   * arrays.
   *
   * Note: we could try to generalize column resolution code/move it to the
   * schema tree. It is probably best to write a simpler version dedicated to
   * projection for now since types are leaf-only. The type-per-token idea
   * solves this problem (in the absence of wildcards).
   *
   * @param tree
   */
  void resolve_ordered_columns(const std::shared_ptr<clp_s::SchemaTree>& tree);

  /**
   * @return the list of matching nodes
   */
  std::vector<std::vector<int32_t>> const& get_matching_nodes_list() const {
    return m_matching_nodes_list;
  }

 private:
  /**
   * Resolves an individual column as described by the `resolve_columns` method.
   * @param tree
   * @param column
   */
  void resolve_column(
      const std::shared_ptr<clp_s::SchemaTree>& tree,
      const std::shared_ptr<clp_s::search::ColumnDescriptor>& column);

  std::vector<std::shared_ptr<clp_s::search::ColumnDescriptor>>
      m_selected_columns;
  std::vector<std::vector<int32_t>> m_matching_nodes_list;
  absl::flat_hash_set<int32_t> m_matching_nodes;
  clp_s::search::ProjectionMode m_projection_mode{
      clp_s::search::ProjectionMode::ReturnAllColumns};
};
} // namespace facebook::velox::connector::clp::search_lib