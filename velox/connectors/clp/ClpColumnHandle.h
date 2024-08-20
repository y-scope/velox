#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpColumnHandle : public ColumnHandle {
 public:
  ClpColumnHandle(
      const std::string& columnName,
      const TypePtr& columnType,
      bool nullable)
      : columnName_(columnName), columnType_(columnType), nullable_(nullable) {}

  const std::string& columnName() const {
    return columnName_;
  }

  const TypePtr& columnType() const {
    return columnType_;
  }

  bool nullable() const {
    return nullable_;
  }

 private:
  const std::string columnName_;
  const TypePtr columnType_;
  const bool nullable_;
};
} // namespace facebook::velox::connector::clp
