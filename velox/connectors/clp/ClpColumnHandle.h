#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpColumnHandle : public ColumnHandle {
 public:
  ClpColumnHandle(
      const std::string& columnName,
      const std::string& originalColumnName,
      const TypePtr& columnType,
      bool nullable)
      : columnName_(columnName),
        originalColumnName_(originalColumnName),
        columnType_(columnType),
        nullable_(nullable) {}

  const std::string& columnName() const {
    return columnName_;
  }

  const std::string& originalColumnName() const {
    return originalColumnName_;
  }

  const TypePtr& columnType() const {
    return columnType_;
  }

  bool nullable() const {
    return nullable_;
  }

 private:
  const std::string columnName_;
  const std::string originalColumnName_;
  const TypePtr columnType_;
  const bool nullable_;
};
} // namespace facebook::velox::connector::clp
