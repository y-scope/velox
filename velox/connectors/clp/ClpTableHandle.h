#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpTableHandle : public ConnectorTableHandle {
 public:
  ClpTableHandle(std::string connectorId, const std::string& tableName)
      : ConnectorTableHandle(connectorId), tableName_(tableName) {}

  [[nodiscard]] const std::string& name() const {
    return tableName_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

 private:
  const std::string tableName_;
};
} // namespace facebook::velox::connector::clp
