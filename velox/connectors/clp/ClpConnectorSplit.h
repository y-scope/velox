#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpConnectorSplit : public connector::ConnectorSplit {
 public:
  ClpConnectorSplit(
      const std::string& connectorId,
      const std::string& schemaName,
      const std::string& tableName)
      : connector::ConnectorSplit(connectorId),
        schemaName_(schemaName),
        tableName_(tableName){}

  [[nodiscard]] std::string toString() const override {
    return fmt::format("CLP: {}.{}", schemaName_, tableName_);
  }

  [[nodiscard]] std::string schemaName() const {
    return schemaName_;
  }

  [[nodiscard]] std::string tableName() const {
    return tableName_;
  }

 private:
  const std::string schemaName_;
  const std::string tableName_;
};
} // namespace facebook::velox::connector::clp
