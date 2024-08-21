#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpConnectorSplit : public connector::ConnectorSplit {
 public:
  ClpConnectorSplit(
      const std::string& connectorId,
      const std::string& schemaName,
      const std::string& tableName,
      const std::shared_ptr<std::string>& query)
      : connector::ConnectorSplit(connectorId),
        schemaName_(schemaName),
        tableName_(tableName),
        query_(query) {}

  [[nodiscard]] std::string toString() const override {
    return fmt::format("CLP: {}.{}", schemaName_, tableName_);
  }

 private:
  const std::string schemaName_;
  const std::string tableName_;
  const std::shared_ptr<std::string> query_;
};
} // namespace facebook::velox::connector::clp
