#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
struct ClpConnectorSplit : public connector::ConnectorSplit {
  ClpConnectorSplit(
      const std::string& connectorId,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& archivePath)
      : connector::ConnectorSplit(connectorId),
        schemaName_(schemaName),
        tableName_(tableName),
        archivePath_(archivePath) {}

  [[nodiscard]] std::string toString() const override {
    return fmt::format("CLP: {}.{}", schemaName_, tableName_);
  }

  const std::string schemaName_;
  const std::string tableName_;
  const std::string archivePath_;
};
} // namespace facebook::velox::connector::clp
