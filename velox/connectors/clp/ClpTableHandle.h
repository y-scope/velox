#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpTableHandle : public ConnectorTableHandle {
 public:
  ClpTableHandle(
      std::string connectorId,
      const std::string& tableName,
      std::shared_ptr<std::string> query)
      : ConnectorTableHandle(connectorId),
        tableName_(tableName),
        query_(std::move(query)) {}

  [[nodiscard]] const std::string& tableName() const {
    return tableName_;
  }

  [[nodiscard]] const std::shared_ptr<std::string>& query() const {
    return query_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

 private:
  const std::string tableName_;
  std::shared_ptr<std::string> query_;
};
} // namespace facebook::velox::connector::clp
