#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpConnectorSplit : public connector::ConnectorSplit {
 public:
  ClpConnectorSplit() = default;
  ~ClpConnectorSplit() override = default;
};
} // namespace facebook::velox::connector::clp
