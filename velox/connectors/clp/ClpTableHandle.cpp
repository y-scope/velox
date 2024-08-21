#include "velox/connectors/clp/ClpTableHandle.h"

namespace facebook::velox::connector::clp {
std::string ClpTableHandle::toString() const {
  return ConnectorTableHandle::toString();
}

folly::dynamic ClpTableHandle::serialize() const {
  return ConnectorTableHandle::serialize();
}
} // namespace facebook::velox::connector::clp
