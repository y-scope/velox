#include "velox/connectors/clp/ClpConnector.h"
#include "velox/connectors/clp/ClpDataSource.h"

namespace facebook::velox::connector::clp {
ClpConnector::ClpConnector(
    const std::string& id,
    std::shared_ptr<const Config> config,
    folly::Executor* executor)
    : Connector(id) {}

std::unique_ptr<DataSource> ClpConnector::createDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<ClpDataSource>();
}

std::unique_ptr<DataSink> ClpConnector::createDataSink(
    RowTypePtr inputType,
    std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
    ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy) {
  VELOX_NYI("createDataSink for ClpConnector is not implemented!");
}
} // namespace facebook::velox::connector::clp
