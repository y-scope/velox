#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::clp {
class ClpConnector : public Connector {
 public:
  ClpConnector(
      const std::string& id,
      std::shared_ptr<const Config> config,
      folly::Executor* executor);

  const std::shared_ptr<const Config>& connectorConfig() const override {
    return config_;
  }

  bool canAddDynamicFilter() const override {
    return false;
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  bool supportsSplitPreload() override {
    return false;
  }

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override final;

  folly::Executor* executor() const override {
    return executor_;
  }

 private:
  std::shared_ptr<const Config> config_;
  folly::Executor* executor_;
};
} // namespace facebook::velox::connector::clp
