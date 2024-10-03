#pragma once

#include "velox/connectors/Connector.h"
#include "velox/connectors/clp/ClpConfig.h"

namespace facebook::velox::connector::clp {
class ClpConnector : public Connector {
 public:
  ClpConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor);

  [[nodiscard]] const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const override {
    return config_->config();
  }

  [[nodiscard]] bool canAddDynamicFilter() const override {
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
      CommitStrategy commitStrategy) override;

  [[nodiscard]] folly::Executor* executor() const override {
    return executor_;
  }

 private:
  std::shared_ptr<const ClpConfig> config_;
  folly::Executor* executor_;
};

class ClpConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* kClpConnectorName = "clp";

  ClpConnectorFactory() : ConnectorFactory(kClpConnectorName) {}
  explicit ClpConnectorFactory(const char* connectorName)
      : ConnectorFactory(connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor) override {
    return std::make_shared<ClpConnector>(id, config, executor);
  }
};
} // namespace facebook::velox::connector::clp
