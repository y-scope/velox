#pragma once

#include <boost/process.hpp>
#include "velox/connectors/Connector.h"
#include "velox/connectors/clp/ClpConfig.h"

#include "simdjson.h"

namespace facebook::velox::connector::clp {
class ClpDataSource : public DataSource {
 public:
  ClpDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const ClpConfig>& clpConfig);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {
    VELOX_NYI("Dynamic filters not supported by ClpConnector.");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  static std::string getTypedColumnName(std::string& name, std::string suffix) {
    return name + "_" + suffix;
  }

  void parseJsonLine(
      simdjson::ondemand::value element,
      std::string& path,
      std::vector<VectorPtr>& vectors,
      uint64_t index);

  std::string executablePath_;
  std::string archiveDir_;
  std::string kqlQuery_;
  bool polymorphicTypeEnabled_;
  velox::memory::MemoryPool* pool_;
  boost::process::ipstream resultsStream_;
  RowTypePtr outputType_;
  std::map<std::string, size_t> columnIndices_;
  std::map<std::string, size_t> arrayOffsets_;
  uint64_t completedRows_{0};
  uint64_t completedBytes_{0};
};
} // namespace facebook::velox::connector::clp
