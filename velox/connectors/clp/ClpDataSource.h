#pragma once

#include <unistd.h>     // For pipe, fork, exec
#include <sys/wait.h>   // For waitpid
#include <set>
#include <mutex>
#include <fstream>
#include <thread>

#include <boost/filesystem.hpp>

#include "velox/connectors/Connector.h"
#include "velox/connectors/clp/ClpConfig.h"

#include "velox/connectors/clp/search_lib/Cursor.h"

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
  std::string executablePath_;
  std::string archiveDir_;
  std::string inputSource_;
  std::string kqlQuery_;
  bool polymorphicTypeEnabled_;
  velox::memory::MemoryPool* pool_;
  RowTypePtr outputType_;
  std::set<std::string> columnUntypedNames_;
  std::map<std::string, size_t> columnIndices_;
  std::map<std::string, size_t> arrayOffsets_;
  uint64_t completedRows_{0};
  uint64_t completedBytes_{0};

  std::vector<search_lib::Field> fields_;

  std::unique_ptr<search_lib::Cursor> cursor_;
};
} // namespace facebook::velox::connector::clp
