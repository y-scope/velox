/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <set>

#include <boost/filesystem.hpp>

#include "velox/connectors/Connector.h"
#include "velox/connectors/clp/ClpConfig.h"

#include "velox/connectors/clp/search_lib/ClpCursor.h"

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
  void addFieldsRecursively(
      const TypePtr& columnType,
      const std::string& parentName);

  VectorPtr createVector(
      const TypePtr& type,
      size_t size,
      const std::vector<clp_s::BaseColumnReader*>& projectedColumns,
      const std::vector<size_t>& filteredRows,
      size_t& readerIndex);

  std::string archiveSource_;
  std::string kqlQuery_;
  velox::memory::MemoryPool* pool_;
  RowTypePtr outputType_;
  std::set<std::string> columnUntypedNames_;
  std::map<std::string, size_t> columnIndices_;
  std::map<std::string, size_t> arrayOffsets_;
  uint64_t completedRows_{0};
  uint64_t completedBytes_{0};

  std::vector<search_lib::Field> fields_;

  std::unique_ptr<search_lib::ClpCursor> cursor_;
};
} // namespace facebook::velox::connector::clp
