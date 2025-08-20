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

#include "velox/connectors/clp/search_lib/BaseClpCursor.h"

namespace facebook::velox::connector::clp::search_lib {

class ClpArchiveCursor final : public BaseClpCursor {
 public:
  explicit ClpArchiveCursor(
      clp_s::InputSource inputSource,
      std::string_view splitPath);
  ~ClpArchiveCursor() override;

  uint64_t fetchNext(
      uint64_t numRows,
      const std::shared_ptr<std::vector<uint64_t>>& filteredRowIndices)
      override;

  const std::vector<clp_s::BaseColumnReader*>& getProjectedColumns()
      const override;

 protected:
  ErrorCode loadSplit() override;

 private:
  std::vector<int32_t> matchedSchemas_;
  size_t currentSchemaIndex_{0};
  int32_t currentSchemaId_{-1};
  bool currentSchemaTableLoaded_{false};

  std::shared_ptr<clp_s::search::SchemaMatch> schemaMatch_;
  std::shared_ptr<ClpQueryRunner> queryRunner_;
  std::shared_ptr<clp_s::search::Projection> projection_;

  std::shared_ptr<clp_s::ArchiveReader> archiveReader_;
};

} // namespace facebook::velox::connector::clp::search_lib
