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

#include "ffi/ir_stream/Deserializer.hpp"
#include "streaming_compression/Decompressor.hpp"
#include "velox/connectors/clp/search_lib/BaseClpCursor.h"
#include "velox/connectors/clp/search_lib/ir/ClpIrQueryHandler.h"
#include "velox/connectors/clp/search_lib/ir/ClpIrUnitHandler.h"

namespace facebook::velox::connector::clp::search_lib {

class ClpIrCursor final : public BaseClpCursor {
 public:
  explicit ClpIrCursor(
      clp_s::InputSource inputSource,
      std::string_view splitPath,
      bool ignoreCase)
      : BaseClpCursor(inputSource, splitPath), ignoreCase_(ignoreCase) {}

  uint64_t fetchNext(
      uint64_t numRows,
      const std::shared_ptr<std::vector<uint64_t>>& filteredRowIndices)
      override;

  VectorPtr createVector(
      memory::MemoryPool* pool,
      const TypePtr& vectorType,
      size_t vectorSize,
      const std::shared_ptr<std::vector<uint64_t>>& filteredRows,
      size_t& readerIndex) override;

 protected:
  ErrorCode loadSplit() override;

 private:
  std::shared_ptr<::clp::ReaderInterface> irReader_{nullptr};
  bool ignoreCase_;
  std::shared_ptr<::clp::ffi::ir_stream::
                      Deserializer<ClpIrUnitHandler, ir::QueryHandlerType>>
      irDeserializer_;

  ystdlib::error_handling::Result<void> deserialize() const;

  std::vector<
      std::pair<std::string, clp_s::search::ast::literal_type_bitmask_t>>
  splitFieldsToNamesAndTypes() const;
};

} // namespace facebook::velox::connector::clp::search_lib
