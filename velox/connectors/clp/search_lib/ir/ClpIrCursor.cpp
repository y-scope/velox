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

#include "clp_s/ColumnReader.hpp"
#include "clp_s/InputConfig.hpp"

#include "ffi/ir_stream/search/QueryHandler.hpp"
#include "velox/connectors/clp/search_lib/ir/ClpIrCursor.h"
#include "velox/connectors/clp/search_lib/ir/ClpIrVectorLoader.h"

using namespace clp_s;

namespace facebook::velox::connector::clp::search_lib {

uint64_t ClpIrCursor::fetchNext(uint64_t numRows) {
  readerIndex_ = 0;
  if (ErrorCode::Success != errorCode_) {
    return 0;
  }

  if (false == currentSplitLoaded_) {
    errorCode_ = loadSplit();
    if (ErrorCode::Success != errorCode_) {
      return 0;
    }
  }

  auto deserializeResult = deserialize(numRows);
  if (deserializeResult.has_error()) {
    auto error = deserializeResult.error();
    VELOX_FAIL(
        "IR file {} might be broken, failed to deserialize. {}: {}",
        this->splitPath_,
        error.category().name(),
        error.message());
  }
  return irDeserializer_->get_num_log_events_deserialized();
}

size_t ClpIrCursor::getNumFilteredRows() const {
  return irDeserializer_->get_ir_unit_handler().getFilteredLogEvents()->size();
}

VectorPtr ClpIrCursor::createVector(
    memory::MemoryPool* pool,
    const TypePtr& vectorType,
    size_t vectorSize) {
  VELOX_CHECK_EQ(
      projectedColumnIdxNodeIdMap_.size(),
      outputColumns_.size(),
      "Projected columns size {} does not match fields size {}",
      projectedColumnIdxNodeIdMap_.size(),
      outputColumns_.size());
  return createVectorHelper(pool, vectorType, vectorSize);
}

ErrorCode ClpIrCursor::loadSplit() {
  auto networkAuthOption = inputSource_ == InputSource::Filesystem
      ? NetworkAuthOption{.method = AuthMethod::None}
      : NetworkAuthOption{.method = AuthMethod::S3PresignedUrlV4};

  auto irHandler = ClpIrUnitHandler{};

  auto projections = splitFieldsToNamesAndTypes();
  auto queryHandlerResult{QueryHandlerType::create(
      projectionResolutionCallback_,
      std::move(expr_),
      projections,
      ignoreCase_)};
  if (!queryHandlerResult) {
    VLOG(2) << "Failed to create query handler for deserialization.";
    return ErrorCode::InternalError;
  }
  auto queryHandler = std::move(queryHandlerResult).value();

  auto irPath = Path{.source = inputSource_, .path = splitPath_};
  irReader_ = try_create_reader(irPath, networkAuthOption);
  if (nullptr == irReader_) {
    VLOG(2) << "Failed to open kv-ir stream \"" << splitPath_
            << "\" for reading.";
    return ErrorCode::InternalError;
  }

  auto deserializerResult = ::clp::ffi::ir_stream::make_deserializer(
      *irReader_, std::move(irHandler), std::move(queryHandler));
  if (!deserializerResult) {
    VLOG(2) << "Failed to create deserializer for deserialization.";
    return ErrorCode::InternalError;
  }
  irDeserializer_ = std::make_shared<
      ::clp::ffi::ir_stream::Deserializer<ClpIrUnitHandler, QueryHandlerType>>(
      std::move(deserializerResult).value());

  currentSplitLoaded_ = true;
  return ErrorCode::Success;
}

std::vector<std::pair<std::string, search::ast::literal_type_bitmask_t>>
ClpIrCursor::splitFieldsToNamesAndTypes() const {
  auto result = std::vector<
      std::pair<std::string, search::ast::literal_type_bitmask_t>>{};
  for (size_t i{0}; i < outputColumns_.size(); ++i) {
    auto column = outputColumns_[i];
    search::ast::literal_type_bitmask_t literalType;
    switch (column.type) {
      case ColumnType::Array:
        literalType = search::ast::LiteralType::ArrayT;
        break;
      case ColumnType::Boolean:
        literalType = search::ast::LiteralType::BooleanT;
        break;
      case ColumnType::Float:
        literalType = search::ast::LiteralType::FloatT;
        break;
      case ColumnType::Integer:
        literalType = search::ast::LiteralType::IntegerT;
        break;
      case ColumnType::String:
        literalType = search::ast::LiteralType::VarStringT |
            search::ast::LiteralType::ClpStringT;
        break;
      case ColumnType::Timestamp:
        // TODO: IR timestamp support pending; constrain to Unknown to avoid
        // mismatched projections.
        literalType = search::ast::LiteralType::EpochDateT;
        break;
      default:
        literalType = search::ast::LiteralType::UnknownT;
        break;
    }
    result.emplace_back(column.name, literalType);
  }
  return result;
}

ystdlib::error_handling::Result<void> ClpIrCursor::deserialize(
    uint64_t numRows) {
  irDeserializer_->get_ir_unit_handler().clearFilteredLogEvents();
  uint64_t cnt{0};
  while (cnt < numRows) {
    auto deserializeResult =
        irDeserializer_->deserialize_next_ir_unit(*irReader_);
    if (deserializeResult.has_error()) {
      auto error = deserializeResult.error();
      if (std::errc::result_out_of_range == error ||
          irDeserializer_->is_stream_completed()) {
        break;
      }
      return error;
    }
    if (::clp::ffi::ir_stream::IrUnitType::LogEvent ==
        deserializeResult.value()) {
      ++cnt;
    }
  }
  return ystdlib::error_handling::success();
}

VectorPtr ClpIrCursor::createVectorHelper(
    memory::MemoryPool* pool,
    const TypePtr& vectorType,
    size_t vectorSize) {
  if (vectorType->kind() == TypeKind::ROW) {
    std::vector<VectorPtr> children;
    auto& rowType = vectorType->as<TypeKind::ROW>();
    for (uint32_t i = 0; i < rowType.size(); ++i) {
      children.push_back(
          createVectorHelper(pool, rowType.childAt(i), vectorSize));
    }
    return std::make_shared<RowVector>(
        pool, vectorType, nullptr, vectorSize, std::move(children));
  }
  auto vector = BaseVector::create(vectorType, vectorSize, pool);
  vector->setNulls(allocateNulls(vectorSize, pool, bits::kNull));
  VELOX_CHECK_LT(
      readerIndex_, outputColumns_.size(), "Reader index out of bounds");
  auto projectedColumn = outputColumns_[readerIndex_];
  auto projectedColumnType = projectedColumn.type;
  auto it = projectedColumnIdxNodeIdMap_.find(readerIndex_);
  bool isResolved = it != projectedColumnIdxNodeIdMap_.end();
  ::clp::ffi::SchemaTree::Node::id_t projectedColumnNodeId;
  if (isResolved) {
    projectedColumnNodeId = it->second;
  }
  readerIndex_++;
  return std::make_shared<LazyVector>(
      pool,
      vectorType,
      vectorSize,
      std::make_unique<ClpIrVectorLoader>(
          isResolved,
          projectedColumnType,
          projectedColumnNodeId,
          irDeserializer_->get_ir_unit_handler().getFilteredLogEvents()),
      std::move(vector));
}

} // namespace facebook::velox::connector::clp::search_lib
