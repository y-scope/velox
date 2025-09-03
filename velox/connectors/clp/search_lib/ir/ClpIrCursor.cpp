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

#include "velox/connectors/clp/search_lib/ir/ClpIrCursor.h"
#include "ffi/ir_stream/search/QueryHandler.hpp"

#include "clp_s/ColumnReader.hpp"
#include "clp_s/InputConfig.hpp"

using namespace clp_s;

namespace facebook::velox::connector::clp::search_lib {

uint64_t ClpIrCursor::fetchNext(uint64_t numRows) {
  if (ErrorCode::Success != errorCode_) {
    return 0;
  }

  if (false == currentSplitLoaded_) {
    errorCode_ = loadSplit();
    if (ErrorCode::Success != errorCode_) {
      return 0;
    }
  }

  auto deserializeResult = deserialize();
  if (ystdlib::error_handling::success() != deserializeResult) {
    VELOX_FAIL(
        "IR file {} might be broken, failed to deserialize", this->splitPath_);
  }
  return irDeserializer_->get_ir_unit_handler().getFilteredLogEvents()->size();
}

size_t ClpIrCursor::getNumFilteredRows() {
  return irDeserializer_->get_ir_unit_handler().getFilteredLogEvents()->size();
}

VectorPtr ClpIrCursor::createVector(
    memory::MemoryPool* pool,
    const TypePtr& vectorType,
    size_t vectorSize) {
  return nullptr;
}

ystdlib::error_handling::Result<void> ClpIrCursor::deserialize() const {
  while (::clp::ffi::ir_stream::IrUnitType::EndOfStream !=
         YSTDLIB_ERROR_HANDLING_TRYX(
             irDeserializer_->deserialize_next_ir_unit(*irReader_))) {
  }
  return ystdlib::error_handling::success();
}

ErrorCode ClpIrCursor::loadSplit() {
  auto networkAuthOption = inputSource_ == InputSource::Filesystem
      ? NetworkAuthOption{.method = AuthMethod::None}
      : NetworkAuthOption{.method = AuthMethod::S3PresignedUrlV4};

  auto irHandler{ClpIrUnitHandler{}};

  auto projections = splitFieldsToNamesAndTypes();
  auto queryHandlerResult{ir::QueryHandlerType::create(
      ir::handleProjectionResolution,
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
      *irReader_, irHandler, std::move(queryHandler));
  if (!deserializerResult) {
    VLOG(2) << "Failed to create deserializer for deserialization.";
    return ErrorCode::InternalError;
  }
  irDeserializer_ = std::make_shared<::clp::ffi::ir_stream::Deserializer<
      ClpIrUnitHandler,
      ir::QueryHandlerType>>(std::move(deserializerResult).value());

  return ErrorCode::Success;
}

std::vector<std::pair<std::string, clp_s::search::ast::literal_type_bitmask_t>>
ClpIrCursor::splitFieldsToNamesAndTypes() const {
  auto result = std::vector<
      std::pair<std::string, clp_s::search::ast::literal_type_bitmask_t>>{};
  for (size_t i{0}; i < outputColumns_.size(); ++i) {
    auto column = outputColumns_[i];
    clp_s::search::ast::literal_type_bitmask_t literalType;
    switch (column.type) {
      case ColumnType::Array:
        literalType = clp_s::search::ast::LiteralType::ArrayT;
        break;
      case ColumnType::Boolean:
        literalType = clp_s::search::ast::LiteralType::BooleanT;
        break;
      case ColumnType::Float:
        literalType = clp_s::search::ast::LiteralType::FloatT;
        break;
      case ColumnType::Integer:
        literalType = clp_s::search::ast::LiteralType::IntegerT;
        break;
      case ColumnType::String:
        literalType = clp_s::search::ast::LiteralType::VarStringT;
        break;
      case ColumnType::Timestamp:
        literalType = clp_s::search::ast::LiteralType::EpochDateT;
        break;
      default:
        literalType = clp_s::search::ast::LiteralType::UnknownT;
        break;
    }
    result.emplace_back(column.name, literalType);
  }
  return result;
}

} // namespace facebook::velox::connector::clp::search_lib
