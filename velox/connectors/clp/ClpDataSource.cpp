#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <iostream>
#include <optional>

#include "velox/connectors/clp/ClpColumnHandle.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include "velox/connectors/clp/ClpDataSource.h"

#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/connectors/clp/search_lib/ClpVectorLoader.h"
#include "velox/connectors/clp/search_lib/ClpCursor.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::clp {

ClpDataSource::ClpDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const ClpConfig>& clpConfig)
    : pool_(pool), outputType_(outputType) {
  polymorphicTypeEnabled_ = clpConfig->polymorphicTypeEnabled();
  inputSource_ = clpConfig->inputSource();
  boost::algorithm::to_lower(inputSource_);
  auto clpTableHandle = std::dynamic_pointer_cast<ClpTableHandle>(tableHandle);
  if ("local" != inputSource_ && "s3" != inputSource_) {
    VELOX_USER_FAIL("Illegal input source: {}", inputSource_);
  }

  auto query = clpTableHandle->query();
  if (query && !query->empty()) {
    kqlQuery_ = *query;
  } else {
    kqlQuery_ = "*";
  }

  for (const auto& outputName : outputType->names()) {
    auto columnHandle = columnHandles.find(outputName);
    VELOX_CHECK(
        columnHandle != columnHandles.end(),
        "ColumnHandle not found for output name: {}",
        outputName);
    auto clpColumnHandle =
        std::dynamic_pointer_cast<ClpColumnHandle>(columnHandle->second);
    VELOX_CHECK_NOT_NULL(
        clpColumnHandle,
        "ColumnHandle must be an instance of ClpColumnHandle for output name: {}",
        outputName);
    auto columnName = clpColumnHandle->originalColumnName();
    auto columnType = clpColumnHandle->columnType();
    addFieldsRecursively(columnType, columnName);
  }
}

void ClpDataSource::addFieldsRecursively(
    const TypePtr& columnType,
    const std::string& parentName) {
  if (columnType->kind() == TypeKind::ROW) {
    const auto& rowType = columnType->asRow();
    for (size_t i = 0; i < rowType.size(); ++i) {
      const auto& childType = rowType.childAt(i);
      const auto childName = parentName + "." + rowType.nameOf(i);
      addFieldsRecursively(childType, childName);
    }
  } else {
    search_lib::ColumnType clpColumnType = search_lib::ColumnType::Unknown;
    switch (columnType->kind()) {
      case TypeKind::BOOLEAN:
        clpColumnType = search_lib::ColumnType::Boolean;
        break;
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::TINYINT:
        clpColumnType = search_lib::ColumnType::Integer;
        break;
      case TypeKind::DOUBLE:
      case TypeKind::REAL:
        clpColumnType = search_lib::ColumnType::Float;
        break;
      case TypeKind::VARCHAR:
        clpColumnType = search_lib::ColumnType::String;
        break;
      case TypeKind::ARRAY:
        clpColumnType = search_lib::ColumnType::Array;
        break;
      default:
        VELOX_USER_FAIL("Type not supported: {}", columnType->name());
    }
    fields_.emplace_back(search_lib::Field{clpColumnType, parentName});
  }
}

void ClpDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto clpSplit = std::dynamic_pointer_cast<ClpConnectorSplit>(split);

  if (inputSource_ == "local") {
    cursor_ = std::make_unique<search_lib::ClpCursor>(
        clp_s::InputSource::Filesystem,
        clpSplit->archivePath_,
        false);
  } else if (inputSource_ == "s3") {
    cursor_ = std::make_unique<search_lib::ClpCursor>(
        clp_s::InputSource::Network,
        clpSplit->archivePath_,
        false);
  }

  cursor_->executeQuery(kqlQuery_, fields_);
  projectedColumns_ = cursor->getProjectedColumns();
}

VectorPtr ClpDataSource::createVector(
    const TypePtr& type,
    size_t size,
    const std::vector<size_t>& filteredRows,
    size_t& readerIndex) {
  if (type->kind() == TypeKind::ROW) {
    std::vector<VectorPtr> children;
    auto& rowType = type->as<TypeKind::ROW>();
    // Children are reserved the parent size and accessible for those rows.
    for (int32_t i = 0; i < rowType.size(); ++i) {
      children.push_back(
          createVector(rowType.childAt(i), size, filteredRows, readerIndex));
    }
    return std::make_shared<RowVector>(
        pool_, type, nullptr, size, std::move(children));
  }
  auto vector = BaseVector::create(type, size, pool_);
  vector->setNulls(allocateNulls(size, pool_, bits::kNull));
  auto projectedColumn = projectedColumns_[readerIndex];
  auto projectedType = fields_[readerIndex].type;
  readerIndex++;
  return std::make_shared<LazyVector>(
      pool_,
      type,
      size,
      std::make_unique<search_lib::ClpVectorLoader>(
          projectedColumn, projectedType, filteredRows),
      std::move(vector));
}

std::optional<RowVectorPtr> ClpDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  std::vector<size_t> filteredRows;
  auto errorCode= cursor_->fetch_next(size, filteredRows);
  if (errorCode != search_lib::ErrorCode::Success) {
    SPD
  }
  auto rowsFetched = filteredRows.size();
  if (rowsFetched == 0) {
    return nullptr;
  }
  completedRows_ += rowsFetched;
  size_t readerIndex = 0;
  return std::dynamic_pointer_cast<RowVector>(
      createVector(outputType_, rowsFetched, filteredRows, readerIndex));
}

} // namespace facebook::velox::connector::clp
