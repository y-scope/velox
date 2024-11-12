#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <optional>

#include "velox/connectors/clp/ClpColumnHandle.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include "velox/connectors/clp/ClpDataSource.h"
#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/vector/FlatVector.h"

#include "search_lib/search/Cursor.hpp"

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
  executablePath_ = clpConfig->executablePath();
  VELOX_CHECK(!executablePath_.empty(), "Executable path must be set");
  polymorphicTypeEnabled_ = clpConfig->polymorphicTypeEnabled();
  auto archiveRootDir = clpConfig->archiveDir();
  VELOX_CHECK(!archiveRootDir.empty(), "Archive directory must be set");
  auto clpTableHandle = std::dynamic_pointer_cast<ClpTableHandle>(tableHandle);
  auto archiveDir =
      boost::filesystem::path(archiveRootDir) / clpTableHandle->tableName();
  archiveDir_ = archiveDir.string();
  auto query = clpTableHandle->query();
  if (query && !query->empty()) {
    kqlQuery_ = *query;
  } else {
    kqlQuery_ = "*";
  }

  static const std::vector<std::string> suffixes = {
      "_varchar", "_double", "_bigint", "_boolean"};
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
    auto columnName = clpColumnHandle->columnName();
    clp_s::search::ColumnType clpColumnType;
    switch (clpColumnHandle->columnType()->kind()) {
      case TypeKind::BOOLEAN:
        clpColumnType = clp_s::search::ColumnType::Boolean;
        break;
      case TypeKind::INTEGER:
      case TypeKind::BIGINT:
      case TypeKind::TINYINT:
        clpColumnType = clp_s::search::ColumnType::Integer;
        break;
      case TypeKind::ARRAY:
        clpColumnType = clp_s::search::ColumnType::Array;
        break;
      case TypeKind::DOUBLE:
      case TypeKind::REAL:
        clpColumnType = clp_s::search::ColumnType::Float;
        break;
      case TypeKind::VARCHAR:
        clpColumnType = clp_s::search::ColumnType::String;
        break;
      default:
        VELOX_USER_FAIL(
            "Type not supported: {}", clpColumnHandle->columnType()->name());
    }
    auto processedColumnName = columnName;
    if (polymorphicTypeEnabled_) {
      for (const auto& suffix : suffixes) {
        if (boost::algorithm::ends_with(columnName, suffix)) {
          // Strip the type suffix
          processedColumnName =
              columnName.substr(0, columnName.size() - suffix.size());
          break;
        }
      }
    }
    fields_.emplace_back(
        clp_s::search::Field{clpColumnType, processedColumnName});
  }
}

void ClpDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto clpSplit = std::dynamic_pointer_cast<ClpConnectorSplit>(split);
  auto tableName = clpSplit->tableName();
  auto archiveId = clpSplit->archiveId();
  VELOX_CHECK(!tableName.empty(), "Table name must be set");

  cursor_ = std::make_unique<clp_s::search::Cursor>(
      archiveDir_,
      clp_s::InputOption{.source = clp_s::InputSource::Filesystem},
      std::vector<std::string>{archiveId},
      false);
  cursor_->execute_query(kqlQuery_, fields_);
}

std::optional<RowVectorPtr> ClpDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  std::vector<VectorPtr> vectors;
  vectors.reserve(outputType_->size());
  auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNull);
  for (const auto& childType : outputType_->children()) {
    // Create a vector with NULL values
    auto vector = BaseVector::create(childType, size, pool_);
    vector->setNulls(nulls);
    vectors.emplace_back(vector);
  }

  size_t rowsFetched = cursor_->fetch_next(size, vectors);
  if (rowsFetched == 0) {
    return nullptr;
  }

  completedRows_ += rowsFetched;
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), rowsFetched, std::move(vectors));
}
} // namespace facebook::velox::connector::clp
