#pragma once

#include "clp_s/SchemaReader.hpp"
#include "velox/dwio/common/DirectDecoder.h"
#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/exec/AggregationHook.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::clp::search_lib {
class ClpVectorLoader : public VectorLoader {
 public:
  ClpVectorLoader(
      clp_s::BaseColumnReader* columnReader,
      ColumnType nodeType,
      const std::vector<size_t>& filteredRows)
      : columnReader_(columnReader),
        nodeType_(nodeType),
        filteredRows_(filteredRows) {}

 private:
  template <typename T, typename VectorPtr>
  void populateData(RowSet rows, VectorPtr vector) {
    for (size_t i = 0; i < rows.size(); ++i) {
      auto vectorIndex = rows.at(i);
      auto messageIndex = filteredRows_[vectorIndex];

      if constexpr (std::is_same_v<T, std::string>) {
        auto string_value =
            std::get<std::string>(columnReader_->extract_value(messageIndex));
        vector->set(vectorIndex, facebook::velox::StringView(string_value));
      } else {
        vector->set(
            vectorIndex,
            std::get<T>(columnReader_->extract_value(messageIndex)));
      }

      vector->setNull(vectorIndex, false);
    }
  }

  void loadInternal(
      RowSet rows,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override {
    if (!result) {
      VELOX_USER_FAIL("vector is null");
    }
    auto vector = *result;
    switch (nodeType_) {
      case ColumnType::Integer: {
        auto intVector = vector->asFlatVector<int64_t>();
        populateData<int64_t>(rows, intVector);
        break;
      }
      case ColumnType::Float: {
        auto floatVector = vector->asFlatVector<double>();
        populateData<double>(rows, floatVector);
        break;
      }
      case ColumnType::Boolean: {
        auto boolVector = vector->asFlatVector<bool>();
        populateData<uint8_t>(rows, boolVector);
        break;
      }
      case ColumnType::String: {
        auto stringVector = vector->asFlatVector<StringView>();
        populateData<std::string>(rows, stringVector);
        break;
      }
      case ColumnType::Array: {
        auto arrayVector = std::dynamic_pointer_cast<ArrayVector>(vector);
        auto elements = arrayVector->elements()->asFlatVector<StringView>();
        auto* rawStrings = elements->mutableRawValues();
        vector_size_t elementIndex = 0;

        for (size_t i = 0; i < rows.size(); ++i) {
          auto vectorIndex = rows.at(i);
          auto messageIndex = filteredRows_[vectorIndex];

          auto jsonString = std::get<std::string>(
              columnReader_->extract_value(messageIndex));

          simdjson::padded_string padded(jsonString);
          simdjson::ondemand::document doc;
          try {
            doc = arrayParser_.iterate(padded);
          } catch (const simdjson::simdjson_error& e) {
            VELOX_FAIL("JSON parse error at row {}: {}", vectorIndex, e.what());
          }

          simdjson::ondemand::array array;
          try {
            array = doc.get_array();
          } catch (const simdjson::simdjson_error& e) {
            VELOX_FAIL("Expected JSON array at row {}: {}", vectorIndex, e.what());
          }

          std::vector<std::string_view> arrayElements;
          for (auto arrayElement : array) {
            arrayElements.emplace_back(simdjson::to_json_string(arrayElement).value());
          }

          if (elementIndex + arrayElements.size() > elements->size()) {
            size_t newSize = std::max<size_t>(
                elementIndex + arrayElements.size(),
                static_cast<size_t>(elements->size()) * 2);
            elements->resize(newSize);
          }

          arrayVector->setOffsetAndSize(vectorIndex, elementIndex, arrayElements.size());
          for (auto& arrayElement : arrayElements) {
            elements->set(
                elementIndex++, StringView(arrayElement));
          }
          arrayVector->setNull(vectorIndex, false);
        }
        break;
      }
      default:
        break;
    }
  }

  clp_s::BaseColumnReader* columnReader_;
  ColumnType nodeType_;
  const std::vector<size_t>& filteredRows_;
  inline static thread_local simdjson::ondemand::parser arrayParser_;
};
} // namespace facebook::velox::connector::clp::search_lib