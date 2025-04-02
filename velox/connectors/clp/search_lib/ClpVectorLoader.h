#pragma once

#include "clp_s/SchemaReader.hpp"
#include "clp_s/search/Expression.hpp"
#include "clp_s/search/Output.hpp"
#include "clp_s/search/Projection.hpp"
#include "clp_s/search/SchemaMatch.hpp"
#include "clp_s/search/clp_search/Query.hpp"
#include "clp_src/components/core/src/clp_s/SchemaTree.hpp"
#include "clp_src/components/core/src/clp_s/search/AddTimestampConditions.hpp"

#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::clp {
class ClpVectorLoader : public VectorLoader {
 public:
  ClpVectorLoader(
      BaseColumnReader* columnReader,
      clp_s::NodeType nodeType,
      TypePtr outputType,
      std::vector<size_t>& filteredRows,
      pool)
      : columnReader_(columnReader),
        nodeType_(nodeType),
        outputType_(outputType),
        filteredRows_(filteredRows) {}

 private:
  template <typename T>
  void populateData(RowSet rows, VectorPtr vector) {
    for (size_t i = 0; i < rows.size(); ++i) {
      auto vector_index = rows.at(i);
      auto message_index = filteredRows_[vector_index];
      vector->set(
          vector_index,
          std::get<T>(columnReader_->extract_value(message_index)));
      vector->setNull(vector_index, false);
    }
  }

  void loadInternal(
      RowSet rows,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override {
    *result = BaseVector::create(outputType_, size, pool_);
    *result->setNulls(nulls);
    switch (nodeType_) {
      case NodeType::Integer: {
        auto int_vector = *result->asFlatVector<int64_t>();
        populateData<int64_t>(rows, int_vector);
        break;
      }
      case NodeType::Float: {
        auto float_vector = *result->asFlatVector<double>();
        populateData<double>(rows, float_vector);
        break;
      }
      case NodeType::Boolean: {
        auto bool_vector = *result->asFlatVector<bool>();
        populateData<uint8_t>(rows, bool_vector);
        break;
      }
      case NodeType::ClpString:
      case NodeType::VarString: {
        auto string_vector =
            *result->asFlatVector<facebook::velox::StringView>();
          populateData<std::string>(rows, string_vector);
        break;
      }
      case NodeType::UnstructuredArray: {
        auto array_vector =
            std::dynamic_pointer_cast<facebook::velox::ArrayVector>(
                *result);
        auto vector_index = rows.at(i);
        auto message_index = filteredRows_[vector_index];
        if (vector_index == 0) {
          m_array_offsets[i] = 0;
        }

        auto array_begin_offset = m_array_offsets[i];
        auto array_end_offset = array_begin_offset;
        auto elements = array_vector->elements()
                            ->asFlatVector<facebook::velox::StringView>();
        std::vector<std::string_view> arrayElements;
        auto json_string = std::get<std::string>(
            m_projected_columns[i]->extract_value(message_index));
        auto obj = m_array_parser.iterate(json_string);

        for (auto arrayElement : obj.get_array()) {
          // Get each array element as a string
          auto elementStringWithQuotes =
              simdjson::to_json_string(arrayElement).value();
          auto elementString = elementStringWithQuotes.substr(
              1, elementStringWithQuotes.size() - 2);
          arrayElements.emplace_back(elementString);
        }
        elements->resize(array_end_offset + arrayElements.size());

        for (auto& arrayElement : arrayElements) {
          // Set the element in the array vector
          elements->set(
              array_end_offset++, facebook::velox::StringView(arrayElement));
        }
        m_array_offsets[i] = array_end_offset;
        array_vector->setOffsetAndSize(
            vector_index,
            array_begin_offset,
            array_end_offset - array_begin_offset);
        array_vector->setNull(vector_index, false);
        break;
      }
      case NodeType::NullValue:
      case NodeType::Unknown:
      default:
        column_vectors[i]->setNull(vector_index, true);
        break;
    }
  }

  clp_s::BaseColumnReader* columnReader_;
  clp_s::NodeType nodeType_;
  std::vector<size_t>& filteredRows_;
};
} // namespace facebook::velox::connector::clp