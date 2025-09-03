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

#include "velox/connectors/clp/search_lib/ir/ClpIrVectorLoader.h"
#include "velox/connectors/clp/search_lib/BaseClpCursor.h"

namespace facebook::velox::connector::clp::search_lib {

void ClpIrVectorLoader::loadInternal(
    RowSet rows,
    ValueHook* hook,
    vector_size_t resultSize,
    VectorPtr* result) {
  auto vector = *result;
  for (int vectorIndex : rows) {
    filteredLogEvents_->at(vectorIndex)->
  }
  switch (nodeType_) {
    case ColumnType::Integer: {
      auto intVector = vector->asFlatVector<int64_t>();
    }
    case ColumnType::Float: {
    }
  }
}

} // namespace facebook::velox::connector::clp::search_lib
