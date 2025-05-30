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

#define XXH_INLINE_ALL
#include <xxhash.h> // @manual=third-party//xxHash:xxhash

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ChecksumAggregate.h"
#include "velox/functions/prestosql/aggregates/PrestoHasher.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

/// Computes an order-insensitive checksum of the input
/// vector.
///
/// checksum(T)-> varbinary
class ChecksumAggregate : public exec::Aggregate {
 public:
  explicit ChecksumAggregate(const TypePtr& resultType)
      : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->asUnchecked<FlatVector<StringView>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    auto* rawValues = vector->mutableRawValues();
    vector->clearAllNulls();
    for (auto i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        rawValues[i] = StringView(value<char>(group), sizeof(int64_t));
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vector = (*result)->asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    auto* rawValues = vector->mutableRawValues();
    vector->clearAllNulls();
    for (auto i = 0; i < numGroups; ++i) {
      auto group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        rawValues[i] = *value<int64_t>(group);
      }
    }
  }

  bool isNullOrNullArray(const TypePtr& type) {
    if (type->isUnKnown()) {
      return true;
    }
    // Only supports null array type for now.
    if (type->kind() == TypeKind::ARRAY) {
      return type->asArray().elementType()->isUnKnown();
    }
    return false;
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    const auto& arg = args[0];

    if (isNullOrNullArray(arg->type())) {
      rows.applyToSelected([&](auto row) {
        auto group = groups[row];
        clearNull(group);
        if (arg->isNullAt(row)) {
          computeHashForNull(group);
        } else {
          computeHashForEmptyNullArray(group);
        }
      });
      return;
    }

    auto hasher = getPrestoHasher(arg->type());
    auto hashes = getHashBuffer(rows.end(), arg->pool());
    hasher->hash(arg, rows, hashes);
    auto rawHashes = hashes->as<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      clearNull(group);
      if (arg->isNullAt(row)) {
        computeHashForNull(group);
      } else {
        computeHash(group, rawHashes[row]);
      }
    });
  }

#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
  FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
  FOLLY_ALWAYS_INLINE void safeAdd(int64_t& lhs, const int64_t& rhs) {
    lhs += rhs;
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    decodedIntermediate_.decode(*args[0], rows);

    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      if (!decodedIntermediate_.isNullAt(row)) {
        clearNull(group);
        safeAdd(
            *value<int64_t>(group), decodedIntermediate_.valueAt<int64_t>(row));
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    const auto& arg = args[0];

    if (isNullOrNullArray(arg->type())) {
      rows.applyToSelected([&](auto row) {
        clearNull(group);
        if (arg->isNullAt(row)) {
          computeHashForNull(group);
        } else {
          computeHashForEmptyNullArray(group);
        }
      });

      return;
    }

    auto hasher = getPrestoHasher(arg->type());
    auto hashes = getHashBuffer(rows.end(), arg->pool());
    hasher->hash(arg, rows, hashes);
    auto rawHashes = hashes->as<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      clearNull(group);
      if (arg->isNullAt(row)) {
        computeHashForNull(group);
      } else {
        computeHash(group, rawHashes[row]);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushDown*/) override {
    decodedIntermediate_.decode(*args[0], rows);

    int64_t result = 0;
    bool clearGroupNull = false;
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        safeAdd(result, decodedIntermediate_.valueAt<int64_t>(row));
        clearGroupNull = true;
      }
    });
    if (clearGroupNull) {
      clearNull(group);
    }
    safeAdd(*value<int64_t>(group), result);
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      *value<int64_t>(groups[i]) = 0;
    }
  }

 private:
  FOLLY_ALWAYS_INLINE void computeHash(char* group, const int64_t hash) {
    *value<int64_t>(group) += hash * XXH_PRIME64_1;
  }

  FOLLY_ALWAYS_INLINE void computeHashForNull(char* group) {
    *value<int64_t>(group) += XXH_PRIME64_1;
  }

  FOLLY_ALWAYS_INLINE void computeHashForEmptyNullArray(char* group) {
    *value<int64_t>(group) += 0;
  }

  FOLLY_ALWAYS_INLINE PrestoHasher* getPrestoHasher(TypePtr typePtr) {
    if (prestoHasher_ == nullptr) {
      prestoHasher_ = std::make_unique<PrestoHasher>(std::move(typePtr));
    }
    return prestoHasher_.get();
  }

  FOLLY_ALWAYS_INLINE BufferPtr& getHashBuffer(
      vector_size_t size,
      velox::memory::MemoryPool* pool) {
    // hashes_->size() is in bytes.
    if (hashes_ == nullptr) {
      hashes_ = AlignedBuffer::allocate<int64_t>(size, pool);
    } else if (hashes_->size() < (size * sizeof(int64_t))) {
      AlignedBuffer::reallocate<int64_t>(&hashes_, size);
    }

    return hashes_;
  }

  std::unique_ptr<PrestoHasher> prestoHasher_;
  BufferPtr hashes_;
  DecodedVector decodedIntermediate_;
};

} // namespace

void registerChecksumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("varbinary")
          .intermediateType("bigint")
          .argumentType("T")
          .build(),
  };

  auto name = prefix + kChecksum;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [&name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes one argument", name);

        if (exec::isPartialOutput(step)) {
          return std::make_unique<ChecksumAggregate>(BIGINT());
        }

        return std::make_unique<ChecksumAggregate>(VARBINARY());
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
