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

#include <glog/logging.h>
#include <sstream>

#include "clp_s/search/ast/ConvertToExists.hpp"
#include "clp_s/search/ast/EmptyExpr.hpp"
#include "clp_s/search/ast/NarrowTypes.hpp"
#include "clp_s/search/ast/OrOfAndForm.hpp"
#include "clp_s/search/kql/kql.hpp"
#include "velox/connectors/clp/search_lib/BaseClpCursor.h"

using namespace clp_s;
using namespace clp_s::search;
using namespace clp_s::search::ast;

namespace facebook::velox::connector::clp::search_lib {

template <typename T>
auto estimatePrecision(T timestamp) -> TimestampPrecision {
  constexpr int64_t kEpochMilliseconds1971{31536000000};
  constexpr int64_t kEpochMicroseconds1971{31536000000000};
  constexpr int64_t kEpochNanoseconds1971{31536000000000000};
  auto absTimestamp = timestamp >= 0 ? timestamp : -timestamp;

  if (absTimestamp > kEpochNanoseconds1971) {
    return TimestampPrecision::Nanoseconds;
  } else if (absTimestamp > kEpochMicroseconds1971) {
    return TimestampPrecision::Microseconds;
  } else if (absTimestamp > kEpochMilliseconds1971) {
    return TimestampPrecision::Milliseconds;
  } else {
    return TimestampPrecision::Seconds;
  }
}

auto convertToVeloxTimestamp(double timestamp) -> Timestamp {
  switch (estimatePrecision(timestamp)) {
    case TimestampPrecision::Nanoseconds:
      timestamp /= Timestamp::kNanosInSecond;
      break;
    case TimestampPrecision::Microseconds:
      timestamp /= Timestamp::kMicrosecondsInSecond;
      break;
    case TimestampPrecision::Milliseconds:
      timestamp /= Timestamp::kMillisecondsInSecond;
      break;
    case TimestampPrecision::Seconds:
      break;
  }
  double seconds{std::floor(timestamp)};
  double nanoseconds{(timestamp - seconds) * Timestamp::kNanosInSecond};
  return Timestamp(
      static_cast<int64_t>(seconds), static_cast<uint64_t>(nanoseconds));
}

auto convertToVeloxTimestamp(int64_t timestamp) -> Timestamp {
  int64_t precisionDifference{Timestamp::kNanosInSecond};
  switch (estimatePrecision(timestamp)) {
    case TimestampPrecision::Nanoseconds:
      break;
    case TimestampPrecision::Microseconds:
      precisionDifference =
          Timestamp::kNanosInSecond / Timestamp::kNanosecondsInMicrosecond;
      break;
    case TimestampPrecision::Milliseconds:
      precisionDifference =
          Timestamp::kNanosInSecond / Timestamp::kNanosecondsInMillisecond;
      break;
    case TimestampPrecision::Seconds:
      precisionDifference =
          Timestamp::kNanosInSecond / Timestamp::kNanosInSecond;
      break;
  }
  int64_t seconds{timestamp / precisionDifference};
  int64_t nanoseconds{
      (timestamp % precisionDifference) *
      (Timestamp::kNanosInSecond / precisionDifference)};
  if (nanoseconds < 0) {
    seconds -= 1;
    nanoseconds += Timestamp::kNanosInSecond;
  }
  return Timestamp(seconds, static_cast<uint64_t>(nanoseconds));
}

void BaseClpCursor::executeQuery(
    const std::string& query,
    const std::vector<Field>& outputColumns) {
  query_ = query;
  outputColumns_ = outputColumns;
  errorCode_ = preprocessQuery();
}

ErrorCode BaseClpCursor::preprocessQuery() {
  auto queryStream = std::istringstream(query_);
  expr_ = kql::parse_kql_expression(queryStream);
  if (nullptr == expr_) {
    VLOG(2) << "Failed to parse query '" << query_ << "'";
    return ErrorCode::InvalidQuerySyntax;
  }

  if (std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    VLOG(2) << "Query '" << query_ << "' is logically false";
    return ErrorCode::LogicalError;
  }

  OrOfAndForm standardizePass;
  if (expr_ = standardizePass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    VLOG(2) << "Query '" << query_ << "' is logically false";
    return ErrorCode::LogicalError;
  }

  NarrowTypes narrowPass;
  if (expr_ = narrowPass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    VLOG(2) << "Query '" << query_ << "' is logically false";
    return ErrorCode::LogicalError;
  }

  ConvertToExists convertPass;
  if (expr_ = convertPass.run(expr_);
      std::dynamic_pointer_cast<EmptyExpr>(expr_)) {
    VLOG(2) << "Query '" << query_ << "' is logically false";
    return ErrorCode::LogicalError;
  }

  return ErrorCode::Success;
}

} // namespace facebook::velox::connector::clp::search_lib
