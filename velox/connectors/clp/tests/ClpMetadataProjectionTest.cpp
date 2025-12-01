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

#include <gtest/gtest.h>

#include "velox/common/base/Fs.h"
#include "velox/connectors/clp/ClpColumnHandle.h"
#include "velox/connectors/clp/ClpConnector.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::clp;
using facebook::velox::exec::test::PlanBuilder;

class ClpMetadataProjectionTest : public exec::test::OperatorTestBase {
 public:
  const std::string kClpConnectorId = "test-clp";

  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::registerConnectorFactory(
        std::make_shared<connector::clp::ClpConnectorFactory>());
    auto clpConnector =
        connector::getConnectorFactory(
            connector::clp::ClpConnectorFactory::kClpConnectorName)
            ->newConnector(
                kClpConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>{}));
    connector::registerConnector(clpConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kClpConnectorId);
    connector::unregisterConnectorFactory(
        connector::clp::ClpConnectorFactory::kClpConnectorName);
    OperatorTestBase::TearDown();
  }

  /// Creates a CLP split with metadata column values for testing metadata
  /// projection.
  exec::Split makeClpSplitWithMetadata(
      const std::string& splitPath,
      ClpConnectorSplit::SplitType type,
      std::shared_ptr<std::string> kqlQuery,
      std::map<std::string, MetadataValueType> metadataValues) {
    auto metadataMap =
        std::make_shared<std::map<std::string, MetadataValueType>>(
            std::move(metadataValues));
    return exec::Split(std::make_shared<ClpConnectorSplit>(
        kClpConnectorId,
        splitPath,
        static_cast<int>(type),
        kqlQuery,
        metadataMap));
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }

  static std::string getExampleFilePath(const std::string& filePath) {
    std::string current_path = fs::current_path().string();
    return current_path + "/examples/" + filePath;
  }
};

/**
 * Tests metadata projection by injecting constant values for columns that are
 * pre-fetched from the metadata database. This test validates that:
 * 1. String metadata values are correctly projected as constant vectors
 * 2. Integer metadata values are correctly projected as constant vectors
 * 3. Double metadata values are correctly projected as constant vectors
 * 4. Metadata projection works with IR split type
 * 5. Metadata columns are combined correctly with regular data columns
 */
TEST_F(ClpMetadataProjectionTest, metadataProjection) {
  const std::shared_ptr<std::string> kqlQuery = nullptr;

  // Define metadata values of different types
  std::map<std::string, MetadataValueType> metadataValues = {
      {"source_file", std::string("/var/log/app.log")},
      {"partition_id", static_cast<int64_t>(42)},
      {"sampling_rate", 0.75}};

  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW(
                      {"requestId",
                       "method",
                       "source_file",
                       "partition_id",
                       "sampling_rate"},
                      {VARCHAR(), VARCHAR(), VARCHAR(), BIGINT(), DOUBLE()}))
                  .tableHandle(std::make_shared<ClpTableHandle>(
                      kClpConnectorId, "test_1"))
                  .assignments(
                      {{"requestId",
                        std::make_shared<ClpColumnHandle>(
                            "requestId", "requestId", VARCHAR())},
                       {"method",
                        std::make_shared<ClpColumnHandle>(
                            "method", "method", VARCHAR())},
                       {"source_file",
                        std::make_shared<ClpColumnHandle>(
                            "source_file", "source_file", VARCHAR())},
                       {"partition_id",
                        std::make_shared<ClpColumnHandle>(
                            "partition_id", "partition_id", BIGINT())},
                       {"sampling_rate",
                        std::make_shared<ClpColumnHandle>(
                            "sampling_rate", "sampling_rate", DOUBLE())}})
                  .endTableScan()
                  .planNode();

  auto output = getResults(
      plan,
      {makeClpSplitWithMetadata(
          getExampleFilePath("test_1_ir.clp.zst"),
          ClpConnectorSplit::SplitType::kIr,
          kqlQuery,
          metadataValues)});

  auto expected = makeRowVector(
      {// requestId (from data)
       makeFlatVector<StringView>({
           "req-100",
           "req-101",
           "req-102",
           "req-103",
           "req-104",
           "req-105",
           "req-106",
           "req-107",
           "req-108",
           "req-109",
       }),
       // method (from data)
       makeFlatVector<StringView>({
           "GET",
           "POST",
           "GET",
           "PUT",
           "DELETE",
           "GET",
           "POST",
           "GET",
           "PATCH",
           "GET",
       }),
       // source_file (metadata - string constant)
       makeFlatVector<StringView>(
           10, [](auto /* row */) { return "/var/log/app.log"; }),
       // partition_id (metadata - int64 constant)
       makeFlatVector<int64_t>(10, [](auto /* row */) { return 42; }),
       // sampling_rate (metadata - double constant)
       makeFlatVector<double>(10, [](auto /* row */) { return 0.75; })});

  test::assertEqualVectors(expected, output);
}

/**
 * Tests that metadata columns take precedence over data columns when a column
 * name exists in both sources.
 */
TEST_F(ClpMetadataProjectionTest, metadataProjectionPrecedence) {
  const std::shared_ptr<std::string> kqlQuery = nullptr;

  // Define metadata value for "method" column which also exists in the data.
  // In the data, method has values like "GET", "POST", "PUT", etc.
  // We override it with a constant metadata value.
  std::map<std::string, MetadataValueType> metadataValues = {
      {"method", std::string("METADATA_OVERRIDE")}};

  auto plan =
      PlanBuilder()
          .startTableScan()
          .outputType(ROW({"requestId", "method"}, {VARCHAR(), VARCHAR()}))
          .tableHandle(
              std::make_shared<ClpTableHandle>(kClpConnectorId, "test_1"))
          .assignments(
              {{"requestId",
                std::make_shared<ClpColumnHandle>(
                    "requestId", "requestId", VARCHAR())},
               {"method",
                std::make_shared<ClpColumnHandle>(
                    "method", "method", VARCHAR())}})
          .endTableScan()
          .planNode();

  auto output = getResults(
      plan,
      {makeClpSplitWithMetadata(
          getExampleFilePath("test_1_ir.clp.zst"),
          ClpConnectorSplit::SplitType::kIr,
          kqlQuery,
          metadataValues)});

  // Expected: method column should have the metadata value "METADATA_OVERRIDE"
  // for all rows, NOT the actual data values ("GET", "POST", etc.)
  auto expected =
      makeRowVector({// requestId (from data)
                     makeFlatVector<StringView>({
                         "req-100",
                         "req-101",
                         "req-102",
                         "req-103",
                         "req-104",
                         "req-105",
                         "req-106",
                         "req-107",
                         "req-108",
                         "req-109",
                     }),
                     // method (from metadata - overrides data values)
                     makeFlatVector<StringView>(10, [](auto /* row */) {
                       return "METADATA_OVERRIDE";
                     })});

  test::assertEqualVectors(expected, output);
}

} // namespace
