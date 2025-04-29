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

#include "velox/connectors/clp/ClpConnector.h"
#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include <folly/init/Init.h>
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::clp;

using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::tpch::Table;

class ClpConnectorTest : public exec::test::OperatorTestBase {
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
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(clpConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kClpConnectorId);
    connector::unregisterConnectorFactory(
        connector::clp::ClpConnectorFactory::kClpConnectorName);
    OperatorTestBase::TearDown();
  }

  exec::Split makeClpSplit(const std::string & tableName, const std::string & archivePath) {
    return exec::Split(std::make_shared<ClpConnectorSplit>(
        kClpConnectorId, "default", tableName, archivePath));
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }

  void runScaleFactorTest(double scaleFactor);
};

// Simple scan of first 5 rows of "nation".
TEST_F(ClpConnectorTest, simple) {
    auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({}, {})) // change it
                  .tableHandle(std::make_shared<ClpTableHandle>(
                      kClpConnectorId, Table::TBL_LINEITEM, 0.01))
                  .assignments({
                      {"n_nationkey", std::make_shared<ClpColumnHandle>("n_nationkey")},
                      {"n_name", std::make_shared<ClpColumnHandle>("n_name")},
                      {"n_regionkey", std::make_shared<ClpColumnHandle>("n_regionkey")},
                      {"n_comment", std::make_shared<ClpColumnHandle>("n_comment")},
                  })
                  .endTableScan()
                  .filter("n_nationkey < 5")
                  .planNode();

  auto output = getResults(plan, {makeClpSplit()});
  auto expected = makeRowVector({
      // n_nationkey
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      // n_name
      makeFlatVector<StringView>({
          "ALGERIA",
          "ARGENTINA",
          "BRAZIL",
          "CANADA",
          "EGYPT",
      }),
      // n_regionkey
      makeFlatVector<int64_t>({0, 1, 1, 1, 4}),
      // n_comment
      makeFlatVector<StringView>({
          " haggle. carefully final deposits detect slyly agai",
          "al foxes promise slyly according to the regular accounts. bold requests alon",
          "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ",
          "eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold",
          "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d",
      }),
  });
  test::assertEqualVectors(expected, output);
}

// Extract single column from "nation".
TEST_F(ClpConnectorTest, singleColumn) {
  auto plan =
      PlanBuilder().tpchTableScan(Table::TBL_NATION, {"n_name"}).planNode();

  auto output = getResults(plan, {makeClpSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALGERIA",       "ARGENTINA", "BRAZIL", "CANADA",
      "EGYPT",         "ETHIOPIA",  "FRANCE", "GERMANY",
      "INDIA",         "INDONESIA", "IRAN",   "IRAQ",
      "JAPAN",         "JORDAN",    "KENYA",  "MOROCCO",
      "MOZAMBIQUE",    "PERU",      "CHINA",  "ROMANIA",
      "SAUDI ARABIA",  "VIETNAM",   "RUSSIA", "UNITED KINGDOM",
      "UNITED STATES",
  })});
  test::assertEqualVectors(expected, output);
  EXPECT_EQ("n_name", output->type()->asRow().nameOf(0));
}

// Check that aliases are correctly resolved.
TEST_F(ClpConnectorTest, singleColumnWithAlias) {
  const std::string aliasedName = "my_aliased_column_name";

  auto outputType = ROW({aliasedName}, {VARCHAR()});
  auto plan =
      PlanBuilder()
          .startTableScan()
          .outputType(outputType)
          .tableHandle(std::make_shared<ClpTableHandle>(
              kClpConnectorId, Table::TBL_NATION))
          .assignments({
              {aliasedName, std::make_shared<ClpColumnHandle>("n_name")},
              {"other_name", std::make_shared<ClpColumnHandle>("n_name")},
              {"third_column",
               std::make_shared<ClpColumnHandle>("n_regionkey")},
          })
          .endTableScan()
          .limit(0, 1, false)
          .planNode();

  auto output = getResults(plan, {makeClpSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALGERIA",
  })});
  test::assertEqualVectors(expected, output);

  EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
  EXPECT_EQ(1, output->childrenSize());
}

void ClpConnectorTest::runScaleFactorTest(double scaleFactor) {
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({}, {}))
                  .tableHandle(std::make_shared<ClpTableHandle>(
                      kClpConnectorId, Table::TBL_SUPPLIER, scaleFactor))
                  .endTableScan()
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  auto output = getResults(plan, {makeClpSplit()});
  int64_t expectedRows = tpch::getRowCount(Table::TBL_SUPPLIER, scaleFactor);
  auto expected = makeRowVector(
      {makeFlatVector<int64_t>(std::vector<int64_t>{expectedRows})});
  test::assertEqualVectors(expected, output);
}

// Aggregation over a larger table.
TEST_F(ClpConnectorTest, simpleAggregation) {
  VELOX_ASSERT_THROW(
      runScaleFactorTest(-1), "Tpch scale factor must be non-negative");
  runScaleFactorTest(0.01);
  runScaleFactorTest(1.0);
  runScaleFactorTest(5.0);
  runScaleFactorTest(13.0);
}

TEST_F(ClpConnectorTest, lineitemTinyRowCount) {
  // Lineitem row count depends on the orders.
  // Verify against Java tiny result.
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(ROW({}, {}))
                  .tableHandle(std::make_shared<ClpTableHandle>(
                      kClpConnectorId, Table::TBL_LINEITEM, 0.01))
                  .endTableScan()
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  std::vector<exec::Split> splits;
  const size_t numParts = 4;

  for (size_t i = 0; i < numParts; ++i) {
    splits.push_back(makeClpSplit(numParts, i));
  }

  auto output = getResults(plan, std::move(splits));
  EXPECT_EQ(60'175, output->childAt(0)->asFlatVector<int64_t>()->valueAt(0));
}

TEST_F(ClpConnectorTest, unknownColumn) {
  EXPECT_THROW(
      {
        PlanBuilder()
            .tpchTableScan(Table::TBL_NATION, {"does_not_exist"})
            .planNode();
      },
      VeloxUserError);
}

// Ensures that splits broken down using different configurations return the
// same dataset in the end.
TEST_F(ClpConnectorTest, multipleSplits) {
  auto plan = PlanBuilder()
                  .tpchTableScan(
                      Table::TBL_NATION,
                      {"n_nationkey", "n_name", "n_regionkey", "n_comment"})
                  .planNode();

  // Use a full read from a single split to use as the source of truth.
  auto fullResult = getResults(plan, {makeClpSplit()});
  size_t nationRowCount = tpch::getRowCount(tpch::Table::TBL_NATION, 1);
  EXPECT_EQ(nationRowCount, fullResult->size());

  // Run query with different number of parts, up until `totalParts >
  // nationRowCount` in which cases each split will return one or zero records.
  for (size_t totalParts = 1; totalParts < (nationRowCount + 5); ++totalParts) {
    std::vector<exec::Split> splits;
    splits.reserve(totalParts);

    for (size_t i = 0; i < totalParts; ++i) {
      splits.emplace_back(makeClpSplit(totalParts, i));
    }

    auto output = getResults(plan, std::move(splits));
    test::assertEqualVectors(fullResult, output);
  }
}

// Join nation and region.
TEST_F(ClpConnectorTest, join) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpchTableScan(
              tpch::Table::TBL_NATION, {"n_regionkey"}, 1.0 /*scaleFactor*/)
          .capturePlanNodeId(nationScanId)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              PlanBuilder(planNodeIdGenerator)
                  .tpchTableScan(
                      tpch::Table::TBL_REGION,
                      {"r_regionkey", "r_name"},
                      1.0 /*scaleFactor*/)
                  .capturePlanNodeId(regionScanId)
                  .planNode(),
              "", // extra filter
              {"r_name"})
          .singleAggregation({"r_name"}, {"count(1) as nation_cnt"})
          .orderBy({"r_name"}, false)
          .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .split(nationScanId, makeClpSplit())
                    .split(regionScanId, makeClpSplit())
                    .copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<StringView>(
          {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}),
      makeConstant<int64_t>(5, 5),
  });
  test::assertEqualVectors(expected, output);
}

TEST_F(ClpConnectorTest, orderDateCount) {
  auto plan = PlanBuilder()
                  .tpchTableScan(Table::TBL_ORDERS, {"o_orderdate"}, 0.01)
                  .filter("o_orderdate = '1992-01-01'::DATE")
                  .limit(0, 10, false)
                  .planNode();

  auto output = getResults(plan, {makeClpSplit()});
  auto orderDate = output->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ("1992-01-01", DATE()->toString(orderDate->valueAt(0)));
  // Match with count obtained from Java.
  EXPECT_EQ(9, orderDate->size());
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
