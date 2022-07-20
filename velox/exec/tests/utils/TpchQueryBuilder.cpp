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
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/common/base/tests/Fs.h"
#include "velox/connectors/hive/HiveConnector.h"

namespace facebook::velox::exec::test {

void TpchQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        dwio::common::ReaderOptions readerOptions;
        readerOptions.setFileFormat(format_);
        std::unique_ptr<dwio::common::Reader> reader =
            dwio::common::getReaderFactory(readerOptions.getFileFormat())
                ->createReader(
                    std::make_unique<dwio::common::FileInputStream>(
                        dirEntry.path()),
                    readerOptions);
        const auto fileType = reader->rowType();
        const auto fileColumnNames = fileType->names();
        // There can be extra columns in the file towards the end.
        VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
        std::unordered_map<std::string, std::string> fileColumnNamesMap(
            columns.size());
        std::transform(
            columns.begin(),
            columns.end(),
            fileColumnNames.begin(),
            std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
            [](std::string a, std::string b) { return std::make_pair(a, b); });
        auto columnNames = columns;
        auto types = fileType->children();
        types.resize(columnNames.size());
        tableMetadata_[tableName].type =
            std::make_shared<RowType>(std::move(columnNames), std::move(types));
        tableMetadata_[tableName].fileColumnNames =
            std::move(fileColumnNamesMap);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
  }
}

const std::vector<std::string>& TpchQueryBuilder::getTableNames() {
  return kTableNames_;
}

TpchPlan TpchQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getQ1Plan();
    case 2:
      return getQ2Plan();
    case 3:
      return getQ3Plan();
    case 4:
      return getQ4Plan();
    case 5:
      return getQ5Plan();
    case 6:
      return getQ6Plan();
    case 7:
      return getQ7Plan();
    case 13:
      return getQ13Plan();
    case 18:
      return getQ18Plan();
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

namespace {
const std::string kLineitem = "lineitem";
const std::string kOrders = "orders";
const std::string kCustomer = "customer";
const std::string kPart = "part";
const std::string kSupplier = "supplier";
const std::string kPartSupp = "partsupp";
const std::string kNation = "nation";
const std::string kRegion = "region";

const std::string kDateConversionSuffix = "::DATE";
} // namespace

TpchPlan TpchQueryBuilder::getQ1Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_returnflag",
      "l_linestatus",
      "l_quantity",
      "l_extendedprice",
      "l_discount",
      "l_tax",
      "l_shipdate"};

  auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  // shipdate <= '1998-09-02'
  const auto shipDate = "l_shipdate";
  std::string filter;
  // DWRF does not support Date type. Use Varchar instead.
  if (selectedRowType->findChild(shipDate)->isVarchar()) {
    filter = "l_shipdate <= '1998-09-02'";
  } else {
    filter = "l_shipdate <= '1998-09-02'::DATE";
  }

  core::PlanNodeId lineitemPlanNodeId;

  auto plan =
      PlanBuilder()
          .tableScan(kLineitem, selectedRowType, fileColumnNames, {filter})
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(
              {"l_returnflag",
               "l_linestatus",
               "l_quantity",
               "l_extendedprice",
               "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
               "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
               "l_discount"})
          .partialAggregation(
              {"l_returnflag", "l_linestatus"},
              {"sum(l_quantity)",
               "sum(l_extendedprice)",
               "sum(l_sum_disc_price)",
               "sum(l_sum_charge)",
               "avg(l_quantity)",
               "avg(l_extendedprice)",
               "avg(l_discount)",
               "count(0)"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_returnflag", "l_linestatus"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

core::PlanNodePtr TpchQueryBuilder::getQ2MinimumCostSupplierPlan(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  core::PlanNodeId partsuppScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto partsupp =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kPartSupp,
              getRowType(
                  kPartSupp, {"ps_partkey", "ps_suppkey", "ps_supplycost"}))
          .capturePlanNodeId(partsuppScanNodeId);

  auto supplier =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"s_suppkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kSupplier,
                       getRowType(kSupplier, {"s_suppkey", "s_nationkey"}))
                   .capturePlanNodeId(supplierScanNodeId)
                   .planNode()})
          .planNode();

  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"n_nationkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kNation,
                       getRowType(kNation, {"n_nationkey", "n_regionkey"}))
                   .capturePlanNodeId(nationScanNodeId)
                   .planNode()})
          .planNode();
  auto region = PlanBuilder(planNodeIdGenerator)
                    .localPartition(
                        {"r_regionkey"},
                        {PlanBuilder(planNodeIdGenerator)
                             .tableScan(
                                 kRegion,
                                 getRowType(kRegion, {"r_regionkey", "r_name"}),
                                 getFileColumnNames(kRegion),
                                 {"r_name = 'EUROPE'"})
                             .capturePlanNodeId(regionScanNodeId)
                             .planNode()})
                    .planNode();

  auto partialMinCostSuppliers =
      partsupp
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              {"ps_partkey", "ps_supplycost", "s_nationkey"})
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              {"ps_partkey", "ps_supplycost", "n_regionkey"})
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              region,
              "",
              {"ps_partkey", "ps_supplycost"})
          .partialAggregation(
              {"ps_partkey"}, {"min(ps_supplycost) as pmin_supplycost"})
          .planNode();

  auto minCostSuppliers = PlanBuilder(planNodeIdGenerator)
                              .localPartition({}, {partialMinCostSuppliers})
                              .finalAggregation(
                                  {"ps_partkey"},
                                  {"min(pmin_supplycost) AS min_supplycost"},
                                  {DOUBLE()})
                              .planNode();

  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartSupp);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);

  return minCostSuppliers;
}

TpchPlan TpchQueryBuilder::getQ2Plan() const {
  TpchPlan context;
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  auto minCostSuppliers =
      getQ2MinimumCostSupplierPlan(planNodeIdGenerator, context);

  core::PlanNodeId partScanNodeId;
  core::PlanNodeId partsuppScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto part =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kPart,
              getRowType(kPart, {"p_partkey", "p_size", "p_mfgr", "p_type"}),
              getFileColumnNames(kPart),
              {},
              {"(p_size = 15) AND (p_type LIKE '%BRASS')"})
          .capturePlanNodeId(partScanNodeId);

  auto partsupp =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"ps_partkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kPartSupp,
                       getRowType(
                           kPartSupp,
                           {"ps_partkey", "ps_suppkey", "ps_supplycost"}))
                   .capturePlanNodeId(partsuppScanNodeId)
                   .planNode()})
          .planNode();

  auto supplier = PlanBuilder(planNodeIdGenerator)
                      .localPartition(
                          {"s_suppkey"},
                          {PlanBuilder(planNodeIdGenerator)
                               .tableScan(
                                   kSupplier,
                                   getRowType(
                                       kSupplier,
                                       {"s_suppkey",
                                        "s_name",
                                        "s_address",
                                        "s_nationkey",
                                        "s_phone",
                                        "s_acctbal",
                                        "s_comment"}))
                               .capturePlanNodeId(supplierScanNodeId)
                               .planNode()})
                      .planNode();

  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"n_nationkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kNation,
                       getRowType(
                           kNation, {"n_nationkey", "n_name", "n_regionkey"}))
                   .capturePlanNodeId(nationScanNodeId)
                   .planNode()})
          .planNode();
  auto region = PlanBuilder(planNodeIdGenerator)
                    .localPartition(
                        {"r_regionkey"},
                        {PlanBuilder(planNodeIdGenerator)
                             .tableScan(
                                 kRegion,
                                 getRowType(kRegion, {"r_regionkey", "r_name"}),
                                 getFileColumnNames(kRegion),
                                 {"r_name = 'EUROPE'"})
                             .capturePlanNodeId(regionScanNodeId)
                             .planNode()})
                    .planNode();

  auto plan =
      part.hashJoin(
              {"p_partkey"},
              {"ps_partkey"},
              partsupp,
              "",
              {"p_partkey", "p_mfgr", "ps_suppkey", "ps_supplycost"})
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              {"p_partkey",
               "p_mfgr",
               "ps_supplycost",
               "s_name",
               "s_address",
               "s_nationkey",
               "s_phone",
               "s_acctbal",
               "s_comment"})
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              {"p_partkey",
               "p_mfgr",
               "ps_supplycost",
               "s_name",
               "s_address",
               "s_phone",
               "s_acctbal",
               "s_comment",
               "n_name",
               "n_regionkey"})
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              region,
              "",
              {"p_partkey",
               "p_mfgr",
               "ps_supplycost",
               "s_name",
               "s_address",
               "s_phone",
               "s_acctbal",
               "s_comment",
               "n_name"})
          .hashJoin(
              {"p_partkey", "ps_supplycost"},
              {"ps_partkey", "min_supplycost"},
              minCostSuppliers,
              "",
              {"p_partkey",
               "p_mfgr",
               "ps_supplycost",
               "s_name",
               "s_address",
               "s_phone",
               "s_acctbal",
               "s_comment",
               "n_name",
               "min_supplycost"})
          .topN({"s_acctbal desc", "n_name", "s_name", "p_partkey"}, 100, false)
          .planNode();

  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartSupp);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

core::PlanNodePtr TpchQueryBuilder::getQ3OrderPlans(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  std::vector<std::string> selectedOrdersColumns{
      "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"};

  auto selectedOrdersRowType = getRowType(kOrders, selectedOrdersColumns);
  std::string ordersFilter = "o_orderdate < '1995-03-15'";

  if (!selectedOrdersRowType->findChild("o_orderdate")->isVarchar()) {
    ordersFilter += kDateConversionSuffix;
  }

  core::PlanNodeId ordersScanNodeId;
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .localPartition(
                        {"o_custkey"},
                        {PlanBuilder(planNodeIdGenerator)
                             .tableScan(
                                 kOrders,
                                 getRowType(kOrders, selectedOrdersColumns),
                                 getFileColumnNames(kOrders),
                                 {ordersFilter},
                                 {})
                             .capturePlanNodeId(ordersScanNodeId)
                             .planNode()})
                    .planNode();

  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  return orders;
}

PlanBuilder TpchQueryBuilder::getQ3CustomerPlans(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  core::PlanNodeId customersScanNodeId;
  auto customers =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"c_custkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kCustomer,
                       getRowType(kCustomer, {"c_custkey", "c_mktsegment"}),
                       getFileColumnNames(kCustomer),
                       {"c_mktsegment = 'BUILDING'"},
                       {})
                   .capturePlanNodeId(customersScanNodeId)
                   .planNode()});

  context.dataFiles[customersScanNodeId] = getTableFilePaths(kCustomer);
  return customers;
}

core::PlanNodePtr TpchQueryBuilder::getQ3LineItemsPlans(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  std::vector<std::string> selectedLineItemsColumns{
      "l_shipdate", "l_discount", "l_orderkey", "l_extendedprice"};

  auto selectedLineItemsRowType =
      getRowType(kLineitem, selectedLineItemsColumns);

  std::string lineItemsFilter = "l_shipdate > '1995-03-15'";

  if (!selectedLineItemsRowType->findChild("l_shipdate")->isVarchar()) {
    lineItemsFilter += kDateConversionSuffix;
  }

  core::PlanNodeId lineItemsScanNodeId;
  auto lineItems =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"l_orderkey"},
              {PlanBuilder(planNodeIdGenerator)
                   .tableScan(
                       kLineitem,
                       getRowType(kLineitem, selectedLineItemsColumns),
                       getFileColumnNames(kOrders),
                       {lineItemsFilter},
                       {})
                   .capturePlanNodeId(lineItemsScanNodeId)
                   .planNode()})
          .planNode();

  context.dataFiles[lineItemsScanNodeId] = getTableFilePaths(kLineitem);
  return lineItems;
}

TpchPlan TpchQueryBuilder::getQ3Plan() const {
  TpchPlan context;
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto customers = getQ3CustomerPlans(planNodeIdGenerator, context);
  auto orders = getQ3OrderPlans(planNodeIdGenerator, context);
  auto lineItems = getQ3LineItemsPlans(planNodeIdGenerator, context);

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .localPartition(
                      {"o_orderkey", "o_orderdate", "o_shippriority"},
                      {customers
                           .hashJoin(
                               {"c_custkey"},
                               {"o_custkey"},
                               orders,
                               "",
                               {"o_orderkey", "o_orderdate", "o_shippriority"})
                           .hashJoin(
                               {"o_orderkey"},
                               {"l_orderkey"},
                               lineItems,
                               "",
                               {"o_orderdate",
                                "o_shippriority",
                                "o_orderkey",
                                "l_extendedprice",
                                "l_discount"})
                           .project(
                               {"o_orderkey",
                                "(l_extendedprice) * (1.0 - l_discount)",
                                "o_orderdate",
                                "o_shippriority"})
                           .partialAggregation(
                               {"o_orderkey", "o_orderdate", "o_shippriority"},
                               {"sum(p1) AS revenue"})
                           .planNode()})
                  .finalAggregation()
                  .topN({"revenue desc", "o_orderdate asc"}, 10, false)
                  .planNode();

  context.plan = std::move(plan);
  context.dataFileFormat = format_;
  return context;
}

PlanBuilder TpchQueryBuilder::getQ4OrdersPlan(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  std::vector<std::string> ordersSelectedColumns = {
      "o_orderpriority", "o_orderdate", "o_orderkey"};
  auto orderSelectedRowType = getRowType(kOrders, ordersSelectedColumns);
  const auto& ordersFileColumnNames = getFileColumnNames(kOrders);

  std::string orderDateFilter;
  if (orderSelectedRowType->findChild("o_orderdate")->isVarchar()) {
    orderDateFilter = "o_orderdate between '1993-07-01' and '1993-10-01'";
  } else {
    orderDateFilter =
        "o_orderdate between '1993-07-01'::DATE and '1993-10-01'::DATE";
  }

  core::PlanNodeId orderPlanNodeId;
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        orderSelectedRowType,
                        ordersFileColumnNames,
                        {},
                        {orderDateFilter})
                    .capturePlanNodeId(orderPlanNodeId);
  context.dataFiles[orderPlanNodeId] = getTableFilePaths(kOrders);
  return orders;
}

core::PlanNodePtr TpchQueryBuilder::getQ4LineItemsPlan(
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    TpchPlan& context) const {
  std::vector<std::string> lineitemSelectedColumns = {
      "l_orderkey", "l_commitdate", "l_receiptdate"};
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemSelectedColumns);
  const auto& lineitemFileColumnNames = getFileColumnNames(kLineitem);

  std::string commitDateFilter;
  if (lineitemSelectedRowType->findChild("l_commitdate")->isVarchar()) {
    commitDateFilter = "l_commitdate < l_receiptdate";
  } else {
    commitDateFilter = "l_commitdate::DATE < l_receiptdate::DATE";
  }

  core::PlanNodeId lineitemPlanNodeId;
  auto lineitems = PlanBuilder(planNodeIdGenerator)
                       .localPartition(
                           {"l_orderkey"},
                           {PlanBuilder(planNodeIdGenerator)
                                .tableScan(
                                    kLineitem,
                                    lineitemSelectedRowType,
                                    lineitemFileColumnNames,
                                    {},
                                    {commitDateFilter})
                                .capturePlanNodeId(lineitemPlanNodeId)
                                .partialAggregation({"l_orderkey"}, {})
                                .planNode()})
                       .finalAggregation({"l_orderkey"}, {}, {BIGINT()})
                       .planNode();

  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  return lineitems;
}

TpchPlan TpchQueryBuilder::getQ4Plan() const {
  TpchPlan context;
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto orders = getQ4OrdersPlan(planNodeIdGenerator, context);
  auto lineitems = getQ4LineItemsPlan(planNodeIdGenerator, context);

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .localPartition(
              {"o_orderpriority"},
              {orders
                   .hashJoin(
                       {"o_orderkey"},
                       {"l_orderkey"},
                       lineitems,
                       "",
                       {"o_orderpriority"})
                   .partialAggregation(
                       {"o_orderpriority"}, {"count(1) AS partialCount"})
                   .planNode()})
          .finalAggregation(
              {"o_orderpriority"},
              {"sum(partialCount) as order_count"},
              {BIGINT(), VARCHAR()})
          .orderBy({"o_orderpriority asc"}, false)
          .planNode();

  context.plan = std::move(plan);
  context.dataFileFormat = format_;
  return context;
}

PlanBuilder TpchQueryBuilder::getQ5CustomersPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  std::vector<std::string> selectedColumns = {"c_custkey", "c_nationkey"};

  core::PlanNodeId customersScanNodeId;
  auto customers = PlanBuilder(planNodeIdGenerator)
                       .tableScan(
                           kCustomer,
                           getRowType(kCustomer, selectedColumns),
                           getFileColumnNames(kCustomer),
                           {},
                           {})
                       .capturePlanNodeId(customersScanNodeId)
                       .localPartition({"c_custkey"});

  context.dataFiles[customersScanNodeId] = getTableFilePaths(kCustomer);
  return customers;
}

core::PlanNodePtr TpchQueryBuilder::getQ5OrdersPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  std::vector<std::string> selectedColumns = {
      "o_orderdate", "o_orderkey", "o_custkey"};
  auto orderSelectedRowType = getRowType(kOrders, selectedColumns);

  std::string filter;
  if (orderSelectedRowType->findChild("o_orderdate")->isVarchar()) {
    filter = "o_orderdate between '1994-01-01' and '1995-01-01'";
  } else {
    filter = "o_orderdate between '1994-01-01'::DATE and '1995-01-01'::DATE";
  }

  core::PlanNodeId orderPlanNodeId;
  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        orderSelectedRowType,
                        getFileColumnNames(kOrders),
                        {},
                        {filter})
                    .capturePlanNodeId(orderPlanNodeId)
                    .planNode();

  context.dataFiles[orderPlanNodeId] = getTableFilePaths(kOrders);
  return orders;
}

core::PlanNodePtr TpchQueryBuilder::getQ5LineItemsPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  std::vector<std::string> selectedColumns{
      "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"};

  core::PlanNodeId lineItemsScanNodeId;
  auto lineItems = PlanBuilder(planNodeIdGenerator)
                       .tableScan(
                           kLineitem,
                           getRowType(kLineitem, selectedColumns),
                           getFileColumnNames(kOrders),
                           {},
                           "")
                       .capturePlanNodeId(lineItemsScanNodeId)
                       .localPartition({"l_orderkey"})
                       .planNode();

  context.dataFiles[lineItemsScanNodeId] = getTableFilePaths(kLineitem);
  return lineItems;
}

core::PlanNodePtr TpchQueryBuilder::getQ5SuppliersPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsSupplier = {
      "s_suppkey", "s_nationkey"};

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kSupplier,
                      getRowType(kSupplier, selectedColumnsSupplier),
                      getFileColumnNames(kSupplier),
                      {},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition({"s_suppkey"})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kSupplier);
  return plan;
}

core::PlanNodePtr TpchQueryBuilder::getQ5NationsPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumns = {
      "n_nationkey", "n_name", "n_regionkey"};

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kNation,
                      getRowType(kNation, selectedColumns),
                      getFileColumnNames(kNation),
                      {},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition({"n_nationkey"})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kNation);
  return plan;
}

core::PlanNodePtr TpchQueryBuilder::getQ5RegionsPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumns = {"r_regionkey", "r_name"};

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kRegion,
                      getRowType(kRegion, selectedColumns),
                      getFileColumnNames(kRegion),
                      {"r_name = 'ASIA'"},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition({"r_regionkey"})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kRegion);
  return plan;
}

TpchPlan TpchQueryBuilder::getQ5Plan() const {
  TpchPlan context;
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto planCustomers = getQ5CustomersPlan(context, planNodeIdGenerator);
  auto planOrders = getQ5OrdersPlan(context, planNodeIdGenerator);
  auto planLineItems = getQ5LineItemsPlan(context, planNodeIdGenerator);
  auto planSuppliers = getQ5SuppliersPlan(context, planNodeIdGenerator);
  auto planNations = getQ5NationsPlan(context, planNodeIdGenerator);
  auto planRegions = getQ5RegionsPlan(context, planNodeIdGenerator);

  auto plan =
      planCustomers
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              planOrders,
              "",
              {"c_nationkey", "o_orderkey"})
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              planLineItems,
              "",
              {"c_nationkey", "l_suppkey", "l_extendedprice", "l_discount"})
          .hashJoin(
              {"l_suppkey", "c_nationkey"},
              {"s_suppkey", "s_nationkey"},
              planSuppliers,
              "",
              {"s_nationkey", "l_extendedprice", "l_discount"})
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              planNations,
              "",
              {"l_extendedprice", "l_discount", "n_name", "n_regionkey"})
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              planRegions,
              "",
              {"l_extendedprice", "l_discount", "n_name"})
          .project(
              {"n_name AS name", "l_extendedprice * (1.0 - l_discount) AS rev"})
          .partialAggregation({"name"}, {"sum(rev) AS partialRevenue"})
          .localPartition({"name"})
          .finalAggregation(
              {"name"}, {"sum(partialRevenue) AS revenue"}, {DOUBLE()})
          .orderBy({"revenue desc"}, false)
          .planNode();
  context.plan = std::move(plan);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ6Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};

  auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  const auto shipDate = "l_shipdate";
  std::string shipDateFilter;
  // DWRF does not support Date type. Use Varchar instead.
  if (selectedRowType->findChild(shipDate)->isVarchar()) {
    shipDateFilter = "l_shipdate between '1994-01-01' and '1994-12-31'";
  } else {
    shipDateFilter =
        "l_shipdate between '1994-01-01'::DATE and '1994-12-31'::DATE";
  }

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(
                      kLineitem,
                      selectedRowType,
                      fileColumnNames,
                      {shipDateFilter,
                       "l_discount between 0.05 and 0.07",
                       "l_quantity < 24.0"})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"l_extendedprice * l_discount"})
                  .partialAggregation({}, {"sum(p0)"})
                  .localPartition({})
                  .finalAggregation()
                  .planNode();
  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

std::pair<std::string, PlanBuilder> TpchQueryBuilder::getQ7LineItemPlanBuilder(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsLineItem = {
      "l_shipdate", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"};
  auto selectedRowTypeLineItem = getRowType(kLineitem, selectedColumnsLineItem);
  const auto& fileColumnNamesLineItem = getFileColumnNames(kLineitem);

  std::string filter;
  std::string extractExpression;
  if (selectedRowTypeLineItem->findChild("l_shipdate")->isVarchar()) {
    filter = "l_shipdate between '1995-01-01' and '1996-12-31'";
    extractExpression = "YEAR(CAST(l_shipdate AS datetime)) AS l_year";
  } else {
    filter = "l_shipdate between '1995-01-01'::DATE and '1996-12-31'::DATE";
    extractExpression = "YEAR(l_shipdate) AS l_year";
  }

  auto planBuilder = PlanBuilder(planNodeIdGenerator)
                         .tableScan(
                             kLineitem,
                             selectedRowTypeLineItem,
                             fileColumnNamesLineItem,
                             {filter},
                             "")
                         .capturePlanNodeId(planNodeId);

  context.dataFiles[planNodeId] = getTableFilePaths(kLineitem);
  return {extractExpression, planBuilder};
}

core::PlanNodePtr TpchQueryBuilder::getQ7SupplierPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsSupplier = {
      "s_suppkey", "s_nationkey"};
  auto selectedRowTypeSupplier = getRowType(kSupplier, selectedColumnsSupplier);
  const auto& fileColumnNamesSupplier = getFileColumnNames(kSupplier);

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kSupplier,
                      selectedRowTypeSupplier,
                      fileColumnNamesSupplier,
                      {},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition({"s_suppkey"})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kSupplier);
  return plan;
}

core::PlanNodePtr TpchQueryBuilder::getQ7OrdersPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsOrders = {"o_orderkey", "o_custkey"};
  auto selectedRowTypeOrders = getRowType(kOrders, selectedColumnsOrders);
  const auto& fileColumnNamesOrders = getFileColumnNames(kOrders);

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kOrders, selectedRowTypeOrders, fileColumnNamesOrders, {}, "")
          .capturePlanNodeId(planNodeId)
          .localPartition({"o_orderkey"})
          .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kOrders);
  return plan;
}

core::PlanNodePtr TpchQueryBuilder::getQ7CustomerPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsCustomer = {
      "c_custkey", "c_nationkey"};
  auto selectedRowTypeCustomer = getRowType(kCustomer, selectedColumnsCustomer);
  const auto& fileColumnNamesCustomer = getFileColumnNames(kCustomer);

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kCustomer,
                      selectedRowTypeCustomer,
                      fileColumnNamesCustomer,
                      {},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition({"c_custkey"})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kCustomer);
  return plan;
}

core::PlanNodePtr TpchQueryBuilder::getQ7NationPlan(
    TpchPlan& context,
    std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
    std::string nationType) const {
  core::PlanNodeId planNodeId;
  std::vector<std::string> selectedColumnsNation = {"n_name", "n_nationkey"};
  auto selectedRowTypeNation = getRowType(kNation, selectedColumnsNation);
  const auto& fileColumnNamesNation = getFileColumnNames(kNation);

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kNation,
                      selectedRowTypeNation,
                      fileColumnNamesNation,
                      {"n_name = 'GERMANY' OR n_name = 'FRANCE'"},
                      "")
                  .capturePlanNodeId(planNodeId)
                  .localPartition(
                      {"n_nationkey"}, {"n_nationkey", "n_name " + nationType})
                  .planNode();

  context.dataFiles[planNodeId] = getTableFilePaths(kNation);
  return plan;
}

TpchPlan TpchQueryBuilder::getQ7Plan() const {
  TpchPlan context;
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();

  auto [extractExpression, planBuilderLineItem] =
      getQ7LineItemPlanBuilder(context, planNodeIdGenerator);
  auto planSupplier = getQ7SupplierPlan(context, planNodeIdGenerator);
  auto planOrders = getQ7OrdersPlan(context, planNodeIdGenerator);
  auto planCustomer = getQ7CustomerPlan(context, planNodeIdGenerator);
  auto planSupplierNation =
      getQ7NationPlan(context, planNodeIdGenerator, "AS supp_name");
  auto planCustomerNation =
      getQ7NationPlan(context, planNodeIdGenerator, "AS cust_name");

  auto plan =
      planBuilderLineItem
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              planSupplier,
              "",
              {"s_nationkey",
               "l_orderkey",
               "l_extendedprice",
               "l_discount",
               "l_shipdate"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              planOrders,
              "",
              {"s_nationkey",
               "l_extendedprice",
               "l_discount",
               "l_shipdate",
               "o_custkey"})
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              planCustomer,
              "",
              {"s_nationkey",
               "l_extendedprice",
               "l_discount",
               "l_shipdate",
               "c_nationkey"})
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              planSupplierNation,
              "",
              {"l_extendedprice",
               "l_discount",
               "l_shipdate",
               "c_nationkey",
               "supp_name"})
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              planCustomerNation,
              "(supp_name='FRANCE' OR cust_name='FRANCE') AND (supp_name='GERMANY' OR cust_name='GERMANY')",
              {"l_extendedprice",
               "l_discount",
               "l_shipdate",
               "supp_name",
               "cust_name"})
          .project(
              {"supp_name",
               "cust_name",
               extractExpression,
               "l_extendedprice * (1.0 - l_discount) AS volume"})
          .partialAggregation(
              {"supp_name", "cust_name", "l_year"},
              {"sum(volume) AS partialRevenue"})
          .localPartition(
              {"supp_name", "cust_name", "l_year"},
              {"supp_name", "cust_name", "l_year", "partialRevenue"})
          .finalAggregation(
              {"supp_name", "cust_name", "l_year"},
              {"sum(partialRevenue) AS revenue"},
              {DOUBLE()})
          .orderBy({"supp_name", "cust_name", "l_year"}, false)
          .planNode();

  context.plan = std::move(plan);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ13Plan() const {
  std::vector<std::string> ordersColumns = {
      "o_custkey", "o_comment", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey"};

  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {},
              "o_comment not like '%special%requests%'")
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers,
              "",
              {"c_custkey", "o_orderkey"},
              core::JoinType::kRight)
          .partialAggregation({"c_custkey"}, {"count(o_orderkey) as pc_count"})
          .localPartition({})
          .finalAggregation(
              {"c_custkey"}, {"count(pc_count) as c_count"}, {BIGINT()})
          .singleAggregation({"c_count"}, {"count(0) as custdist"})
          .orderBy({"custdist DESC", "c_count DESC"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ18Plan() const {
  std::vector<std::string> lineitemColumns = {"l_orderkey", "l_quantity"};
  std::vector<std::string> ordersColumns = {
      "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"};
  std::vector<std::string> customerColumns = {"c_name", "c_custkey"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  auto bigOrders =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .partialAggregation(
              {"l_orderkey"}, {"sum(l_quantity) AS partial_sum"})
          .localPartition({"l_orderkey"})
          .finalAggregation(
              {"l_orderkey"}, {"sum(partial_sum) AS quantity"}, {DOUBLE()})
          .filter("quantity > 300.0")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              bigOrders,
              "",
              {"o_orderkey",
               "o_custkey",
               "o_orderdate",
               "o_totalprice",
               "l_orderkey",
               "quantity"})
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kCustomer, customerSelectedRowType, customerFileColumns)
                  .capturePlanNodeId(customerScanNodeId)
                  .planNode(),
              "",
              {"c_name",
               "c_custkey",
               "o_orderkey",
               "o_orderdate",
               "o_totalprice",
               "quantity"})
          .localPartition({})
          .orderBy({"o_totalprice DESC", "o_orderdate"}, false)
          .limit(0, 100, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> TpchQueryBuilder::kTableNames_ = {
    kLineitem,
    kOrders,
    kCustomer,
    kPart,
    kSupplier,
    kPartSupp,
    kNation,
    kRegion};

const std::unordered_map<std::string, std::vector<std::string>>
    TpchQueryBuilder::kTables_ = {
        std::make_pair(
            "lineitem",
            std::vector<std::string>{
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment"}),
        std::make_pair(
            "orders",
            std::vector<std::string>{
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_orderpriority",
                "o_clerk",
                "o_shippriority",
                "o_comment"}),
        std::make_pair(
            "customer",
            std::vector<std::string>{
                "c_custkey",
                "c_name",
                "c_address",
                "c_nationkey",
                "c_phone",
                "c_acctbal",
                "c_mktsegment",
                "c_comment"}),
        std::make_pair(
            "part",
            std::vector<std::string>{
                "p_partkey",
                "p_name",
                "p_mfgr",
                "p_brand",
                "p_type",
                "p_size",
                "p_container",
                "p_retailprice",
                "p_comment"}),
        std::make_pair(
            "partsupp",
            std::vector<std::string>{
                "ps_partkey",
                "ps_suppkey",
                "ps_availqty",
                "ps_supplycost",
                "ps_comment"}),
        std::make_pair(
            "supplier",
            std::vector<std::string>{
                "s_suppkey",
                "s_name",
                "s_address",
                "s_nationkey",
                "s_phone",
                "s_acctbal",
                "s_comment"}),
        std::make_pair(
            "nation",
            std::vector<std::string>{
                "n_nationkey",
                "n_name",
                "n_regionkey",
                "n_comment"}),
        std::make_pair(
            "region",
            std::vector<std::string>{"r_regionkey", "r_name", "r_comment"})};
} // namespace facebook::velox::exec::test
