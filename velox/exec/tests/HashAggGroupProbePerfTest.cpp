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

#include "velox/common/testutil/TestValue.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
using TestValue = facebook::velox::common::testutil::TestValue;
using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class HashAggGroupProbePerfTest : public OperatorTestBase {
public:
protected:
 int VELOX_INPUT_BATCH_SIZE = 1000000;
 const int64_t BASE_TIMESTAMP_MS = 1682006400000L;
 struct TagValueIndex {
   int dcValueIndex;
   int clusterValueIndex;
   int hostValueIndex;
   int tag0ValueIndex;
   int tag1ValueIndex;
 };
 struct RowWithMap {
   std::string metricName;
   std::string dc;
   std::string cluster;
   std::string host;
   std::string tag0;
   std::string tag1;
   std::vector<int64_t> timestamps;
   std::vector<double> _values;
 };
 struct Row {
   std::string metricName;
   std::string dc;
   std::string cluster;
   std::string host;
   std::string tag0;
   std::string tag1;
   int64_t timestamp;
   double _value;
 };

 int dcCardinality = 4;
 int clusterCardinality = 5;
 int hostCardinality = 1000;
 int tag0Cardinality = 2;
 int tag1Cardinality = 2;
 std::vector<std::string> dcValues;
 std::vector<std::string> clusterValues;
 std::vector<std::string> hostValues;
 std::vector<std::string> tag0Values;
 std::vector<std::string> tag1Values;
 std::vector<TagValueIndex> tagValueIndices;

 std::vector<std::string> generateTagValues(
     std::string prefix,
     int cardinality) {
   std::vector<std::string> values(cardinality);
   for (int i = 0; i < cardinality; i++) {
     values[i] = prefix + "_" + std::to_string(i);
   }
   std::random_device rd;
   std::mt19937 g(rd());
   std::shuffle(values.begin(), values.end(), g);
   return values;
 }

 void prepareTags(
     int dcCardinality,
     int clusterCardinality,
     int hostCardinality,
     int tag0Cardinality,
     int tag1Cardinality) {
   int totalSeriesNum = dcCardinality * clusterCardinality * hostCardinality *
       tag0Cardinality * tag1Cardinality;
   dcValues = generateTagValues("dc", dcCardinality);
   clusterValues = generateTagValues("cluster", clusterCardinality);
   hostValues = generateTagValues("host", hostCardinality);
   tag0Values = generateTagValues("tag0", tag0Cardinality);
   tag1Values = generateTagValues("tag1", tag1Cardinality);
   tagValueIndices = std::vector<TagValueIndex>(totalSeriesNum);
   int index = 0;
   for (int dc = 0; dc < dcCardinality; dc++) {
     for (int cluster = 0; cluster < clusterCardinality; cluster++) {
       for (int host = 0; host < hostCardinality; host++) {
         for (int tag0 = 0; tag0 < tag0Cardinality; tag0++) {
           for (int tag1 = 0; tag1 < tag1Cardinality; tag1++) {
             tagValueIndices[index++] =
                 TagValueIndex{dc, cluster, host, tag0, tag1};
           }
         }
       }
     }
   }
 }
 RowVectorPtr rowsToVeloxRow(std::vector<Row>& rows) {
   std::vector<std::optional<StringView>> nameData;
   std::vector<std::optional<StringView>> dcData;
   std::vector<std::optional<StringView>> clusterData;
   std::vector<std::optional<StringView>> hostData;
   std::vector<std::optional<StringView>> tag0Data;
   std::vector<std::optional<StringView>> tag1Data;
   std::vector<std::optional<int64_t>> timeStampData;
   std::vector<double> valueData;

   for (const Row& row : rows) {
     nameData.push_back("metrics.test1");
     dcData.push_back(StringView(row.dc));
     clusterData.push_back(StringView(row.cluster));
     hostData.push_back(StringView(row.host));
     tag0Data.push_back(StringView(row.tag0));
     tag1Data.push_back(StringView(row.tag1));
     timeStampData.push_back(row.timestamp);
     valueData.push_back(row._value);
   }

   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> metricName =
       vectorMaker_.dictionaryVector<StringView>(nameData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> dc =
       vectorMaker_.dictionaryVector<StringView>(dcData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> cluster =
       vectorMaker_.dictionaryVector<StringView>(clusterData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> host =
       vectorMaker_.dictionaryVector<StringView>(hostData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> tag0 =
       vectorMaker_.dictionaryVector<StringView>(tag0Data);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> tag1 =
       vectorMaker_.dictionaryVector<StringView>(tag1Data);
   auto timeStamp = vectorMaker_.dictionaryVector<int64_t>(timeStampData);
   FlatVectorPtr<double> value = vectorMaker_.flatVector<double>(valueData);

   return vectorMaker_.rowVector(
       {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
       {metricName, dc, cluster, host, tag0, tag1, timeStamp, value});
 }
 RowVectorPtr rowsToVeloxRowWithMap(std::vector<RowWithMap>& rows) {
   std::vector<std::optional<StringView>> nameData;
   std::vector<std::optional<StringView>> dcData;
   std::vector<std::optional<StringView>> clusterData;
   std::vector<std::optional<StringView>> hostData;
   std::vector<std::optional<StringView>> tag0Data;
   std::vector<std::optional<StringView>> tag1Data;
   std::vector<std::optional<int64_t>> timeStampData;
   std::vector<int64_t> timestampss;
   std::vector<double> valuess;
   std::vector<vector_size_t> offsets;
   vector_size_t offset = 0;
   for (const RowWithMap& row : rows) {
     nameData.push_back("metrics.test1");
     dcData.push_back(StringView(row.dc));
     clusterData.push_back(StringView(row.cluster));
     hostData.push_back(StringView(row.host));
     tag0Data.push_back(StringView(row.tag0));
     tag1Data.push_back(StringView(row.tag1));
     timestampss.insert(
         timestampss.end(), row.timestamps.begin(), row.timestamps.end());
     valuess.insert(valuess.end(), row._values.begin(), row._values.end());
     offsets.push_back(offset);
     offset += row._values.size();
     //      timeStampData.push_back(row.timestamp);
     //      valueData.push_back(row._value);
   }

   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> metricName =
       vectorMaker_.dictionaryVector<StringView>(nameData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> dc =
       vectorMaker_.dictionaryVector<StringView>(dcData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> cluster =
       vectorMaker_.dictionaryVector<StringView>(clusterData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> host =
       vectorMaker_.dictionaryVector<StringView>(hostData);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> tag0 =
       vectorMaker_.dictionaryVector<StringView>(tag0Data);
   DictionaryVectorPtr<VectorMaker::EvalType<StringView>> tag1 =
       vectorMaker_.dictionaryVector<StringView>(tag1Data);
   auto timeStamp = vectorMaker_.dictionaryVector<int64_t>(timeStampData);
   //    FlatVectorPtr<double> value =
   //    vectorMaker_.flatVector<double>(valueData);

   FlatVectorPtr<int64_t> keyvector =
       vectorMaker_.flatVector<int64_t>(timestampss);
   FlatVectorPtr<double> valuevector =
       vectorMaker_.flatVector<double>(valuess);
   //    auto timeStamp =
   //    vectorMaker_.dictionaryVector<int64_t>(timeStampData);
   //    FlatVectorPtr<double> value =
   //    vectorMaker_.flatVector<double>(valueData);
   MapVectorPtr mapvector =
       vectorMaker_.mapVector(offsets, keyvector, valuevector, {});
   return vectorMaker_.rowVector(
       {"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
       {metricName, dc, cluster, host, tag0, tag1, mapvector});
 }

 std::vector<RowVectorPtr> generateVeloxInputs(
     std::string metricName,
     int dataRangeInHour,
     int totalSeriesNum) {
   std::vector<RowVectorPtr> result;
   long eachSeriesDPNum = 120 * dataRangeInHour;
   std::vector<Row> rows;

   int batch = 0;
   for (int seriesIndex = 0; seriesIndex < totalSeriesNum; seriesIndex++) {
     TagValueIndex tagValueIndex = tagValueIndices[seriesIndex];
     std::string dcValue = dcValues[tagValueIndex.dcValueIndex];
     std::string clusterValue = clusterValues[tagValueIndex.clusterValueIndex];
     std::string hostValue = hostValues[tagValueIndex.hostValueIndex];
     std::string tag0Value = tag0Values[tagValueIndex.tag0ValueIndex];
     std::string tag1Value = tag1Values[tagValueIndex.tag1ValueIndex];
     long startTimestamp = BASE_TIMESTAMP_MS;
     for (int dpIndex = 0; dpIndex < eachSeriesDPNum; dpIndex++) {
       rows.push_back(
           Row{metricName,
               dcValue,
               clusterValue,
               hostValue,
               tag0Value,
               tag1Value,
               startTimestamp,
               1});
       startTimestamp += 30000;
       if (rows.size() >= VELOX_INPUT_BATCH_SIZE) {
         result.push_back(rowsToVeloxRow(rows));

         rows.clear();
       }
     }
   }

   if (!rows.empty()) {
     result.push_back(rowsToVeloxRow(rows));
     rows.clear();
   }
   return result;
 }
 std::vector<RowVectorPtr> generateVeloxInputsWithMap(
     std::string metricName,
     int dataRangeInHour,
     int totalSeriesNum) {
   std::vector<RowVectorPtr> result;
   long eachSeriesDPNum = 120 * dataRangeInHour;
   std::vector<RowWithMap> rows;

   int batch = 0;
   for (int seriesIndex = 0; seriesIndex < totalSeriesNum; seriesIndex++) {
     TagValueIndex tagValueIndex = tagValueIndices[seriesIndex];
     std::string dcValue = dcValues[tagValueIndex.dcValueIndex];
     std::string clusterValue = clusterValues[tagValueIndex.clusterValueIndex];
     std::string hostValue = hostValues[tagValueIndex.hostValueIndex];
     std::string tag0Value = tag0Values[tagValueIndex.tag0ValueIndex];
     std::string tag1Value = tag1Values[tagValueIndex.tag1ValueIndex];
     long startTimestamp = BASE_TIMESTAMP_MS;
     std::vector<int64_t> timestamps;
     std::vector<double> values;
     for (int dpIndex = 0; dpIndex < eachSeriesDPNum; dpIndex++) {
       timestamps.push_back(startTimestamp);
       values.push_back(1);
       startTimestamp += 30000;
     }

     // emit a row
     rows.push_back(RowWithMap{
         metricName,
         dcValue,
         clusterValue,
         hostValue,
         tag0Value,
         tag1Value,
         timestamps,
         values});
   }

   if (!rows.empty()) {
     result.push_back(rowsToVeloxRowWithMap(rows));
     rows.clear();
   }
   return result;
 }
};
__attribute__((noinline)) void free1(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free2(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free3(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free4(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free5(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free6(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free7(VectorPtr p) {
 p.reset();
}

__attribute__((noinline)) void free8(VectorPtr p) {
 p.reset();
}
__attribute__((noinline)) void free9(VectorPtr p) {
 p.reset();
}
__attribute__((noinline)) void free10(VectorPtr p) {
 p.reset();
}

//
TEST_F(HashAggGroupProbePerfTest, DISABLED_veloxVectorReleaseStudy) {
 for (int k = 0; k < 10; ++k) {
   int szBase = 1024 * 1024 * 10;
   std::vector<VectorPtr> data(10);
   for (int i = 0; i < 10; i++) {
     data[i] = vectorMaker_.flatVector<double>(
         szBase * (i + 1),
         [](facebook::velox::vector_size_t row) { return 1; },
         nullptr,
         DOUBLE());
   }
   free1(std::move(data[0]));
   free2(std::move(data[1]));
   free3(std::move(data[2]));
   free4(std::move(data[3]));
   free5(std::move(data[4]));
   free6(std::move(data[5]));
   free7(std::move(data[6]));
   free8(std::move(data[7]));
   free9(std::move(data[8]));
   free10(std::move(data[9]));
 }
}

__attribute__((noinline)) void freeTimestamp(std::vector<RowVectorPtr> p) {
 p.clear();
}

__attribute__((noinline)) void freeMap(std::vector<RowVectorPtr> p) {
 p.clear();
}

__attribute__((noinline)) void benchmarkFree(
   std::vector<RowVectorPtr> inputWithTimestamp,
   std::vector<RowVectorPtr> inputWithMap) {
 freeTimestamp(std::move(inputWithTimestamp));
 freeMap(std::move(inputWithMap));
}
TEST_F(
   HashAggGroupProbePerfTest,
   DISABLED_veloxVectorReleaseStudyTimestampVsMap) {
 prepareTags(
     dcCardinality,
     clusterCardinality,
     hostCardinality,
     tag0Cardinality,
     tag1Cardinality);
 for (int i = 0; i < 100; i++) {
   std::vector<RowVectorPtr> inputWithMap = generateVeloxInputsWithMap(
       "metricName1",
       3 /*dataRangeInHr*/,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);
   for (int i = 0; i < inputWithMap.size(); i++) {
     auto sz = inputWithMap[i]->children().size();
     inputWithMap[i] =
         vectorMaker_.rowVector({inputWithMap[i]->children()[sz - 1]});
   }

   std::vector<RowVectorPtr> inputWithTimestamp = generateVeloxInputs(
       "metricName1",
       3 /*dataRangeInHr*/,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);
   for (int i = 0; i < inputWithTimestamp.size(); i++) {
     int sz = inputWithTimestamp[i]->childrenSize();
     inputWithTimestamp[i] = vectorMaker_.rowVector(
         {inputWithTimestamp[i]->children()[sz - 2],
          inputWithTimestamp[i]->children()[sz - 1]});
   }

   benchmarkFree(std::move(inputWithTimestamp), std::move(inputWithMap));
 }
}

TEST_F(HashAggGroupProbePerfTest, aggregationNoFilter) {
 TestValue::setMode(1);
 VELOX_INPUT_BATCH_SIZE = 1000000;
 //  VELOX_INPUT_BATCH_SIZE = ((float)VELOX_INPUT_BATCH_SIZE) * 2;
 const int numRuns = 10;
 std::vector<long> executionTimes(0);
 bool justKV = true;
 for (int i = 0; i < numRuns; ++i) {
   prepareTags(
       dcCardinality,
       clusterCardinality,
       hostCardinality,
       tag0Cardinality,
       tag1Cardinality);

   int dataRangeInHour = 3;
   //    std::vector<RowVectorPtr> vectors = generateVeloxInputs(
   //        "metricName1",
   //        dataRangeInHour,
   //        dcCardinality * clusterCardinality * hostCardinality *
   //        tag0Cardinality *
   //            tag1Cardinality);

   auto input = generateVeloxInputs(
       "metricName1",
       dataRangeInHour,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);
   if (justKV) {
     for (int i = 0; i < input.size(); i++) {
       int sz = input[i]->childrenSize();
       input[i] = vectorMaker_.rowVector(
           {input[i]->children()[sz - 2], input[i]->children()[sz - 1]});
     }
   }

   std::shared_ptr<const core::PlanNode> plan;
   if (!justKV) {
     plan = PlanBuilder()
                .values(std::move(input))
                //                    .project(
                //                        {"c0",
                //                         "c1",
                //                         "c2",
                //                         "c3",
                //                         "c4",
                //                         "c5",
                //                         "(c6 - c6 % 3600000) as c6",
                //                         "c7"})
                .singleAggregation({"c1", "c2", "c4", "c6"}, {"sum(c7)"})
                .planNode();
   } else {
     plan = PlanBuilder()
                .values(std::move(input))
                //                    .project(
                //                        {"c0",
                //                         "c1",
                //                         "c2",
                //                         "c3",
                //                         "c4",
                //                         "c5",
                //                         "(c6 - c6 % 3600000) as c6",
                //                         "c7"})
                .singleAggregation({"c0"}, {"sum(c1)"})
                .planNode();
   }
   input.clear();

   auto executeStart = std::chrono::high_resolution_clock::now();

   AssertQueryBuilder(plan, duckDbQueryRunner_)
       .maxDrivers(12)
       .assertEmptyResults();
   auto executeTime = std::chrono::high_resolution_clock::now();
   executionTimes.push_back(
       std::chrono::duration<double, std::milli>(executeTime - executeStart)
           .count());
 }
 {
   std::sort(executionTimes.begin(), executionTimes.end());
   int p50Index = executionTimes.size() / 2;
   int p95Index = (int)(executionTimes.size() * 0.95);
   int p99Index = (int)(executionTimes.size() * 0.99);
   std::cout << "\np50 compute time: " << executionTimes[p50Index] << " .";
   std::cout << "\np95 compute time: " << executionTimes[p95Index] << " .";
   std::cout << "\np99 compute time: " << executionTimes[p99Index] << " .";
   executionTimes.clear();
 }

 TestValue::setMode(2);
 VELOX_INPUT_BATCH_SIZE = 1000000;
 VELOX_INPUT_BATCH_SIZE = ((float)VELOX_INPUT_BATCH_SIZE) * 3 * 10;
// VELOX_INPUT_BATCH_SIZE = 10000000;
 for (int i = 0; i < numRuns; ++i) {
   prepareTags(
       dcCardinality,
       clusterCardinality,
       hostCardinality,
       tag0Cardinality,
       tag1Cardinality);

   int dataRangeInHour = 3;
   //    std::vector<RowVectorPtr> vectors = generateVeloxInputs(
   //        "metricName1",
   //        dataRangeInHour,
   //        dcCardinality * clusterCardinality * hostCardinality *
   //        tag0Cardinality *
   //            tag1Cardinality);

   auto input = generateVeloxInputs(
       "metricName1",
       dataRangeInHour,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);
   if (justKV) {
     for (int i = 0; i < input.size(); i++) {
       int sz = input[i]->childrenSize();
       input[i] = vectorMaker_.rowVector(
           {input[i]->children()[sz - 2], input[i]->children()[sz - 1]});
     }
   }

   std::shared_ptr<const core::PlanNode> plan;
   if (!justKV) {
     plan = PlanBuilder()
                .values(std::move(input))
                //                    .project(
                //                        {"c0",
                //                         "c1",
                //                         "c2",
                //                         "c3",
                //                         "c4",
                //                         "c5",
                //                         "(c6 - c6 % 3600000) as c6",
                //                         "c7"})
                .singleAggregation({"c1", "c2", "c4", "c6"}, {"sum(c7)"})
                .planNode();
   } else {
     plan = PlanBuilder()
                .values(std::move(input))
                //                    .project(
                //                        {"c0",
                //                         "c1",
                //                         "c2",
                //                         "c3",
                //                         "c4",
                //                         "c5",
                //                         "(c6 - c6 % 3600000) as c6",
                //                         "c7"})
                .singleAggregation({"c0"}, {"sum(c1)"})
                .planNode();
   }
   input.clear();

   auto executeStart = std::chrono::high_resolution_clock::now();

   AssertQueryBuilder(plan, duckDbQueryRunner_)
       .maxDrivers(12)
       .assertEmptyResults();
   auto executeTime = std::chrono::high_resolution_clock::now();
   executionTimes.push_back(
       std::chrono::duration<double, std::milli>(executeTime - executeStart)
           .count());
 }
 //  TestValue::setMode(3);
 //  VELOX_INPUT_BATCH_SIZE = 1000000;
 //  VELOX_INPUT_BATCH_SIZE = ((float)VELOX_INPUT_BATCH_SIZE) * 2 / 10;
 //  for (int i = 0; i < numRuns; ++i) {
 //    prepareTags(
 //        dcCardinality,
 //        clusterCardinality,
 //        hostCardinality,
 //        tag0Cardinality,
 //        tag1Cardinality);
 //
 //    int dataRangeInHour = 3;
 //    //    std::vector<RowVectorPtr> vectors = generateVeloxInputs(
 //    //        "metricName1",
 //    //        dataRangeInHour,
 //    //        dcCardinality * clusterCardinality * hostCardinality *
 //    tag0Cardinality *
 //    //            tag1Cardinality);
 //
 //    auto input = generateVeloxInputs(
 //        "metricName1",
 //        dataRangeInHour,
 //        dcCardinality * clusterCardinality * hostCardinality *
 //        tag0Cardinality *
 //            tag1Cardinality);
 //    if (justKV) {
 //      for (int i = 0; i < input.size(); i++) {
 //        int sz = input[i]->childrenSize();
 //        input[i] = vectorMaker_.rowVector({input[i]->children()[sz-2],
 //        input[i]->children()[sz-1]});
 //      }
 //    }
 //
 //    std::shared_ptr<const core::PlanNode> plan;
 //    if(!justKV) {
 //      plan = PlanBuilder()
 //                 .values(std::move(input))
 //                 //                    .project(
 //                 //                        {"c0",
 //                 //                         "c1",
 //                 //                         "c2",
 //                 //                         "c3",
 //                 //                         "c4",
 //                 //                         "c5",
 //                 //                         "(c6 - c6 % 3600000) as c6",
 //                 //                         "c7"})
 //                 .singleAggregation({"c1", "c2", "c4", "c6"}, {"sum(c7)"})
 //                 .planNode();
 //    } else {
 //      plan = PlanBuilder()
 //                 .values(std::move(input))
 //                 //                    .project(
 //                 //                        {"c0",
 //                 //                         "c1",
 //                 //                         "c2",
 //                 //                         "c3",
 //                 //                         "c4",
 //                 //                         "c5",
 //                 //                         "(c6 - c6 % 3600000) as c6",
 //                 //                         "c7"})
 //                 .singleAggregation({"c0"}, {"sum(c1)"})
 //                 .planNode();
 //    }
 //    input.clear();
 //
 //    auto executeStart = std::chrono::high_resolution_clock::now();
 //
 //    AssertQueryBuilder(plan,
 //    duckDbQueryRunner_).maxDrivers(12).assertEmptyResults(); auto
 //    executeTime = std::chrono::high_resolution_clock::now();
 //    executionTimes.push_back(
 //        std::chrono::duration<double, std::milli>(executeTime -
 //        executeStart)
 //            .count());
 //  }
 std::sort(executionTimes.begin(), executionTimes.end());
 int p50Index = executionTimes.size() / 2;
 int p95Index = (int)(executionTimes.size() * 0.95);
 int p99Index = (int)(executionTimes.size() * 0.99);
 std::cout << "\np50 compute time: " << executionTimes[p50Index] << " .";
 std::cout << "\np95 compute time: " << executionTimes[p95Index] << " .";
 std::cout << "\np99 compute time: " << executionTimes[p99Index] << " .";
}

TEST_F(HashAggGroupProbePerfTest, DISABLED_aggregationNoFilterWithMap) {
// TestValue ::setMode(3);
 VELOX_INPUT_BATCH_SIZE = 1000000;
 const int numRuns = 1;
 bool justKV = true;
 std::vector<long> executionTimes(0);
 for (int i = 0; i < numRuns; ++i) {
   prepareTags(
       dcCardinality,
       clusterCardinality,
       hostCardinality,
       tag0Cardinality,
       tag1Cardinality);

   int dataRangeInHour = 3;
   //    std::vector<RowVectorPtr> vectors = generateVeloxInputs(
   //        "metricName1",
   //        dataRangeInHour,
   //        dcCardinality * clusterCardinality * hostCardinality *
   //        tag0Cardinality *
   //            tag1Cardinality);

   auto input = generateVeloxInputsWithMap(
       "metricName1",
       dataRangeInHour,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);

   if (justKV) {
     for (int i = 0; i < input.size(); i++) {
       auto sz = input[i]->children().size();
       input[i] = vectorMaker_.rowVector({input[i]->children()[sz - 1]});
     }
   }

   std::shared_ptr<const core::PlanNode> plan;

   if (justKV) {
     plan = PlanBuilder()
                .values(std::move(input))
                //                      .project(
                //                          {"c0"})
                .singleAggregation({}, {"map_union_sum(c0) as c0"})
                //                      .unnest({}, {"c0"})
                .planNode();
   } else {
     plan = PlanBuilder()
                .values(std::move(input))
                .project({"c0", "c1", "c2", "c3", "c4", "c5", "c6"})
                .singleAggregation(
                    {"c1", "c2", "c4"}, {"map_union_sum(c6) as c6"})
                .unnest({"c1"}, {"c6"})
                .planNode();
   }
   input.clear();

   auto executeStart = std::chrono::high_resolution_clock::now();

   AssertQueryBuilder(plan, duckDbQueryRunner_)
       .maxDrivers(12)
       .assertEmptyResults();
   auto executeTime = std::chrono::high_resolution_clock::now();
   executionTimes.push_back(
       std::chrono::duration<double, std::milli>(executeTime - executeStart)
           .count());
 }
 std::sort(executionTimes.begin(), executionTimes.end());

 int p50Index = executionTimes.size() / 2;
 int p95Index = (int)(executionTimes.size() * 0.95);
 int p99Index = (int)(executionTimes.size() * 0.99);
 std::cout << "\np50 compute time: " << executionTimes[p50Index] << " .";
 std::cout << "\np95 compute time: " << executionTimes[p95Index] << " .";
 std::cout << "\np99 compute time: " << executionTimes[p99Index] << " .";
}

TEST_F(HashAggGroupProbePerfTest, DISABLED_aggregationWithFilter) {
 const int numRuns = 15;
 std::vector<long> executionTimes(0);
 for (int i = 0; i < numRuns; ++i) {
   prepareTags(
       dcCardinality,
       clusterCardinality,
       hostCardinality,
       tag0Cardinality,
       tag1Cardinality);

   int dataRangeInHour = 3;
   std::vector<RowVectorPtr> vectors = generateVeloxInputs(
       "metricName1",
       dataRangeInHour,
       dcCardinality * clusterCardinality * hostCardinality * tag0Cardinality *
           tag1Cardinality);

   auto plan =
       PlanBuilder(pool_.get())
           .values(vectors)
           .filter(
               "c1 IN ('dc_0') AND c2 IN ('cluster_0','cluster_1') AND c4 in ('tag0_0')")
           .project(
               {"c0",
                "c1",
                "c2",
                "c3",
                "c4",
                "c5",
                "(c6 - c6 % 3600000) as c6",
                "c7"})
           .singleAggregation({"c1", "c2", "c4"}, {"sum(c7)"})
           .planNode();

   auto executeStart = std::chrono::high_resolution_clock::now();

   AssertQueryBuilder(plan, duckDbQueryRunner_)
       .maxDrivers(12)
       .assertEmptyResults();
   auto executeTime = std::chrono::high_resolution_clock::now();
   executionTimes.push_back(
       std::chrono::duration<double, std::milli>(executeTime - executeStart)
           .count());
 }
 std::sort(executionTimes.begin(), executionTimes.end());

 int p50Index = executionTimes.size() / 2;
 int p95Index = (int)(executionTimes.size() * 0.95);
 int p99Index = (int)(executionTimes.size() * 0.99);
 std::cout << "\np50 compute time: " << executionTimes[p50Index] << " .";
 std::cout << "\np95 compute time: " << executionTimes[p95Index] << " .";
 std::cout << "\np99 compute time: " << executionTimes[p99Index] << " .";
}
