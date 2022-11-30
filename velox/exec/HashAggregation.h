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
#pragma once

#include "velox/exec/GroupingSet.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

static uint32_t powersOfTwo[] = {
    0,
    1 << 0,
    1 << 1,
    1 << 2,
    1 << 3,
    1 << 4,
    1 << 5,
    1 << 6,
    1 << 7,
    1 << 8,
    1 << 9,
    1 << 10};
static uint32_t powersOfTwoLength = sizeof(powersOfTwo) / sizeof(uint32_t);

class HashAggregation : public Operator {
 public:
  HashAggregation(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::AggregationNode>& aggregationNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && !partialFull_;
  }

  void noMoreInput() override {
    groupingSet_->noMoreInput();
    Operator::noMoreInput();
  }

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    groupingSet_.reset();
  }

 private:
  // Checks if the spilling is allowed for this hash aggregation. As for now, we
  // don't allow spilling for distinct aggregation
  // (https://github.com/facebookincubator/velox/issues/3263) and pre-grouped
  // aggregation (https://github.com/facebookincubator/velox/issues/3264). We
  // will add support later to re-enable.
  bool isSpillAllowed(
      const std::shared_ptr<const core::AggregationNode>& node) const;

  // Think of HashAggregation of a stream of batches, some have low cardinality
  // and some have high cardinality.
  // This class checks a batch's cardinality and decide heuristically whether
  // the following K batches are likely to be high/low cardinality and then
  // enables/disables hastable grouping in GroupingSet accordingly.
  // Value of K is selected using exponential back-off.
  // This class maintains all state (counter, current exponent) associated with
  // exponential back-off; users of this class simply needs to call
  // executeIteration after each batch and this class will take care of whether
  // hashing/grouping will be skipped and how many batches till the next check.
  // See AggregationTest for examples of live behavior.
  class SkipPartialAggregationGroupingEvaluator {
   public:
    SkipPartialAggregationGroupingEvaluator(const double goodPct)
        : partialAggregationGoodPct_(goodPct) {}

    void executeIteration(
        double percent,
        GroupingSet& groupingSet,
        folly::Synchronized<OperatorStats>& stats) {
      if (iterationsUntilNextEvaluation() > 1) {
        consumeOneIteration();
        return;
      }
      if (iterationsUntilNextEvaluation() == 1) {
        groupingSet.enableGrouping();
        consumeOneIteration();
        return;
      }
      if (iterationsUntilNextEvaluation() == 0) {
        stats.wlock()->addRuntimeStat(
            "disablePartialAggregationGroupingEvaluation",
            RuntimeCounter(percent));
        if (percent < partialAggregationGoodPct_) {
          decreaseInterval();
        } else {
          increaseInterval();
        }
        if (iterationsUntilNextEvaluation() != 0) {
          groupingSet.disableGrouping();
        }
      }
    }

   private:
    int iterationsUntilNextEvaluation() {
      return iterationsUntilNextEvaluation_;
    }
    void consumeOneIteration() {
      iterationsUntilNextEvaluation_--;
    }
    void increaseInterval() {
      if (intervalIndex_ + 1 < powersOfTwoLength) {
        ++intervalIndex_;
      }
      iterationsUntilNextEvaluation_ = powersOfTwo[intervalIndex_];
    }
    void decreaseInterval() {
      if (intervalIndex_ > 0) {
        --intervalIndex_;
      }
      iterationsUntilNextEvaluation_ = powersOfTwo[intervalIndex_];
    }

    const double partialAggregationGoodPct_;
    uint32_t iterationsUntilNextEvaluation_ = 0;
    uint32_t intervalIndex_ = 0;
  };

  void prepareOutput(vector_size_t size);

  // Invoked to reset partial aggregation state if it was full and has been
  // flushed.
  void resetPartialOutputIfNeed();

  // Invoked on partial output flush to try to bump up the partial aggregation
  // memory usage if it needs. 'aggregationPct' is the ratio between the number
  // of output rows and the number of input rows as a percentage. It is a
  // measure of the effectiveness of the partial aggregation.
  void maybeIncreasePartialAggregationMemoryUsage(double aggregationPct);

  bool considerSkipPartialAggregationGrouping();

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  const bool isPartialOutput_;
  const bool isDistinct_;
  const bool isGlobal_;
  const std::shared_ptr<memory::MemoryUsageTracker> memoryTracker_;
  const double partialAggregationGoodPct_;
  const int64_t maxExtendedPartialAggregationMemoryUsage_;
  const std::optional<Spiller::Config> spillConfig_;

  int64_t maxPartialAggregationMemoryUsage_;
  std::unique_ptr<GroupingSet> groupingSet_;

  bool partialFull_ = false;
  bool newDistincts_ = false;
  bool finished_ = false;
  RowContainerIterator resultIterator_;
  bool pushdownChecked_ = false;
  bool mayPushdown_ = false;

  bool isRawInput_;
  bool emptyPreGroupedKeyChannels_;
  bool allowSkipPartialAggregationGrouping_;
  bool hasVarianceAggregation_;
  bool hasMasks_;

  /// Count the number of input rows. It is reset on partial aggregation output
  /// flush.
  int64_t numInputRows_ = 0;
  /// Count the number of output rows. It is reset on partial aggregation output
  /// flush.
  int64_t numOutputRows_ = 0;

  /// Possibly reusable output vector.
  RowVectorPtr output_;

  SkipPartialAggregationGroupingEvaluator
      disablePartialAggregationGroupingEvaluator_;
};

} // namespace facebook::velox::exec
