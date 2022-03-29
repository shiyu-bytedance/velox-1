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

#include <cmath>
#include <queue>
#include <type_traits>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions::kll {

namespace detail {

uint32_t computeTotalCapacity(uint16_t k, uint8_t numLevels);

uint16_t levelCapacity(uint16_t k, uint8_t numLevels, uint8_t height);

// Collect elements in odd or even positions to first half of buf.
template <typename T, typename RandomBit>
void randomlyHalveDown(
    T* buf,
    uint32_t start,
    uint32_t length,
    RandomBit& randomBit) {
  VELOX_DCHECK_EQ(length & 1, 0);
  const uint32_t halfLength = length / 2;
  const uint32_t offset = randomBit();
  uint32_t j = start + offset;
  for (uint32_t i = start; i < start + halfLength; i++) {
    buf[i] = buf[j];
    j += 2;
  }
}

// Collect elements in odd or even positions to second half of buf.
template <typename T, typename RandomBit>
void randomlyHalveUp(
    T* buf,
    uint32_t start,
    uint32_t length,
    RandomBit& randomBit) {
  VELOX_DCHECK_EQ(length & 1, 0);
  const uint32_t halfLength = length / 2;
  const uint32_t offset = randomBit();
  uint32_t j = (start + length) - 1 - offset;
  for (uint32_t i = (start + length) - 1; i >= (start + halfLength); i--) {
    buf[i] = buf[j];
    j -= 2;
  }
}

// Merge 2 sorted ranges:
//   buf[startA] to buf[startA + lenA]
//   buf[startB] to buf[startB + lenB]
//
// The target range starting buf[startC] could overlap with range B,
// so we cannot use std::merge here.
template <typename T, typename C>
void mergeOverlap(
    T* buf,
    uint32_t startA,
    uint32_t lenA,
    uint32_t startB,
    uint32_t lenB,
    uint32_t startC,
    C compare) {
  const uint32_t limA = startA + lenA;
  const uint32_t limB = startB + lenB;
  VELOX_DCHECK_LE(limA, startC);
  VELOX_DCHECK_LE(startC + lenA, startB);
  uint32_t a = startA;
  uint32_t b = startB;
  uint32_t c = startC;
  while (a < limA && b < limB) {
    if (compare(buf[a], buf[b])) {
      buf[c++] = buf[a++];
    } else {
      buf[c++] = buf[b++];
    }
  }
  while (a < limA) {
    buf[c++] = buf[a++];
  }
  while (b < limB) {
    buf[c++] = buf[b++];
  }
}

// Return floor(log2(p/q)).
uint8_t floorLog2(uint64_t p, uint64_t q);

struct CompressResult {
  uint8_t finalNumLevels;
  uint32_t finalCapacity;
  uint32_t finalNumItems;
};

/*
 * Here is what we do for each level:
 * If it does not need to be compacted, then simply copy it over.
 *
 * Otherwise, it does need to be compacted, so...
 *   Copy zero or one guy over.
 *   If the level above is empty, halve up.
 *   Else the level above is nonempty, so...
 *        halve down, then merge up.
 *   Adjust the boundaries of the level above.
 *
 * It can be proved that generalCompress returns a sketch that satisfies the
 * space constraints no matter how much data is passed in.
 * All levels except for level zero must be sorted before calling this, and will
 * still be sorted afterwards.
 * Level zero is not required to be sorted before, and may not be sorted
 * afterwards.
 */
template <typename T, typename C, typename RandomBit>
CompressResult generalCompress(
    uint16_t k,
    uint8_t numLevelsIn,
    T* items,
    uint32_t* inLevels,
    uint32_t* outLevels,
    bool isLevelZeroSorted,
    RandomBit& randomBit) {
  VELOX_DCHECK_GT(numLevelsIn, 0);
  uint8_t currentNumLevels = numLevelsIn;
  // `currentItemCount` decreases with each compaction.
  uint32_t currentItemCount = inLevels[numLevelsIn] - inLevels[0];
  // Increases if we add levels.
  uint32_t targetItemCount = computeTotalCapacity(k, currentNumLevels);
  outLevels[0] = 0;
  for (uint8_t level = 0; level < currentNumLevels; ++level) {
    // If we are at the current top level, add an empty level above it
    // for convenience, but do not increment currentNumLevels until later.
    if (level == (currentNumLevels - 1)) {
      inLevels[level + 2] = inLevels[level + 1];
    }
    const auto rawBeg = inLevels[level];
    const auto rawLim = inLevels[level + 1];
    const auto rawPop = rawLim - rawBeg;
    if ((currentItemCount < targetItemCount) ||
        (rawPop < levelCapacity(k, currentNumLevels, level))) {
      // Move level over as is.
      // Make sure we are not moving data upwards.
      VELOX_DCHECK_GE(rawBeg, outLevels[level]);
      std::move(&items[rawBeg], &items[rawLim], &items[outLevels[level]]);
      outLevels[level + 1] = outLevels[level] + rawPop;
    } else {
      // The sketch is too full AND this level is too full, so we compact it.
      // Note: this can add a level and thus change the sketches capacities.
      const auto popAbove = inLevels[level + 2] - rawLim;
      const bool oddPop = rawPop & 1;
      const auto adjBeg = rawBeg + oddPop;
      const auto adjPop = rawPop - oddPop;
      const auto halfAdjPop = adjPop / 2;

      if (oddPop) { // Move one guy over.
        items[outLevels[level]] = std::move(items[rawBeg]);
        outLevels[level + 1] = outLevels[level] + 1;
      } else { // Even number of items in this level.
        outLevels[level + 1] = outLevels[level];
      }

      // Level zero might not be sorted, so we must sort it if we wish
      // to compact it.
      if ((level == 0) && !isLevelZeroSorted) {
        std::sort(&items[adjBeg], &items[adjBeg + adjPop], C());
      }

      if (popAbove == 0) { // Level above is empty, so halve up.
        randomlyHalveUp(items, adjBeg, adjPop, randomBit);
      } else { // Level above is nonempty, so halve down, then merge up.
        randomlyHalveDown(items, adjBeg, adjPop, randomBit);
        mergeOverlap(
            items,
            adjBeg,
            halfAdjPop,
            rawLim,
            popAbove,
            adjBeg + halfAdjPop,
            C());
      }

      // Track the fact that we just eliminated some data.
      currentItemCount -= halfAdjPop;

      // Adjust the boundaries of the level above.
      inLevels[level + 1] = inLevels[level + 1] - halfAdjPop;

      // Increment num levels if we just compacted the old top level
      // this creates some more capacity (the size of the new bottom
      // level).
      if (level == (currentNumLevels - 1)) {
        ++currentNumLevels;
        targetItemCount += levelCapacity(k, currentNumLevels, 0);
      }
    }
  }
  VELOX_DCHECK_EQ(outLevels[currentNumLevels] - outLevels[0], currentItemCount);
  return {currentNumLevels, targetItemCount, currentItemCount};
}

uint64_t sumSampleWeights(uint8_t numLevels, const uint32_t* levels);

} // namespace detail

template <typename T, typename A, typename C>
KllSketch<T, A, C>::KllSketch(uint16_t k, const A& allocator, uint32_t seed)
    : k_(k),
      allocator_(allocator),
      randomBit_(seed),
      n_(0),
      items_(k, allocator),
      levels_(2, k, allocator),
      isLevelZeroSorted_(false) {}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::insert(T value) {
  if (n_ == 0) {
    minValue_ = maxValue_ = value;
  } else {
    minValue_ = std::min(minValue_, value, C());
    maxValue_ = std::max(maxValue_, value, C());
  }
  items_[insertPosition()] = value;
}

template <typename T, typename A, typename C>
uint32_t KllSketch<T, A, C>::insertPosition() {
  if (levels_[0] == 0) {
    const uint8_t level = findLevelToCompact();

    // It is important to add the new top level right here. Be aware
    // that this operation grows the buffer and shifts the data and
    // also the boundaries of the data and grows the levels array.
    if (level == numLevels() - 1) {
      addEmptyTopLevelToCompletelyFullSketch();
    }

    const uint32_t rawBeg = levels_[level];
    const uint32_t rawLim = levels_[level + 1];
    // +2 is OK because we already added a new top level if necessary.
    const uint32_t popAbove = levels_[level + 2] - rawLim;
    const uint32_t rawPop = rawLim - rawBeg;
    const bool oddPop = rawPop & 1;
    const uint32_t adjBeg = rawBeg + oddPop;
    const uint32_t adjPop = rawPop - oddPop;
    const uint32_t halfAdjPop = adjPop / 2;

    // Level zero might not be sorted, so we must sort it if we wish
    // to compact it.
    if (level == 0 && !isLevelZeroSorted_) {
      std::sort(&items_[adjBeg], &items_[adjBeg + adjPop], C());
    }
    if (popAbove == 0) {
      detail::randomlyHalveUp(&items_[0], adjBeg, adjPop, randomBit_);
    } else {
      detail::randomlyHalveDown(&items_[0], adjBeg, adjPop, randomBit_);
      detail::mergeOverlap(
          &items_[0],
          adjBeg,
          halfAdjPop,
          rawLim,
          popAbove,
          adjBeg + halfAdjPop,
          C());
    }
    levels_[level + 1] -= halfAdjPop; // Adjust boundaries of the level above.
    if (oddPop) {
      // The current level now contains one item.
      levels_[level] = levels_[level + 1] - 1;
      if (levels_[level] != rawBeg) {
        // Namely this leftover guy.
        items_[levels_[level]] = std::move(items_[rawBeg]);
      }
    } else {
      levels_[level] = levels_[level + 1]; // The current level is now empty.
    }

    // Verify that we freed up halfAdjPop array slots just below the
    // current level.
    VELOX_DCHECK_EQ(levels_[level], rawBeg + halfAdjPop);

    // Finally, we need to shift up the data in the levels below
    // so that the freed-up space can be used by level zero.
    if (level > 0) {
      const uint32_t amount = rawBeg - levels_[0];
      std::move_backward(
          &items_[levels_[0]],
          &items_[levels_[0] + amount],
          &items_[levels_[0] + halfAdjPop + amount]);
      for (uint8_t lvl = 0; lvl < level; lvl++) {
        levels_[lvl] += halfAdjPop;
      }
    }
  }
  ++n_;
  isLevelZeroSorted_ = false;
  return --levels_[0];
}

template <typename T, typename A, typename C>
int KllSketch<T, A, C>::findLevelToCompact() const {
  for (int level = 0;; ++level) {
    VELOX_DCHECK_LT(level + 1, levels_.size());
    const uint32_t pop = levels_[level + 1] - levels_[level];
    const uint32_t cap = detail::levelCapacity(k_, numLevels(), level);
    if (pop >= cap) {
      return level;
    }
  }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::addEmptyTopLevelToCompletelyFullSketch() {
  const uint32_t curTotalCap = levels_.back();

  // Make sure that we are following a certain growth scheme.
  VELOX_DCHECK_EQ(levels_[0], 0);
  VELOX_DCHECK_EQ(items_.size(), curTotalCap);

  const uint32_t deltaCap = detail::levelCapacity(k_, numLevels() + 1, 0);
  const uint32_t newTotalCap = curTotalCap + deltaCap;
  items_.resize(newTotalCap);
  std::move_backward(
      items_.begin(), items_.begin() + curTotalCap, items_.end());

  // This loop includes the old "extra" index at the top.
  for (auto& lvl : levels_) {
    lvl += deltaCap;
  }
  VELOX_DCHECK_EQ(levels_.back(), newTotalCap);
  levels_.push_back(newTotalCap);
}

template <typename T, typename A, typename C>
T KllSketch<T, A, C>::estimateQuantile(double fraction) {
  T ans;
  estimateQuantiles(folly::Range(&fraction, 1), &ans);
  return ans;
}

template <typename T, typename A, typename C>
template <typename Iter>
std::vector<T, A> KllSketch<T, A, C>::estimateQuantiles(
    const folly::Range<Iter>& fractions) {
  std::vector<T, A> ans(fractions.size(), T{}, allocator_);
  estimateQuantiles(fractions, ans.data());
  return ans;
}

template <typename T, typename A, typename C>
template <typename Iter>
void KllSketch<T, A, C>::estimateQuantiles(
    const folly::Range<Iter>& fractions,
    T* out) {
  VELOX_USER_CHECK_GT(n_, 0, "estimateQuantiles called on empty sketch");
  if (!isLevelZeroSorted_) {
    std::sort(&items_[levels_[0]], &items_[levels_[1]], C());
    isLevelZeroSorted_ = true;
  }
  using Entry = typename std::pair<T, uint64_t>;
  using AllocEntry =
      typename std::allocator_traits<A>::template rebind_alloc<Entry>;
  std::vector<Entry, AllocEntry> entries(allocator_);
  entries.reserve(levels_.back());
  for (int level = 0; level < numLevels(); ++level) {
    auto oldLen = entries.size();
    for (int i = levels_[level]; i < levels_[level + 1]; ++i) {
      entries.emplace_back(items_[i], 1 << level);
    }
    if (oldLen > 0) {
      std::inplace_merge(
          entries.begin(),
          entries.begin() + oldLen,
          entries.end(),
          [](auto& x, auto& y) { return C()(x.first, y.first); });
    }
  }
  uint64_t totalWeight = 0;
  for (auto& [_, w] : entries) {
    auto newTotalWeight = totalWeight + w;
    // Only count the number of elements strictly smaller.
    w = totalWeight;
    totalWeight = newTotalWeight;
  }
  int i = 0;
  for (auto& q : fractions) {
    VELOX_CHECK_GE(q, 0.0);
    VELOX_CHECK_LE(q, 1.0);
    if (fractions[i] == 0.0) {
      out[i++] = minValue_;
      continue;
    }
    if (fractions[i] == 1.0) {
      out[i++] = maxValue_;
      continue;
    }
    uint64_t maxWeight = q * totalWeight;
    auto it = std::lower_bound(
        entries.begin(),
        entries.end(),
        std::make_pair(T{}, maxWeight),
        [](auto& x, auto& y) { return x.second < y.second; });
    if (it == entries.end()) {
      out[i++] = entries.back().first;
    } else {
      out[i++] = it->first;
    }
  }
}

template <typename T, typename A, typename C>
template <typename Iter>
void KllSketch<T, A, C>::merge(const folly::Range<Iter>& others) {
  auto newN = n_;
  for (auto& other : others) {
    if (other.n_ == 0) {
      continue;
    }
    if (newN == 0) {
      minValue_ = other.minValue_;
      maxValue_ = other.maxValue_;
    } else {
      minValue_ = std::min(minValue_, other.minValue_, C());
      maxValue_ = std::max(maxValue_, other.maxValue_, C());
    }
    newN += other.n_;
  }
  if (newN == n_) {
    return;
  }
  // Merge bottom level.
  for (auto& other : others) {
    for (uint32_t j = other.levels_[0]; j < other.levels_[1]; ++j) {
      items_[insertPosition()] = other.items_[j];
    }
  }
  // Merge higher levels.
  auto tmpNumItems = getNumRetained();
  auto provisionalNumLevels = numLevels();
  for (auto& other : others) {
    if (other.numLevels() >= 2) {
      tmpNumItems += other.levels_.back() - other.levels_[1];
      provisionalNumLevels = std::max(provisionalNumLevels, other.numLevels());
    }
  }
  if (tmpNumItems > getNumRetained()) {
    std::vector<T, A> workbuf(tmpNumItems);
    const uint8_t ub = 1 + detail::floorLog2(newN, 1);
    const size_t workLevelsSize = ub + 2;
    std::vector<uint32_t, AllocU32> worklevels(workLevelsSize, 0, allocator_);
    std::vector<uint32_t, AllocU32> outlevels(workLevelsSize, 0, allocator_);
    // Populate work arrays.
    worklevels[0] = 0;
    std::move(&items_[levels_[0]], &items_[levels_[1]], &workbuf[0]);
    worklevels[1] = safeLevelSize(0);
    // Merge each level, each level in all sketches are already sorted.
    for (uint8_t lvl = 1; lvl < provisionalNumLevels; ++lvl) {
      using Entry = std::pair<const T*, const T*>;
      using AllocEntry =
          typename std::allocator_traits<A>::template rebind_alloc<Entry>;
      auto gt = [](const Entry& x, const Entry& y) {
        return C()(*y.first, *x.first);
      };
      std::priority_queue<Entry, std::vector<Entry, AllocEntry>, decltype(gt)>
          pq(gt, allocator_);
      if (auto sz = safeLevelSize(lvl); sz > 0) {
        pq.emplace(&items_[levels_[lvl]], &items_[levels_[lvl] + sz]);
      }
      for (auto& other : others) {
        if (auto sz = other.safeLevelSize(lvl); sz > 0) {
          pq.emplace(
              &other.items_[other.levels_[lvl]],
              &other.items_[other.levels_[lvl] + sz]);
        }
      }
      int outIndex = worklevels[lvl];
      while (!pq.empty()) {
        auto [s, t] = pq.top();
        pq.pop();
        workbuf[outIndex++] = *s++;
        if (s < t) {
          pq.emplace(s, t);
        }
      }
      worklevels[lvl + 1] = outIndex;
    }
    auto result = detail::generalCompress<T, C>(
        k_,
        provisionalNumLevels,
        workbuf.data(),
        worklevels.data(),
        outlevels.data(),
        isLevelZeroSorted_,
        randomBit_);
    VELOX_DCHECK_LE(result.finalNumLevels, ub);
    // Now we need to transfer the results back into "this" sketch.
    items_.resize(result.finalCapacity);
    const auto freeSpaceAtBottom = result.finalCapacity - result.finalNumItems;
    std::move(
        &workbuf[outlevels[0]],
        &workbuf[outlevels[0] + result.finalNumItems],
        &items_[freeSpaceAtBottom]);
    levels_.resize(result.finalNumLevels + 1);
    const auto offset = freeSpaceAtBottom - outlevels[0];
    for (unsigned lvl = 0; lvl < levels_.size(); ++lvl) {
      levels_[lvl] = outlevels[lvl] + offset;
    }
  }
  n_ = newN;
  VELOX_DCHECK_EQ(detail::sumSampleWeights(numLevels(), levels_.data()), n_);
}

} // namespace facebook::velox::functions::kll
