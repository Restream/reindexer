#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <random>
#include <vector>

#include "core/id_type.h"
#include "core/idset/idset.h"
#include "core/index/keyentry.h"
#include "core/nsselecter/selectiterator.h"
#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"
#include "gtests/tests/gtest_cout.h"

namespace reindexer_tests {
namespace {

using reindexer::IdSet;
using reindexer::IdSetPlain;
using reindexer::IdType;
using reindexer::IsDistinct_False;
using reindexer::KeyEntry;
using reindexer::SelectIterator;
using reindexer::SelectKeyResult;
using reindexer::SingleSelectKeyResult;
using reindexer::make_intrusive;

IdType RID(int v) { return IdType::FromNumber(v); }

SelectIterator MakeIterator(SelectKeyResult&& skr) { return SelectIterator(std::move(skr), IsDistinct_False, "selectiterator_test", 0); }

/// Keeps KeyEntry storage alive while SelectIterator holds spans/pointers into it.
class [[nodiscard]] SelectIteratorHarness {
public:
	RX_NO_INLINE void AddPlainIds(std::initializer_list<int> ids) {
		auto ptr = make_intrusive<reindexer::intrusive_atomic_rc_wrapper<IdSetPlain>>();
		for (int id : ids) {
			std::ignore = ptr->Add(RID(id), 0);
		}
		ptr->Commit(0);
		skr_.emplace_back(SingleSelectKeyResult(std::move(ptr)));
	}

	RX_NO_INLINE void AddPlainIds(const std::vector<int>& ids) {
		auto ptr = make_intrusive<reindexer::intrusive_atomic_rc_wrapper<IdSetPlain>>();
		for (int id : ids) {
			std::ignore = ptr->Add(RID(id), 0);
		}
		skr_.emplace_back(SingleSelectKeyResult(std::move(ptr)));
	}

	RX_NO_INLINE void AddPlainIdsUnordered(std::initializer_list<int> ids) {
		auto ptr = make_intrusive<reindexer::intrusive_atomic_rc_wrapper<IdSetPlain>>();
		for (int id : ids) {
			ptr->AddUnordered(RID(id));
		}
		skr_.emplace_back(SingleSelectKeyResult(std::move(ptr)));
	}

	RX_NO_INLINE void AddBtreeIds(const std::vector<int>& ids) {
		btreeKeys_.emplace_back();
		auto& entry = btreeKeys_.back();
		for (int id : ids) {
			std::ignore = entry.Unsorted().Add(RID(id), 0);
		}
		ASSERT_NE(nullptr, entry.Unsorted().BTree()) << "IdSet must use btree storage";
		skr_.emplace_back(SingleSelectKeyResult(entry, SortType{0}));
	}

	RX_NO_INLINE void AddRange(int begin, int endExclusive) { skr_.emplace_back(SingleSelectKeyResult(RID(begin), RID(endExclusive))); }

	void SetDeferedExplicitSort(bool v = true) noexcept { skr_.deferedExplicitSort = v; }

	SelectIterator BuildIterator() && { return MakeIterator(std::move(skr_)); }

private:
	SelectKeyResult skr_;
	std::vector<KeyEntry<IdSet>> btreeKeys_;
};

std::vector<int> MakeDenseRange(int begin, int endExclusive) {
	std::vector<int> ids;
	ids.reserve(static_cast<size_t>(endExclusive - begin));
	for (int id = begin; id < endExclusive; ++id) {
		ids.push_back(id);
	}
	return ids;
}

std::vector<int> MergeForwardExpected(const std::vector<std::vector<int>>& partitions) {
	std::vector<size_t> pos(partitions.size(), 0);
	std::vector<int> merged;
	while (true) {
		int minVal = std::numeric_limits<int>::max();
		bool any = false;
		for (size_t i = 0; i < partitions.size(); ++i) {
			if (pos[i] < partitions[i].size()) {
				any = true;
				minVal = std::min(minVal, partitions[i][pos[i]]);
			}
		}
		if (!any) {
			break;
		}
		merged.push_back(minVal);
		for (size_t i = 0; i < partitions.size(); ++i) {
			if (pos[i] < partitions[i].size() && partitions[i][pos[i]] == minVal) {
				++pos[i];
			}
		}
	}
	return merged;
}

std::vector<int> MergeReverseExpected(const std::vector<std::vector<int>>& partitions) {
	auto forward = MergeForwardExpected(partitions);
	std::reverse(forward.begin(), forward.end());
	return forward;
}

std::vector<int> CollectViaNext(SelectIterator& it, bool reverse, int maxIterations = 100000) {
	it.Start(reverse, maxIterations);
	std::vector<int> out;
	IdType hint = reverse ? IdType::Max() : IdType::Min();
	int steps = 0;
	while (!it.End()) {
		if (++steps > maxIterations) {
			ADD_FAILURE() << "SelectIterator did not finish after " << maxIterations << " steps, last value=" << it.Val().ToNumber();
			break;
		}
		if (!it.Next(hint)) {
			break;
		}
		out.push_back(it.Val().ToNumber());
		hint = it.Val();
	}
	return out;
}

void AssertEquivalentTraversal(SelectIterator& reference, SelectIterator& candidate, SelectIterator::Type expectedType, bool reverse,
							   int maxIterations = 100000) {
	reference.Start(reverse, maxIterations);
	const auto refType = reference.GetType();
	ASSERT_EQ(expectedType, refType);

	candidate.Start(reverse, maxIterations);
	ASSERT_EQ(refType, candidate.GetType());

	const auto refSeq = CollectViaNext(reference, reverse, maxIterations);
	const auto candSeq = CollectViaNext(candidate, reverse, maxIterations);
	EXPECT_EQ(refSeq, candSeq);
}

void AssertCompareMatchesNextSequence(const std::vector<std::vector<int>>& partitions, bool reverse, int maxIterations = 100000) {
	const auto expected = reverse ? MergeReverseExpected(partitions) : MergeForwardExpected(partitions);

	SelectIteratorHarness nextHarness;
	for (const auto& part : partitions) {
		nextHarness.AddPlainIds(part);
	}
	auto nextIt = std::move(nextHarness).BuildIterator();
	ASSERT_EQ(expected, CollectViaNext(nextIt, reverse, maxIterations));

	SelectIteratorHarness compareHarness;
	for (const auto& part : partitions) {
		compareHarness.AddPlainIds(part);
	}
	auto compareIt = std::move(compareHarness).BuildIterator();
	compareIt.Start(reverse, maxIterations);

	for (int id : expected) {
		ASSERT_TRUE(compareIt.Compare(RID(id))) << "id=" << id;
		EXPECT_EQ(id, compareIt.Val().ToNumber());
	}

	if (!expected.empty()) {
		const int absent = expected.front() + 1000000;
		EXPECT_FALSE(compareIt.Compare(RID(absent)));
	}
}

int LastValNum(IdType v) noexcept { return v.ToNumber(); }

int ForwardBoundFromHint(int lastValNum, int minHintNum) noexcept { return (minHintNum > lastValNum) ? (minHintNum - 1) : lastValNum; }

int ReverseBoundFromHint(int lastValNum, int maxHintNum) noexcept { return (maxHintNum < lastValNum) ? (maxHintNum + 1) : lastValNum; }

std::optional<int> FirstMergedIdGreaterThan(const std::vector<std::vector<int>>& partitions, int bound) {
	const auto merged = MergeForwardExpected(partitions);
	for (int id : merged) {
		if (id > bound) {
			return id;
		}
	}
	return std::nullopt;
}

std::optional<int> FirstMergedIdLessThan(const std::vector<std::vector<int>>& partitions, int bound) {
	const auto merged = MergeForwardExpected(partitions);
	for (auto it = merged.rbegin(); it != merged.rend(); ++it) {
		if (*it < bound) {
			return *it;
		}
	}
	return std::nullopt;
}

void ExpectNextFwd(SelectIterator& it, IdType minHint, std::optional<int> expectedId) {
	const bool hasNext = it.Next(minHint);
	if (expectedId) {
		ASSERT_TRUE(hasNext);
		EXPECT_EQ(*expectedId, it.Val().ToNumber());
	} else {
		EXPECT_FALSE(hasNext);
		EXPECT_TRUE(it.End());
	}
}

void ExpectNextRev(SelectIterator& it, IdType maxHint, std::optional<int> expectedId) {
	const bool hasNext = it.Next(maxHint);
	if (expectedId) {
		ASSERT_TRUE(hasNext);
		EXPECT_EQ(*expectedId, it.Val().ToNumber());
	} else {
		EXPECT_FALSE(hasNext);
		EXPECT_TRUE(it.End());
	}
}

std::vector<int> MakeBtreeSizedIds(int count, int first = 0) {
	std::vector<int> ids;
	ids.reserve(static_cast<size_t>(count));
	for (int i = 0; i < count; ++i) {
		ids.push_back(first + i * 3);
	}
	return ids;
}

std::vector<int> UniqueSortedIds(std::vector<int> ids) {
	std::sort(ids.begin(), ids.end());
	ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
	return ids;
}

void GenerateRandomIdSets(std::mt19937& rng, SelectIteratorHarness& harness, std::vector<std::vector<int>>* partitions) {
	enum class [[nodiscard]] RandomIdSetKind : uint8_t { Range, Plain, Btree };
	constexpr int kMinIdSets = 5;
	constexpr int kMaxIdSets = 100;
	constexpr int kMinIdSetSize = 1;
	constexpr int kMaxIdSetSize = 2000;
	constexpr int kMinBtreeSize = 257;
	constexpr int kMaxIdValue = 500'000;

	std::uniform_int_distribution<int> idSetCountDist(kMinIdSets, kMaxIdSets);
	std::uniform_int_distribution<int> sizeDist(kMinIdSetSize, kMaxIdSetSize);
	std::uniform_int_distribution<int> kindDist(0, 2);
	std::uniform_int_distribution<int> idDist(0, kMaxIdValue);

	const int idSetCount = idSetCountDist(rng);
	partitions->clear();
	partitions->reserve(static_cast<size_t>(idSetCount));

	for (int i = 0; i < idSetCount; ++i) {
		const auto kind = static_cast<RandomIdSetKind>(kindDist(rng));
		int size = sizeDist(rng);
		if (kind == RandomIdSetKind::Btree) {
			size = std::max(size, kMinBtreeSize);
		}

		if (kind == RandomIdSetKind::Range) {
			const int begin = idDist(rng);
			const int endExclusive = begin + size;
			harness.AddRange(begin, endExclusive);
			partitions->push_back(MakeDenseRange(begin, endExclusive));
			continue;
		}

		std::vector<int> ids;
		ids.reserve(static_cast<size_t>(size));
		for (int j = 0; j < size; ++j) {
			ids.push_back(idDist(rng));
		}

		if (kind == RandomIdSetKind::Btree) {
			// Btree storage requires at least 257 unique ids; duplicates do not count toward that threshold.
			while (UniqueSortedIds(ids).size() < kMinBtreeSize) {
				ids.push_back(idDist(rng));
			}
		}

		if (kind == RandomIdSetKind::Plain) {
			harness.AddPlainIds(ids);
		} else {
			harness.AddBtreeIds(ids);
		}
		partitions->push_back(UniqueSortedIds(std::move(ids)));
	}
}

}  // namespace

TEST(SelectIteratorTest, PlainPartitionsMerged_ForwardAndReverse) {
	// One pre-merged plain idset (SingleIdset / RevSingleIdset) vs two partitions (Forward / Reverse).
	const std::vector<std::vector<int>> partitions{{1, 5, 9, 13, 17}, {2, 6, 10, 14, 18}};
	const auto expectedFwd = MergeForwardExpected(partitions);
	const int maxIterations = 1000;

	SelectIteratorHarness merged;
	merged.AddPlainIds(expectedFwd);
	auto mergedIt = std::move(merged).BuildIterator();

	SelectIteratorHarness split;
	for (const auto& part : partitions) {
		split.AddPlainIds(part);
	}
	auto splitIt = std::move(split).BuildIterator();

	for (bool reverse : {true, false}) {
		mergedIt.Start(reverse, maxIterations);
		ASSERT_EQ(reverse ? SelectIterator::Type::RevSingleIdset : SelectIterator::Type::SingleIdset, mergedIt.GetType());
		splitIt.Start(reverse, maxIterations);
		ASSERT_EQ(reverse ? SelectIterator::Type::Reverse : SelectIterator::Type::Forward, splitIt.GetType());
		EXPECT_EQ(CollectViaNext(mergedIt, reverse, maxIterations), CollectViaNext(splitIt, reverse, maxIterations));
	}
}

TEST(SelectIteratorTest, Forward_PlainVsBtreePartitions) {
	// Forward merge with plain+btree IdSets (no defered sort) matches an all-plain configuration.
	const std::vector<int> part1 = {3, 11, 19, 27, 35, 43, 51, 59};
	const std::vector<int> part2 = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 1);
	const std::vector<std::vector<int>> partitions{part1, part2};
	const auto expected = MergeForwardExpected(partitions);

	SelectIteratorHarness plainHarness;
	plainHarness.AddPlainIds(part1);
	plainHarness.AddPlainIds(part2);
	auto plainMulti = std::move(plainHarness).BuildIterator();

	SelectIteratorHarness btreeHarness;
	btreeHarness.AddPlainIds(part1);
	btreeHarness.AddBtreeIds(part2);
	auto btreeMulti = std::move(btreeHarness).BuildIterator();

	const bool reverse = false;
	AssertEquivalentTraversal(plainMulti, btreeMulti, SelectIterator::Type::Forward, reverse);
	EXPECT_EQ(expected, CollectViaNext(btreeMulti, reverse));
}

TEST(SelectIteratorTest, Reverse_ThreePartitions) {
	// Reverse: 2 plain + 1 btree IdSet vs 3 plain partitions must produce the same sequence.
	const std::vector<int> btreePart = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 4, 100);
	const std::vector<std::vector<int>> partitions{{5, 15, 25}, {10, 20, 30}, btreePart};
	const auto expected = MergeReverseExpected(partitions);

	SelectIteratorHarness allPlain;
	for (const auto& part : partitions) {
		allPlain.AddPlainIds(part);
	}
	auto plainIt = std::move(allPlain).BuildIterator();

	SelectIteratorHarness mixed;
	mixed.AddPlainIds(partitions[0]);
	mixed.AddPlainIds(partitions[1]);
	mixed.AddBtreeIds(partitions[2]);
	auto mixedIt = std::move(mixed).BuildIterator();

	const bool reverse = true;
	AssertEquivalentTraversal(plainIt, mixedIt, SelectIterator::Type::Reverse, reverse);
	EXPECT_EQ(expected, CollectViaNext(mixedIt, reverse));
}

TEST(SelectIteratorTest, SingleIdset_PlainVsBtree) {
	// SingleIdset: same ids stored as plain or btree yield identical forward/reverse traversal.
	const std::vector<int> ids = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 10, 7);

	SelectIteratorHarness plain;
	plain.AddPlainIds(ids);
	auto plainIt = std::move(plain).BuildIterator();

	SelectIteratorHarness btree;
	btree.AddBtreeIds(ids);
	auto btreeIt = std::move(btree).BuildIterator();

	for (bool reverse : {true, false}) {
		AssertEquivalentTraversal(plainIt, btreeIt, reverse ? SelectIterator::Type::RevSingleIdset : SelectIterator::Type::SingleIdset,
								  reverse);
	}
}

TEST(SelectIteratorTest, SingleRange_Forward) {
	// SingleRange forward: iterate every id in [begin, end).
	constexpr int begin = 100;
	constexpr int endExclusive = 110;
	const auto expectedFwd = MakeDenseRange(begin, endExclusive);
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	harness.AddRange(begin, endExclusive);
	auto it = std::move(harness).BuildIterator();

	const bool reverse = false;
	it.Start(reverse, maxIterations);
	EXPECT_EQ(SelectIterator::Type::SingleRange, it.GetType());
	EXPECT_EQ(expectedFwd, CollectViaNext(it, reverse));
}

TEST(SelectIteratorTest, Forward_RangeAndIdsetMixed) {
	// Forward merge of a range partition and an idset partition deduplicates overlapping ids.
	const std::vector<int> rangePart = MakeDenseRange(100, 110);
	const std::vector<int> idsetPart{105, 115, 120};
	const std::vector<std::vector<int>> partitions{rangePart, idsetPart};
	const auto expected = MergeForwardExpected(partitions);
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	harness.AddRange(100, 110);
	harness.AddPlainIds(idsetPart);
	auto it = std::move(harness).BuildIterator();

	const bool reverse = false;
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::Forward, it.GetType());
	EXPECT_EQ(expected, CollectViaNext(it, reverse));
}

TEST(SelectIteratorTest, SingleRange_Reverse) {
	// RevSingleRange: walk the same range in reverse using chained Next hints.
	constexpr int begin = 100;
	constexpr int endExclusive = 110;
	auto expectedRev = MakeDenseRange(begin, endExclusive);
	std::reverse(expectedRev.begin(), expectedRev.end());
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	harness.AddRange(begin, endExclusive);
	auto it = std::move(harness).BuildIterator();

	const bool reverse = true;
	EXPECT_EQ(expectedRev.size(), it.GetMaxIterations());
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::RevSingleRange, it.GetType());

	std::vector<int> got;
	IdType hint = IdType::Max();
	while (!it.End()) {
		if (!it.Next(hint)) {
			break;
		}
		got.push_back(it.Val().ToNumber());
		hint = it.Val();
	}
	EXPECT_EQ(expectedRev, got);
}

TEST(SelectIteratorTest, Compare_ForwardAndReverse) {
	// Compare<fwd/rev> on a multi-partition Forward iterator matches the Next sequence.
	const std::vector<std::vector<int>> partitions{{2, 8, 14}, {4, 10, 16}};
	for (bool reverse : {true, false}) {
		AssertCompareMatchesNextSequence(partitions, reverse);
	}
}

TEST(SelectIteratorTest, Mixed_GallopLargeGap_ForwardAndReverse) {
	// Mixed plain+btree with a wide id gap; external hints must gallop-skip in Forward and Reverse.
	constexpr int kLargeBase = 1'000'000;
	const std::vector<int> smallPart{1, 2, 3};
	const std::vector<int> largePart = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 12, kLargeBase);
	const std::vector<std::vector<int>> partitions{smallPart, largePart};
	const int maxIterations = 1000;
	ASSERT_GT(largePart.front(), smallPart.back());

	constexpr int kForwardJumpHint = 500'000;
	constexpr int kReverseJumpHint = 1'500'000;
	const auto fwdJumpExpected = FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(LastValNum(IdType::Min()), kForwardJumpHint));
	const auto revJumpExpected = FirstMergedIdLessThan(partitions, ReverseBoundFromHint(LastValNum(IdType::Max()), kReverseJumpHint));
	ASSERT_TRUE(fwdJumpExpected.has_value());
	ASSERT_TRUE(revJumpExpected.has_value());
	EXPECT_EQ(kLargeBase, *fwdJumpExpected);		// NOLINT(bugprone-unchecked-optional-access)
	EXPECT_EQ(largePart.back(), *revJumpExpected);	// NOLINT(bugprone-unchecked-optional-access)

	SelectIteratorHarness allPlain;
	allPlain.AddPlainIds(smallPart);
	allPlain.AddPlainIds(largePart);
	auto plainIt = std::move(allPlain).BuildIterator();

	SelectIteratorHarness mixed;
	mixed.AddPlainIds(smallPart);
	mixed.AddBtreeIds(largePart);
	auto mixedIt = std::move(mixed).BuildIterator();

	for (bool reverse : {true, false}) {
		AssertEquivalentTraversal(plainIt, mixedIt, reverse ? SelectIterator::Type::Reverse : SelectIterator::Type::Forward, reverse);
	}

	auto assertForwardGallop = [&](SelectIterator& it) {
		const bool reverse = false;
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Forward, it.GetType());
		EXPECT_EQ(MergeForwardExpected(partitions), CollectViaNext(it, reverse));

		it.Start(reverse, maxIterations);
		ExpectNextFwd(it, RID(kForwardJumpHint), fwdJumpExpected);
		ExpectNextFwd(it, RID(kLargeBase + 1),
					  FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), kLargeBase + 1)));
	};

	auto assertReverseGallop = [&](SelectIterator& it) {
		const bool reverse = true;
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Reverse, it.GetType());
		EXPECT_EQ(MergeReverseExpected(partitions), CollectViaNext(it, reverse));

		it.Start(reverse, maxIterations);
		ExpectNextRev(it, RID(kReverseJumpHint), revJumpExpected);
		ExpectNextRev(it, RID(kForwardJumpHint),
					  FirstMergedIdLessThan(partitions, ReverseBoundFromHint(it.Val().ToNumber(), kForwardJumpHint)));
	};

	assertForwardGallop(plainIt);
	assertForwardGallop(mixedIt);
	assertReverseGallop(plainIt);
	assertReverseGallop(mixedIt);
}

TEST(SelectIteratorTest, Duplicates_AcrossPartitions) {
	// Overlapping ids in different partitions must appear only once in the merged sequence.
	const std::vector<std::vector<int>> partitions{{1, 5, 10}, {5, 15}};
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	for (const auto& part : partitions) {
		harness.AddPlainIds(part);
	}
	auto it = std::move(harness).BuildIterator();

	for (bool reverse : {true, false}) {
		it.Start(reverse, maxIterations);
		ASSERT_EQ(reverse ? SelectIterator::Type::Reverse : SelectIterator::Type::Forward, it.GetType());
		EXPECT_EQ(reverse ? MergeReverseExpected(partitions) : MergeForwardExpected(partitions), CollectViaNext(it, reverse));
	}
}

TEST(SelectIteratorTest, Heap_MixedPartitions_CustomHintsAndRestart) {
	std::vector<std::vector<int>> partitions;
	const std::vector<int> rangePart = MakeDenseRange(100, 110);
	const std::vector<int> btreePart = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 1000);
	partitions.reserve(SelectKeyResult::kMinSetsForHeapSort + 1);
	partitions.push_back(rangePart);
	for (int i = 0; i < static_cast<int>(SelectKeyResult::kMinSetsForHeapSort - 2); ++i) {
		partitions.push_back({i * 5 + 1, 105, 600 + i});
	}
	partitions.push_back(btreePart);
	const int maxIterations = 1000;

	auto fillHarness = [&partitions, &btreePart](SelectIteratorHarness& harness) {
		harness.AddRange(100, 110);
		for (size_t i = 1; i + 1 < partitions.size(); ++i) {
			harness.AddPlainIds(partitions[i]);
		}
		harness.AddBtreeIds(btreePart);
	};

	SelectIteratorHarness fwdHarness;
	fillHarness(fwdHarness);
	auto fwdIt = std::move(fwdHarness).BuildIterator();
	EXPECT_EQ(MergeForwardExpected(partitions), CollectViaNext(fwdIt, false, maxIterations));
	fwdIt.Start(false, maxIterations);
	ExpectNextFwd(fwdIt, RID(104), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(LastValNum(IdType::Min()), 104)));
	ExpectNextFwd(fwdIt, RID(700), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(fwdIt.Val().ToNumber(), 700)));
	ExpectNextFwd(fwdIt, RID(10'000), std::nullopt);

	SelectIteratorHarness revHarness;
	fillHarness(revHarness);
	auto revIt = std::move(revHarness).BuildIterator();
	EXPECT_EQ(MergeReverseExpected(partitions), CollectViaNext(revIt, true, maxIterations));
	revIt.Start(true, maxIterations);
	ExpectNextRev(revIt, RID(108), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(LastValNum(IdType::Max()), 108)));
	ExpectNextRev(revIt, RID(50), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(revIt.Val().ToNumber(), 50)));
	ExpectNextRev(revIt, RID(0), std::nullopt);

	revIt.Start(true, maxIterations);
	EXPECT_EQ(MergeReverseExpected(partitions), CollectViaNext(revIt, true, maxIterations));
}

TEST(SelectIteratorTest, Heap_ExcludeLastSet) {
	std::vector<std::vector<int>> partitions;
	const size_t partitionsCnt = SelectKeyResult::kMinSetsForHeapSort + 4;
	partitions.reserve(partitionsCnt);
	for (int i = 0; i < static_cast<int>(partitionsCnt); ++i) {
		partitions.push_back({i, 1000 + i});
	}
	const int maxIterations = 1000;

	auto collectAfterExclude = [&](bool reverse) -> std::pair<std::vector<int>, std::vector<int>> {
		SelectIteratorHarness harness;
		for (const auto& part : partitions) {
			harness.AddPlainIds(part);
		}
		auto it = std::move(harness).BuildIterator();
		it.Start(reverse, maxIterations);
		EXPECT_EQ(reverse ? SelectIterator::Type::Reverse : SelectIterator::Type::Forward, it.GetType());

		const auto expectedBeforeExclude = reverse ? MergeReverseExpected(partitions) : MergeForwardExpected(partitions);
		if (!it.Next(reverse ? IdType::Max() : IdType::Min())) {
			ADD_FAILURE() << "Expected first value before ExcludeLastSet";
			return {};
		}
		EXPECT_EQ(expectedBeforeExclude.front(), it.Val().ToNumber());

		const int excludedPartition = reverse ? static_cast<int>(partitions.size() - 1) : 0;
		it.ExcludeLastSet(it.Val());

		std::vector<int> got;
		IdType hint = it.Val();
		while (!it.End()) {
			if (!it.Next(hint)) {
				break;
			}
			got.push_back(it.Val().ToNumber());
			hint = it.Val();
		}

		auto expectedPartitions = partitions;
		expectedPartitions.erase(expectedPartitions.begin() + excludedPartition);
		return std::pair{got, reverse ? MergeReverseExpected(expectedPartitions) : MergeForwardExpected(expectedPartitions)};
	};

	for (bool reverse : {true, false}) {
		auto [got, expected] = collectAfterExclude(reverse);
		EXPECT_EQ(expected, got);
	}
}

TEST(SelectIteratorTest, Forward_SingleIdset_CustomMinHint) {
	// Next(minHint) on SingleIdset: hint skips ahead; hint <= lastVal_ continues from the current position.
	const std::vector<std::vector<int>> partitions{{1, 5, 10, 20}};
	const int maxIterations = 1000;
	const bool reverse = false;

	SelectIteratorHarness harness;
	harness.AddPlainIds(partitions[0]);
	auto it = std::move(harness).BuildIterator();
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::SingleIdset, it.GetType());

	const int last0 = LastValNum(IdType::Min());
	ExpectNextFwd(it, RID(12), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(last0, 12)));
	ExpectNextFwd(it, RID(25), std::nullopt);

	it.Start(reverse, maxIterations);
	ExpectNextFwd(it, RID(3), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(LastValNum(IdType::Min()), 3)));
	ExpectNextFwd(it, RID(12), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), 12)));

	it.Start(reverse, maxIterations);
	ExpectNextFwd(it, RID(5), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(LastValNum(IdType::Min()), 5)));
	ExpectNextFwd(it, RID(3), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), 3)));
}

TEST(SelectIteratorTest, Forward_MultiPartition_CustomMinHint) {
	// Forward merge: external minHint must pick the global minimum id strictly after hint.Decr().
	const std::vector<std::vector<int>> partitions{{1, 9, 17}, {4, 12, 20}};
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	for (const auto& part : partitions) {
		harness.AddPlainIds(part);
	}
	auto it = std::move(harness).BuildIterator();

	const bool reverse = false;
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::Forward, it.GetType());

	const int last0 = LastValNum(IdType::Min());
	ExpectNextFwd(it, RID(11), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(last0, 11)));
	ExpectNextFwd(it, RID(15), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), 15)));
}

TEST(SelectIteratorTest, Forward_PlainAndBtree_CustomMinHint) {
	// Same custom minHint on plain-only and plain+btree Forward iterators yields the same Val().
	const std::vector<int> part1{2, 10, 18};
	const std::vector<int> part2 = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 1);
	const std::vector<std::vector<int>> partitions{part1, part2};
	constexpr int minHint = 15;
	const int maxIterations = 1000;

	const auto expected = FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(LastValNum(IdType::Min()), minHint));
	ASSERT_TRUE(expected.has_value());

	auto runWithHint = [&](SelectIterator& iter) {
		const bool reverse = false;
		iter.Start(reverse, maxIterations);
		ExpectNextFwd(iter, RID(minHint), expected);
	};

	SelectIteratorHarness plainHarness;
	plainHarness.AddPlainIds(part1);
	plainHarness.AddPlainIds(part2);
	auto plainIt = std::move(plainHarness).BuildIterator();

	SelectIteratorHarness btreeHarness;
	btreeHarness.AddPlainIds(part1);
	btreeHarness.AddBtreeIds(part2);
	auto btreeIt = std::move(btreeHarness).BuildIterator();

	runWithHint(plainIt);
	runWithHint(btreeIt);
}

TEST(SelectIteratorTest, Reverse_PlainAndBtree_CustomMaxHint) {
	// Same custom maxHint on plain-only and plain+btree Reverse iterators yields the same Val().
	const std::vector<int> part1{2, 10, 18};
	const std::vector<int> part2 = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 1);
	const std::vector<std::vector<int>> partitions{part1, part2};
	constexpr int maxHint = 16;
	const int maxIterations = 1000;

	const auto expected = FirstMergedIdLessThan(partitions, ReverseBoundFromHint(LastValNum(IdType::Max()), maxHint));
	ASSERT_TRUE(expected.has_value());

	auto runWithHint = [&](SelectIterator& iter) {
		const bool reverse = true;
		iter.Start(reverse, maxIterations);
		ExpectNextRev(iter, RID(maxHint), expected);
	};

	SelectIteratorHarness plainHarness;
	plainHarness.AddPlainIds(part1);
	plainHarness.AddPlainIds(part2);
	auto plainIt = std::move(plainHarness).BuildIterator();

	SelectIteratorHarness btreeHarness;
	btreeHarness.AddPlainIds(part1);
	btreeHarness.AddBtreeIds(part2);
	auto btreeIt = std::move(btreeHarness).BuildIterator();

	runWithHint(plainIt);
	runWithHint(btreeIt);
}

TEST(SelectIteratorTest, Forward_MixedPartitions_CustomMinHint) {
	// range + plain + btree: forward merge/collect and custom minHint jumps.
	// Correct model: k-way MergeForwardExpected + FirstMergedIdGreaterThan (see helpers above).
	const std::vector<int> rangePart = MakeDenseRange(100, 110);
	const std::vector<int> plainPart{2, 50};
	const std::vector<int> btreePart = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 200);
	const std::vector<std::vector<int>> partitions{rangePart, plainPart, btreePart};
	const int maxIterations = 1000;
	const bool reverse = false;

	const auto expectedFwd = MergeForwardExpected(partitions);
	ASSERT_EQ(2, expectedFwd.front());
	ASSERT_EQ(btreePart.front(), expectedFwd[rangePart.size() + plainPart.size()]);

	auto expectForwardHintChain = [&](SelectIterator& it) {
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Forward, it.GetType());
		const int last0 = LastValNum(IdType::Min());
		ExpectNextFwd(it, RID(104), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(last0, 104)));
		ExpectNextFwd(it, RID(108), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), 108)));
		ExpectNextFwd(it, RID(150), FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), 150)));
		ExpectNextFwd(it, RID(it.Val().ToNumber()),
					  FirstMergedIdGreaterThan(partitions, ForwardBoundFromHint(it.Val().ToNumber(), it.Val().ToNumber())));
		ExpectNextFwd(it, RID(1'000'000), std::nullopt);
	};

	auto assertCorrectForwardCollect = [&](SelectIterator& it) {
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Forward, it.GetType());
		EXPECT_EQ(expectedFwd, CollectViaNext(it, reverse));
	};

	SelectIteratorHarness allPlain;
	allPlain.AddRange(100, 110);
	allPlain.AddPlainIds(plainPart);
	allPlain.AddPlainIds(btreePart);
	auto plainIt = std::move(allPlain).BuildIterator();

	SelectIteratorHarness mixed;
	mixed.AddRange(100, 110);
	mixed.AddPlainIds(plainPart);
	mixed.AddBtreeIds(btreePart);
	auto mixedIt = std::move(mixed).BuildIterator();

	AssertEquivalentTraversal(plainIt, mixedIt, SelectIterator::Type::Forward, reverse);
	assertCorrectForwardCollect(plainIt);
	assertCorrectForwardCollect(mixedIt);
	expectForwardHintChain(plainIt);
	expectForwardHintChain(mixedIt);
}

TEST(SelectIteratorTest, Reverse_MixedPartitions_CustomMaxHint) {
	// range + plain + btree: reverse merge/collect and custom maxHint jumps.
	// Correct model: k-way MergeReverseExpected + FirstMergedIdLessThan (see helpers above).
	const std::vector<int> rangePart = MakeDenseRange(100, 110);
	const std::vector<int> plainPart{2, 50};
	const std::vector<int> btreePart = MakeBtreeSizedIds(reindexer::kMaxPlainIdsetSize + 8, 200);
	const std::vector<std::vector<int>> partitions{rangePart, plainPart, btreePart};
	const int maxIterations = 1000;
	const bool reverse = true;

	const auto expectedRev = MergeReverseExpected(partitions);
	const auto expectedFwd = MergeForwardExpected(partitions);

	auto expectReverseHintChain = [&](SelectIterator& it) {
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Reverse, it.GetType());
		const int last0 = LastValNum(IdType::Max());
		ExpectNextRev(it, RID(108), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(last0, 108)));
		ExpectNextRev(it, RID(105), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(it.Val().ToNumber(), 105)));
		ExpectNextRev(it, RID(60), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(it.Val().ToNumber(), 60)));
		ExpectNextRev(it, RID(55), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(it.Val().ToNumber(), 55)));
		ExpectNextRev(it, RID(1), std::nullopt);
	};

	auto assertCorrectReverseCollect = [&](SelectIterator& it) {
		it.Start(reverse, maxIterations);
		ASSERT_EQ(SelectIterator::Type::Reverse, it.GetType());
		EXPECT_EQ(expectedRev, CollectViaNext(it, reverse));

		auto revFromFwd = expectedFwd;
		std::reverse(revFromFwd.begin(), revFromFwd.end());
		it.Start(reverse, maxIterations);
		EXPECT_EQ(revFromFwd, CollectViaNext(it, reverse));
	};

	SelectIteratorHarness allPlain;
	allPlain.AddRange(100, 110);
	allPlain.AddPlainIds(plainPart);
	allPlain.AddPlainIds(btreePart);
	auto plainIt = std::move(allPlain).BuildIterator();

	SelectIteratorHarness mixed;
	mixed.AddRange(100, 110);
	mixed.AddPlainIds(plainPart);
	mixed.AddBtreeIds(btreePart);
	auto mixedIt = std::move(mixed).BuildIterator();

	// Plain and mixed agree with each other, but both can diverge from MergeReverseExpected until the bugs below are fixed.
	AssertEquivalentTraversal(plainIt, mixedIt, SelectIterator::Type::Reverse, reverse);

	// Custom hints on a fresh Start match the k-way model (see expectReverseHintChain).
	SelectIteratorHarness hintPlain;
	hintPlain.AddRange(100, 110);
	hintPlain.AddPlainIds(plainPart);
	hintPlain.AddPlainIds(btreePart);
	auto hintPlainIt = std::move(hintPlain).BuildIterator();
	expectReverseHintChain(hintPlainIt);

	SelectIteratorHarness hintMixed;
	hintMixed.AddRange(100, 110);
	hintMixed.AddPlainIds(plainPart);
	hintMixed.AddBtreeIds(btreePart);
	auto hintMixedIt = std::move(hintMixed).BuildIterator();
	expectReverseHintChain(hintMixedIt);

	assertCorrectReverseCollect(plainIt);
	assertCorrectReverseCollect(mixedIt);

	SelectIteratorHarness afterCollect;
	afterCollect.AddRange(100, 110);
	afterCollect.AddPlainIds(plainPart);
	afterCollect.AddPlainIds(btreePart);
	auto afterCollectIt = std::move(afterCollect).BuildIterator();
	std::ignore = CollectViaNext(afterCollectIt, reverse);
	afterCollectIt.Start(reverse, maxIterations);
	// BUG (selectiterator.h): after full reverse collect, Start() does not reset range rrIt_/plain/btree cursors.
	// Correct: Next(maxHint=108) must return 108; today it returns 50 (plain tail), skipping the range block.
	ExpectNextRev(afterCollectIt, RID(108), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(LastValNum(IdType::Max()), 108)));
}

TEST(SelectIteratorTest, Reverse_SingleIdset_CustomMaxHint) {
	// Next(maxHint) on RevSingleIdset: hint skips backward; hint >= lastVal_ continues from current position.
	const std::vector<std::vector<int>> partitions{{1, 5, 10, 20}};
	const int maxIterations = 1000;
	const bool reverse = true;

	SelectIteratorHarness harness;
	harness.AddPlainIds(partitions[0]);
	auto it = std::move(harness).BuildIterator();
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::RevSingleIdset, it.GetType());

	const int last0 = LastValNum(IdType::Max());
	ExpectNextRev(it, RID(12), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(last0, 12)));
	ExpectNextRev(it, RID(0), std::nullopt);

	it.Start(reverse, maxIterations);
	ExpectNextRev(it, RID(12), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(LastValNum(IdType::Max()), 12)));
	// maxHint >= lastVal_: search continues from 10, not from 12 — next id is 5.
	ExpectNextRev(it, RID(12), 5);
}

TEST(SelectIteratorTest, Reverse_MultiPartition_CustomMaxHint) {
	// Reverse merge: external maxHint must pick the global maximum id strictly before hint.Incr().
	const std::vector<std::vector<int>> partitions{{3, 11, 19}, {6, 14, 22}};
	const int maxIterations = 1000;
	const bool reverse = true;

	SelectIteratorHarness harness;
	for (const auto& part : partitions) {
		harness.AddPlainIds(part);
	}
	auto it = std::move(harness).BuildIterator();
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::Reverse, it.GetType());

	const int last0 = LastValNum(IdType::Max());
	ExpectNextRev(it, RID(16), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(last0, 16)));
	ExpectNextRev(it, RID(10), FirstMergedIdLessThan(partitions, ReverseBoundFromHint(it.Val().ToNumber(), 10)));
}

TEST(SelectIteratorTest, Forward_SingleRange_CustomMinHint) {
	// SingleRange: minHint jumps into the range; the next value is max(rBegin, hint) when hint > lastVal_.
	constexpr int begin = 100;
	constexpr int endExclusive = 110;
	const int maxIterations = 1000;

	SelectIteratorHarness harness;
	harness.AddRange(begin, endExclusive);
	auto it = std::move(harness).BuildIterator();

	const bool reverse = false;
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::SingleRange, it.GetType());

	ExpectNextFwd(it, RID(105), 105);
	ExpectNextFwd(it, RID(108), 108);
	ExpectNextFwd(it, RID(108), 109);
	ExpectNextFwd(it, RID(200), std::nullopt);
}

TEST(SelectIteratorTest, SingleIdSetWithDeferedSort_MatchesMergedPlain) {
	// After defered merge, SingleIdSetWithDeferedSort matches one pre-merged plain SingleIdset.
	const int maxIterations = 100'000;
	std::vector<std::vector<int>> partitions;
	partitions.reserve(SelectKeyResult::kMinSetsForGenericSort + 5);
	for (int i = 0; i < static_cast<int>(SelectKeyResult::kMinSetsForGenericSort + 5); ++i) {
		partitions.push_back({i * 2, i * 2 + 200});
	}
	const auto expected = MergeForwardExpected(partitions);

	SelectIteratorHarness merged;
	merged.AddPlainIds(expected);
	auto mergedIt = std::move(merged).BuildIterator();

	const bool reverse = false;
	mergedIt.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::SingleIdset, mergedIt.GetType());
	EXPECT_EQ(expected, CollectViaNext(mergedIt, reverse));

	SelectIteratorHarness defered;
	defered.SetDeferedExplicitSort();
	for (const auto& part : partitions) {
		defered.AddPlainIds(part);
	}
	auto deferedIt = std::move(defered).BuildIterator();
	deferedIt.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::SingleIdSetWithDeferedSort, deferedIt.GetType());
	EXPECT_EQ(expected, CollectViaNext(deferedIt, reverse));
}

TEST(SelectIteratorTest, RevSingleIdSetWithDeferedSort_MatchesMergedPlain) {
	// After defered merge, RevSingleIdSetWithDeferedSort matches one pre-merged plain RevSingleIdset.
	const int maxIterations = 100'000;
	std::vector<std::vector<int>> partitions;
	partitions.reserve(SelectKeyResult::kMinSetsForGenericSort + 5);
	for (int i = 0; i < static_cast<int>(SelectKeyResult::kMinSetsForGenericSort + 5); ++i) {
		partitions.push_back({i * 2, i * 2 + 200});
	}
	const auto expectedFwd = MergeForwardExpected(partitions);
	const auto expectedRev = MergeReverseExpected(partitions);

	SelectIteratorHarness merged;
	merged.AddPlainIds(expectedFwd);
	auto mergedIt = std::move(merged).BuildIterator();

	const bool reverse = true;
	mergedIt.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::RevSingleIdset, mergedIt.GetType());
	EXPECT_EQ(expectedRev, CollectViaNext(mergedIt, reverse));

	SelectIteratorHarness defered;
	defered.SetDeferedExplicitSort();
	for (const auto& part : partitions) {
		defered.AddPlainIds(part);
	}
	auto deferedIt = std::move(defered).BuildIterator();
	deferedIt.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::RevSingleIdSetWithDeferedSort, deferedIt.GetType());
	EXPECT_EQ(expectedRev, CollectViaNext(deferedIt, reverse));
}

TEST(SelectIteratorTest, Unsorted_PreservesInsertionOrder) {
	// Unsorted: traversal order follows Unordered insertion order and is repeatable across restarts.
	const std::vector<int> expected{7, 3, 20, 15};
	const int maxIterations = 1000;
	const bool reverse = false;

	auto collectUnsorted = [](SelectIterator& iter) {
		std::vector<int> out;
		IdType hint = IdType::Min();
		while (!iter.End()) {
			if (!iter.Next(hint)) {
				break;
			}
			out.push_back(iter.Val().ToNumber());
			hint = iter.Val();
		}
		return out;
	};

	SelectIteratorHarness harness;
	harness.AddPlainIdsUnordered({7, 3, 20, 15});
	auto it = std::move(harness).BuildIterator();
	it.SetUnsorted();
	it.Start(reverse, maxIterations);
	ASSERT_EQ(SelectIterator::Type::Unsorted, it.GetType());

	const auto seq1 = collectUnsorted(it);
	it.Start(reverse, maxIterations);
	const auto seq2 = collectUnsorted(it);

	EXPECT_EQ(seq1, seq2);
	EXPECT_EQ(expected, seq1);
}

TEST(SelectIteratorTest, Randomized_MixedIdSets) {
	constexpr int totalRuns = 5;
	const int maxIterations = 1'000'000;

	for (int run = 0; run < totalRuns; ++run) {
		const auto seed = static_cast<uint64_t>(
			std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
		TestCout() << "SelectIterator randomized test seed: " << seed << " run=" << run << std::endl;

		std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));

		SelectIteratorHarness harness;
		std::vector<std::vector<int>> partitions;
		GenerateRandomIdSets(rng, harness, &partitions);

		auto it = std::move(harness).BuildIterator();

		for (bool reverse : {false, true}) {
			EXPECT_EQ(reverse ? MergeReverseExpected(partitions) : MergeForwardExpected(partitions),
					  CollectViaNext(it, reverse, maxIterations));
		}
	}
}

}  // namespace reindexer_tests
