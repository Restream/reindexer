#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <latch>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/idrelset.h"

namespace reindexer_tests {
namespace {

using reindexer::IdRelType;
using reindexer::PackedIdRelVec;
using reindexer::VDocId;
using reindexer::VDocVersion;
using PackedHolder = reindexer::DataHolder<PackedIdRelVec>;

enum class [[nodiscard]] BuildMode { SingleThread, Parallel };

class [[nodiscard]] NeverCanceled final : public reindexer::index::ICancelable {
public:
	bool IsCanceled() const noexcept override { return false; }
};

void CheckPostings(const PackedIdRelVec& packed, const std::vector<IdRelType>& expected) {
	ASSERT_EQ(packed.size(), expected.size());
	auto it = packed.begin();
	for (const auto& posting : expected) {
		ASSERT_TRUE(it != packed.end());
		EXPECT_EQ(it->VdocId(), posting.VdocId());
		EXPECT_EQ(it->VdocVersion(), posting.VdocVersion());
		const auto& positions = it->Pos();
		ASSERT_EQ(positions.size(), posting.Pos().size());
		for (size_t i = 0; i < positions.size(); ++i) {
			EXPECT_EQ(positions[i], posting.Pos()[i]);
		}
		++it;
	}
	EXPECT_TRUE(it == packed.end());
}

void CheckPackedBytes(const PackedIdRelVec& lhs, const PackedIdRelVec& rhs) {
	ASSERT_EQ(lhs.size(), rhs.size());
	ASSERT_EQ(lhs.data_size(), rhs.data_size());
	EXPECT_EQ(lhs.array_found_pos(), rhs.array_found_pos());
	if (lhs.data_size()) {
		EXPECT_TRUE(std::equal(lhs.data(), lhs.data() + lhs.data_size(), rhs.data()));
	}
}

std::vector<IdRelType> SimplePostings(VDocId first, size_t count) {
	std::vector<IdRelType> postings;
	for (size_t i = 0; i < count; ++i) {
		std::ignore = postings.emplace_back(first + VDocId(i), 1);
		postings.back().Add(5, 0, 0);
	}
	return postings;
}

template <typename IdCont>
void AddDocuments(reindexer::DataHolder<IdCont>& holder, size_t count, VDocVersion version, BuildMode mode) {
	reindexer::VDocsTexts texts;
	std::vector<reindexer::VDocPosting> vdocs;
	for (size_t i = 0; i < count; ++i) {
		auto& fields = texts.emplace_back();
		std::ignore = fields.emplace_back("common common", 0);
		std::ignore = fields.emplace_back(version == 1 ? "common oldonly" : "common newonly", 0);
		vdocs.push_back({.vdocId = VDocId(i + 1), .version = version});
	}
	std::vector<reindexer::VDocWordCounts> wordCounts;
	holder.Process(texts, vdocs, count, 1, wordCounts, mode == BuildMode::Parallel);
}

template <typename IdCont>
void CheckCachedMemory(reindexer::DataHolder<IdCont>& holder) {
	const auto stats = holder.GetMemStat();
	const auto& mem = stats.memory;
	EXPECT_EQ(stats.text.wordsCount, holder.wordsMap_.size());
	size_t capacity = 0, packed = 0;
	for (const auto& [word, ordinal] : holder.wordsMap_) {
		std::ignore = word;
		const auto list = holder.GetWordOccurencesByOrdinal(ordinal);
		capacity += list->heap_size();
		if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
			packed += list->data_size();
		}
	}
	EXPECT_EQ(mem.postingsCapacitySize, capacity);
	EXPECT_GE(mem.postingsSize, capacity);
	if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
		if (mem.packedDataSize.has_value()) {
			EXPECT_EQ(*mem.packedDataSize, packed);
		} else {
			FAIL() << "Packed data size is missing in Memory mode";
		}
		EXPECT_LE(packed, capacity);
	} else {
		EXPECT_FALSE(mem.packedDataSize.has_value());
	}
	size_t suffixes = holder.suffixes_.capacity() * sizeof(typename decltype(holder.suffixes_)::value_type);
	for (const auto& tree : holder.suffixes_) {
		if (tree) {
			suffixes += sizeof(*tree) + tree->bytes_used();
		}
	}
	EXPECT_EQ(mem.suffixesSize, suffixes);
	size_t typoGroups = 0, typoKeys = 0;
	size_t typos = holder.typos_.capacity() * sizeof(typename decltype(holder.typos_)::value_type);
	for (const auto& set : holder.typos_) {
		if (set) {
			typoGroups += set->size();
			typos += sizeof(*set) + set->allocated_mem_size();
			for (const auto& group : *set) {
				typoKeys += reindexer::TypoKeysCount(group);
				typos += group.heap_size();
			}
		}
	}
	EXPECT_EQ(stats.text.typosGroupsCount, typoGroups);
	EXPECT_EQ(stats.text.typosKeysCount, typoKeys);
	EXPECT_EQ(mem.typosSize, typos);
}

class [[nodiscard]] CancelAfterChecks final : public reindexer::index::ICancelable {
public:
	explicit CancelAfterChecks(size_t limit) noexcept : limit_(limit) {}
	bool IsCanceled() const noexcept override { return checks_.fetch_add(1, std::memory_order_relaxed) >= limit_; }

private:
	const size_t limit_;
	mutable std::atomic<size_t> checks_{0};
};

enum class [[nodiscard]] CleanupExit { Cancel, Throw };

template <typename IdCont>
void CheckInterruptedCleanup(CleanupExit exit) {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	reindexer::DataHolder<IdCont> holder(&cfg);
	// All three short lists fit in one cleanup task. Interrupt after its first publication.
	AddDocuments(holder, 16, 1, BuildMode::SingleThread);
	AddDocuments(holder, 16, 2, BuildMode::SingleThread);
	const auto first = holder.GetWordOccurencesByOrdinal(0);
	ASSERT_FALSE(first->empty());
	if (exit == CleanupExit::Cancel) {
		class [[nodiscard]] CancelAfterPublication final : public reindexer::index::ICancelable {
		public:
			CancelAfterPublication(const reindexer::DataHolder<IdCont>& holder, const IdCont* first) noexcept
				: holder_(holder), first_(first) {}
			bool IsCanceled() const noexcept override { return holder_.GetWordOccurencesByOrdinal(0).get() != first_; }

		private:
			const reindexer::DataHolder<IdCont>& holder_;
			const IdCont* first_;
		};
		const CancelAfterPublication cancelable(holder, first.get());
		const auto result = holder.OptimizeDeleted([](VDocId, VDocVersion) { return true; }, cancelable);
		EXPECT_EQ(result.canceled, reindexer::WasCanceled_True);
	} else {
		const NeverCanceled cancelable;
		const auto isDeleted = [&holder, &first](VDocId, VDocVersion) {
			if (holder.GetWordOccurencesByOrdinal(0).get() != first.get()) {
				throw std::runtime_error("Interrupt cleanup after publishing the first list");
			}
			return true;
		};
		EXPECT_THROW(std::ignore = holder.OptimizeDeleted(isDeleted, cancelable), std::runtime_error);
	}
	EXPECT_TRUE(holder.GetWordOccurencesByOrdinal(0)->empty());
	EXPECT_FALSE(first->empty());
	ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
	const NeverCanceled cancelable;
	std::ignore = holder.OptimizeDeleted([](VDocId, VDocVersion) { return true; }, cancelable);
	ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
	EXPECT_EQ(holder.GetMemStat().memory.postingsCapacitySize, 0u);
}

template <typename IdCont>
void CheckMemoryCacheLifecycle() {
	for (const auto mode : {BuildMode::SingleThread, BuildMode::Parallel}) {
		reindexer::FTConfig cfg(1);
		cfg.stopWords.clear();
		cfg.maxTypos = 4;
		cfg.maxTypoLen = 16;
		reindexer::DataHolder<IdCont> holder(&cfg);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		AddDocuments(holder, 256, 1, mode);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		AddDocuments(holder, 512, 2, mode);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		const auto before = holder.GetMemStat();
		EXPECT_GT(before.text.typosGroupsCount, 0u);
		EXPECT_GT(before.text.typosKeysCount, before.text.typosGroupsCount);
		AddDocuments(holder, 512, 3, mode);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		const auto after = holder.GetMemStat();
		EXPECT_EQ(before.memory.suffixesSize, after.memory.suffixesSize);
		EXPECT_EQ(before.text.typosGroupsCount, after.text.typosGroupsCount);
		EXPECT_EQ(before.text.typosKeysCount, after.text.typosKeysCount);
		EXPECT_EQ(before.memory.typosSize, after.memory.typosSize);

		const CancelAfterChecks cancelable(2);
		const auto canceled = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version < 3; }, cancelable);
		EXPECT_EQ(canceled.canceled, reindexer::WasCanceled_True);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		const NeverCanceled neverCanceled;
		std::ignore = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version < 3; }, neverCanceled);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		const auto cleaned = holder.GetMemStat().memory;
		std::ignore = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version < 3; }, neverCanceled);
		EXPECT_EQ(holder.GetMemStat().memory.TotalSize(), cleaned.TotalSize());
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));

		holder.Clear();
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		AddDocuments(holder, 128, 1, mode);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		std::ignore = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version == 1; }, neverCanceled);
		ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
		EXPECT_EQ(holder.GetMemStat().memory.postingsCapacitySize, 0u);
	}
}

template <typename IdCont>
void CheckLinkedJobs() {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	reindexer::DataHolder<IdCont> serial(&cfg), parallel(&cfg);
	for (const VDocVersion version : {1u, 2u, 3u}) {
		const size_t count = version == 1 ? 512 : (version == 2 ? 32 : 256);
		std::shared_ptr<const IdCont> untouched;
		if (version == 2) {
			untouched = parallel.GetWordOccurencesByOrdinal(parallel.FindWordOrdinal("tokenex"));
		}
		std::vector<std::string> storage;
		storage.reserve(count);
		reindexer::VDocsTexts texts;
		std::vector<reindexer::VDocPosting> vdocs;
		for (size_t i = 0; i < count; ++i) {
			const size_t word = i % 128 + (version == 3 ? 64 : 0);
			auto& text = storage.emplace_back("common token");
			text.push_back(char('a' + word / 26));
			text.push_back(char('a' + word % 26));
			auto& fields = texts.emplace_back();
			std::ignore = fields.emplace_back(text, 0);
			std::ignore = fields.emplace_back("common", 0);
			vdocs.push_back({.vdocId = VDocId(count - i), .version = version});
		}
		std::vector<reindexer::VDocWordCounts> wordCounts;
		const auto serialMode = BuildMode::SingleThread;
		const auto parallelMode = BuildMode::Parallel;
		serial.Process(texts, vdocs, 512, 1, wordCounts, serialMode == BuildMode::Parallel);
		parallel.Process(texts, vdocs, 512, 1, wordCounts, parallelMode == BuildMode::Parallel);
		ASSERT_EQ(serial.wordsMap_.size(), parallel.wordsMap_.size());
		for (const auto& [word, ordinal] : serial.wordsMap_) {
			SCOPED_TRACE(::testing::Message() << "word=" << word << " version=" << version);
			const auto otherOrdinal = parallel.FindWordOrdinal(word);
			ASSERT_NE(otherOrdinal, reindexer::IDataHolder::kIncorrectWordOrdinal);
			const auto lhs = serial.GetWordOccurencesByOrdinal(ordinal);
			const auto rhs = parallel.GetWordOccurencesByOrdinal(otherOrdinal);
			if constexpr (std::is_same_v<IdCont, PackedIdRelVec>) {
				ASSERT_NO_FATAL_FAILURE(CheckPackedBytes(*lhs, *rhs));
				EXPECT_LE(rhs->heap_size(), rhs->data_size() + (rhs->data_size() + 9) / 10);
			} else {
				ASSERT_EQ(lhs->size(), rhs->size());
				for (size_t i = 0; i < lhs->size(); ++i) {
					EXPECT_EQ((*lhs)[i].VdocId(), (*rhs)[i].VdocId());
					EXPECT_EQ((*lhs)[i].VdocVersion(), (*rhs)[i].VdocVersion());
					const auto& lhsPos = (*lhs)[i].Pos();
					const auto& rhsPos = (*rhs)[i].Pos();
					ASSERT_EQ(lhsPos.size(), rhsPos.size());
					EXPECT_TRUE(std::equal(lhsPos.begin(), lhsPos.end(), rhsPos.begin()));
				}
			}
		}
		if (untouched) {
			EXPECT_EQ(parallel.GetWordOccurencesByOrdinal(parallel.FindWordOrdinal("tokenex")).get(), untouched.get());
			EXPECT_EQ(untouched->size(), 4u);
		}
	}
}

}  // namespace

TEST(FTPackedPostings, PackedMemoryCacheMatchesStructuresAcrossLifecycle) {
	ASSERT_NO_FATAL_FAILURE(CheckMemoryCacheLifecycle<PackedIdRelVec>());
}

TEST(FTPackedPostings, CpuMemoryCacheMatchesStructuresAcrossLifecycle) {
	ASSERT_NO_FATAL_FAILURE(CheckMemoryCacheLifecycle<reindexer::IdRelVec>());
}

TEST(FTPackedPostings, CleanupTaskAppliesMemoryDeltaOnCancellation) {
	ASSERT_NO_FATAL_FAILURE(CheckInterruptedCleanup<PackedIdRelVec>(CleanupExit::Cancel));
	ASSERT_NO_FATAL_FAILURE(CheckInterruptedCleanup<reindexer::IdRelVec>(CleanupExit::Cancel));
}

TEST(FTPackedPostings, CleanupTaskAppliesMemoryDeltaOnException) {
	ASSERT_NO_FATAL_FAILURE(CheckInterruptedCleanup<PackedIdRelVec>(CleanupExit::Throw));
	ASSERT_NO_FATAL_FAILURE(CheckInterruptedCleanup<reindexer::IdRelVec>(CleanupExit::Throw));
}

TEST(FTPackedPostings, MemoryCacheRemainsConsistentDuringCleanup) {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	PackedHolder holder(&cfg);
	AddDocuments(holder, 1024, 1, BuildMode::Parallel);
	AddDocuments(holder, 2048, 2, BuildMode::Parallel);
	const NeverCanceled cancelable;
	{
		std::latch reading(1);
		std::jthread reader([&holder, &reading](const std::stop_token& stop) {
			bool firstRead = true;
			do {
				const auto mem = holder.GetMemStat().memory;
				EXPECT_TRUE(mem.packedDataSize.has_value());
				EXPECT_LE(mem.packedDataSize.value_or(0), mem.postingsCapacitySize);
				EXPECT_LE(mem.postingsCapacitySize, mem.postingsSize);
				if (firstRead) {
					reading.count_down();
					firstRead = false;
				}
			} while (!stop.stop_requested());
		});
		reading.wait();
		std::ignore = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version == 1; }, cancelable);
	}
	ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
}

TEST(FTPackedPostings, LinkedJobsPreservePackedOrderForMixedUpdates) { ASSERT_NO_FATAL_FAILURE(CheckLinkedJobs<PackedIdRelVec>()); }

TEST(FTPackedPostings, LinkedJobsPreserveCpuOrderForMixedUpdates) { ASSERT_NO_FATAL_FAILURE(CheckLinkedJobs<reindexer::IdRelVec>()); }

TEST(FTPackedPostings, AppendBufferPreservesEncodingAcrossTasks) {
	for (const unsigned baseArrayIdx : {0u, 2u}) {
		for (const size_t batchSize : {1u, 60u, 128u, 129u, 257u}) {
			SCOPED_TRACE(::testing::Message() << "baseArrayIdx=" << baseArrayIdx << " batchSize=" << batchSize);
			PackedIdRelVec reference(2), actual(2);
			std::vector<IdRelType> expected;
			std::ignore = expected.emplace_back(100000, 1);
			expected.back().Add(3, 1, baseArrayIdx);
			reference.insert_back(expected.begin(), expected.end());
			actual.insert_back(expected.begin(), expected.end());
			actual.shrink_to_fit();

			{
				PackedIdRelVec::AppendBuffer tail(actual);
				for (unsigned task = 0; task < 3; ++task) {
					std::vector<IdRelType> postings;
					for (size_t i = 0; i < batchSize; ++i) {
						// IDs decrease at task boundaries; versions exercise inline and extended encoding.
						std::ignore = postings.emplace_back(VDocId(90000 - task * 10000 + i), task == 1 ? 1000 : 2);
						postings.back().Add(unsigned(i), task % 3, task == 1 ? 300 : 0);
						if (i % 3 == 0) {
							for (unsigned pos = 0; pos < 10; ++pos) {
								postings.back().Add(70000 + pos, 2, task == 1 ? 300 : 0);
							}
						}
					}
					reference.insert_back(postings.begin(), postings.end());
					tail.Append(postings.begin(), postings.end());
					std::ignore = expected.insert(expected.end(), postings.begin(), postings.end());
				}
				EXPECT_EQ(actual.size(), 1u);
				tail.Commit();
			}
			ASSERT_NO_FATAL_FAILURE(CheckPackedBytes(actual, reference));
			ASSERT_NO_FATAL_FAILURE(CheckPostings(actual, expected));
			EXPECT_LE(actual.heap_size(), actual.data_size() + (actual.data_size() + 9) / 10);

			// A subsequent append must continue with the committed ID and array state.
			auto next = SimplePostings(7, 1);
			PackedIdRelVec::AppendBuffer tail(actual);
			tail.Append(next.begin(), next.end());
			tail.Commit();
			reference.insert_back(next.begin(), next.end());
			std::ignore = expected.insert(expected.end(), next.begin(), next.end());
			ASSERT_NO_FATAL_FAILURE(CheckPackedBytes(actual, reference));
			ASSERT_NO_FATAL_FAILURE(CheckPostings(actual, expected));
		}
	}
}

TEST(FTPackedPostings, EmptyAndDiscardedAppendBuffersDoNotChangeDestination) {
	PackedIdRelVec packed;
	auto postings = SimplePostings(1, 60);
	{
		PackedIdRelVec::AppendBuffer tail(packed);
		tail.Commit();
		tail.Append(postings.begin(), postings.end());
	}
	EXPECT_TRUE(packed.empty());
	EXPECT_EQ(packed.heap_size(), 0u);
	EXPECT_EQ(packed.array_found_pos(), std::numeric_limits<size_t>::max());

	PackedIdRelVec::AppendBuffer tail(packed);
	tail.Append(postings.begin(), postings.end());
	tail.Commit();
	ASSERT_NO_FATAL_FAILURE(CheckPostings(packed, postings));
	const size_t capacity = packed.heap_size();
	tail.Commit();
	EXPECT_EQ(packed.heap_size(), capacity);
	ASSERT_NO_FATAL_FAILURE(CheckPostings(packed, postings));
}

TEST(FTPackedPostings, ParallelAndSerialIncrementalBuildsHaveSamePostings) {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	PackedHolder serial(&cfg), parallel(&cfg);
	for (const VDocVersion version : {1u, 2u, 3u}) {
		AddDocuments(serial, 512, version, BuildMode::SingleThread);
		AddDocuments(parallel, 512, version, BuildMode::Parallel);
		for (const std::string_view word : {"common", "oldonly", "newonly"}) {
			if (!serial.ContainsWord(word)) {
				EXPECT_FALSE(parallel.ContainsWord(word));
				continue;
			}
			ASSERT_TRUE(parallel.ContainsWord(word));
			const auto lhs = serial.GetWordOccurencesByOrdinal(serial.FindWordOrdinal(word));
			const auto rhs = parallel.GetWordOccurencesByOrdinal(parallel.FindWordOrdinal(word));
			ASSERT_NO_FATAL_FAILURE(CheckPackedBytes(*lhs, *rhs));
			EXPECT_LE(rhs->heap_size(), rhs->data_size() + (rhs->data_size() + 9) / 10);
		}
	}
}

TEST(FTPackedPostings, CleanupCompactsBeforePublicationAndKeepsSnapshots) {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	PackedHolder holder(&cfg);
	AddDocuments(holder, 256, 1, BuildMode::Parallel);
	AddDocuments(holder, 512, 2, BuildMode::Parallel);
	std::vector<std::shared_ptr<const PackedIdRelVec>> before;
	for (size_t ordinal = 0; ordinal < holder.wordsMap_.size(); ++ordinal) {
		std::ignore = before.emplace_back(holder.GetWordOccurencesByOrdinal(ordinal));
	}
	const auto memoryBefore = holder.GetMemStat().memory;
	const NeverCanceled cancelable;
	const auto stat = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version == 1; }, cancelable);
	ASSERT_EQ(stat.deletedPostings, 512u);
	size_t released = 0;
	for (size_t ordinal = 0; ordinal < before.size(); ++ordinal) {
		const auto after = holder.GetWordOccurencesByOrdinal(ordinal);
		if (holder.GetWord(holder.GetWordIdByOrdinal(ordinal)) == u"newonly") {
			EXPECT_EQ(after.get(), before[ordinal].get());
		} else {
			EXPECT_NE(after.get(), before[ordinal].get());
			EXPECT_EQ(after->heap_size(), after->data_size());
		}
		for (const auto& posting : *after) {
			EXPECT_EQ(posting.VdocVersion(), 2u);
		}
		released += before[ordinal]->heap_size() - after->heap_size();
	}
	EXPECT_GT(released, 0u);
	const auto memoryAfter = holder.GetMemStat().memory;
	EXPECT_EQ(memoryBefore.TotalSize() - memoryAfter.TotalSize(), released);
	EXPECT_EQ(memoryBefore.postingsSize - memoryAfter.postingsSize, released);
	ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
	const auto common = holder.FindWordOrdinal("common");
	EXPECT_EQ(before[common]->size(), 768u);
	EXPECT_EQ(holder.GetWordOccurencesByOrdinal(common)->size(), 512u);
	const auto oldOnly = holder.FindWordOrdinal("oldonly");
	EXPECT_TRUE(holder.GetWordOccurencesByOrdinal(oldOnly)->empty());

	before.clear();
	EXPECT_EQ(holder.GetMemStat().memory.postingsSize, memoryAfter.postingsSize)
		<< "Releasing obsolete snapshots must not change the memory of current posting lists";

	const auto snapshot = holder.GetWordOccurencesByOrdinal(common);
	const auto unchanged = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version == 1; }, cancelable);
	EXPECT_EQ(unchanged.deletedPostings, 0u);
	EXPECT_EQ(holder.GetWordOccurencesByOrdinal(common).get(), snapshot.get());
}

TEST(FTPackedPostings, CpuCleanupCompactsBeforePublication) {
	reindexer::FTConfig cfg(1);
	cfg.stopWords.clear();
	cfg.maxTypos = 0;
	cfg.maxTypoLen = 0;
	reindexer::DataHolder<reindexer::IdRelVec> holder(&cfg);
	AddDocuments(holder, 256, 1, BuildMode::SingleThread);
	AddDocuments(holder, 512, 2, BuildMode::SingleThread);
	const auto ordinal = holder.FindWordOrdinal("common");
	auto before = holder.GetWordOccurencesByOrdinal(ordinal);
	const auto memoryBefore = holder.GetMemStat().memory;
	ASSERT_EQ(before->size(), 768u);
	const NeverCanceled cancelable;
	const auto stat = holder.OptimizeDeleted([](VDocId, VDocVersion version) { return version == 1; }, cancelable);
	EXPECT_EQ(stat.deletedPostings, 512u);
	const auto after = holder.GetWordOccurencesByOrdinal(ordinal);
	EXPECT_NE(after.get(), before.get());
	ASSERT_EQ(after->size(), 512u);
	EXPECT_EQ(after->capacity(), after->size());
	EXPECT_EQ(before->size(), 768u);
	const auto memoryAfter = holder.GetMemStat().memory;
	EXPECT_LT(memoryAfter.postingsSize, memoryBefore.postingsSize);
	EXPECT_EQ(memoryBefore.postingsSize - memoryAfter.postingsSize, memoryBefore.postingsCapacitySize - memoryAfter.postingsCapacitySize);
	ASSERT_NO_FATAL_FAILURE(CheckCachedMemory(holder));
	before.reset();
	EXPECT_EQ(holder.GetMemStat().memory.postingsSize, memoryAfter.postingsSize)
		<< "Releasing an obsolete snapshot must not change the memory of current posting lists";
	for (const auto& posting : *after) {
		EXPECT_EQ(posting.VdocVersion(), 2u);
		ASSERT_EQ(posting.Pos().size(), 3u);
	}
}

}  // namespace reindexer_tests
