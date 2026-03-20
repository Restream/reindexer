#pragma once
#include <string_view>
#include <thread>
#include "core/ft/typos.h"
#include "indextexttypes.h"
#include "tools/customhash.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

struct [[nodiscard]] WordTypo {
	WordTypo() = default;
	explicit WordTypo(WordIdType w) noexcept : word(w) {}
	explicit WordTypo(WordIdType w, const TyposVec& p) noexcept : word(w), positions(p) { assertrx_dbg(Sorted(p)); }

	int32_t GetWordID() const noexcept { return word.GetID(); }
	void SetWordID(int32_t id) noexcept { word.SetID(id); }

	WordIdType word;
	TyposVec positions;

	static bool Sorted(const TyposVec& positions) noexcept {
		for (size_t i = 1; i < positions.size(); ++i) {
			if (positions[i] <= positions[i - 1]) {
				return false;
			}
		}

		return true;
	}

	bool CheckMatch(std::string_view& word, std::wstring_view typo) const noexcept {
		const char* wordB = word.data();
		const char* wordE = word.data() + word.size();
		const wchar_t* typoB = typo.data();
		const wchar_t* typoE = typo.data() + typo.size();
		const wchar_t* typoP = typoB;
		const char* wordP = wordB;

		int8_t curPos = 0;
		size_t positionsPos = 0;
		while (wordP < wordE) {
			if (positionsPos < positions.size() && curPos == positions[positionsPos]) {
				++positionsPos;
				++curPos;
				std::ignore = utf8::unchecked::next(wordP);
				continue;
			}

			if (typoP >= typoE || utf8::unchecked::next(wordP) != static_cast<uint32_t>(*(typoP++))) {
				return false;
			}
			++curPos;
		}

		return typoP == typoE;
	}
};

static_assert(sizeof(WordTypo) <= 8, "This size is matter for overall size of the typos map");

// special map for storing typo -> WordTypo
// first we obtain hashes from all typos by calling CalcHash(typo)
// next we split all typos into groups by some hash bits (selecting number of groups approximately equal to <number of typos> / 8)
// finally we sort typos in each group by last kStepBits (6) hash bits (and we can store it instead of step_num because all typos in this
// map has same step_num and we dont need to store it) we store typos positions in separate array to avoid 4 bytes alignment because we want
// to use only 2 bytes per one typo position
class [[nodiscard]] TyposMap {
public:
	constexpr static uint8_t kStepBits = 6;
	constexpr static uint32_t kMaxStepNum = (1 << kStepBits);

	TyposMap() = default;

	static uint32_t CalcHash(std::wstring_view hs) noexcept { return _Hash_bytes(hs.data(), hs.length() * sizeof(wchar_t)); }

	void Build(const std::vector<std::vector<std::pair<uint32_t, WordTypo>>>& typosDatas, uint32_t stepNum, size_t numThreads) {
		stepNum_ = stepNum;
		size_t numTypos = 0;
		for (const auto& typosData : typosDatas) {
			numTypos += typosData.size();
		}

		if (!numTypos) {
			return;
		}

		size_t numGroups = 1;  // must be power of 2
		while (numGroups < numTypos / 8 + 1) {
			numGroups <<= 1;
		}

		groupShifts.resize(numGroups);
		for (const auto& typosData : typosDatas) {
			for (const auto& p : typosData) {
				groupShifts[getGroup(p.first)]++;
			}
		}

		uint32_t next = groupShifts[0];
		groupShifts[0] = 0;
		for (size_t i = 1; i < groupShifts.size(); ++i) {
			uint32_t tmp = groupShifts[i];
			groupShifts[i] = next;
			next += tmp;
		}

		std::vector<uint32_t> groupPositions = groupShifts;
		std::vector<WordTypo> typosOrderedByGroup(numTypos);

		for (const auto& typoDatas : typosDatas) {
			for (const auto& p : typoDatas) {
				uint32_t group = getGroup(p.first);
				typosOrderedByGroup[groupPositions[group]] = p.second;
				typosOrderedByGroup[groupPositions[group]].word.b.step_num = getHashBitsToStore(p.first);
				groupPositions[group]++;
			}
		}

		typoIds_.resize(numTypos);
		typoPositions_.resize(2 * numTypos);

		std::vector<std::thread> ths(numThreads);

		for (size_t threadIdx = 0; threadIdx < numThreads; ++threadIdx) {
			ths[threadIdx] = std::thread([threadIdx, numThreads, numGroups, numTypos, &typosOrderedByGroup, this]() noexcept {
				size_t groupsPerThread = numGroups / numThreads;
				size_t threadGroupsBegin = groupsPerThread * threadIdx;
				size_t threadGroupsEnd = threadGroupsBegin + groupsPerThread;
				if (threadIdx + 1 == numThreads) {
					threadGroupsEnd = numGroups;
				}

				for (size_t groupIdx = threadGroupsBegin; groupIdx < threadGroupsEnd; ++groupIdx) {
					size_t groupEnd = (groupIdx + 1 < numGroups) ? groupShifts[groupIdx + 1] : numTypos;
					buildGroup(typosOrderedByGroup, groupShifts[groupIdx], groupEnd);
				}
			});
		}

		for (auto& th : ths) {
			th.join();
		}
	}

	class [[nodiscard]] Iterator {
	public:
		Iterator(const std::vector<WordIdType>& typoIds, const std::vector<uint8_t>& typoPositions, size_t shift, size_t stepNum) noexcept
			: typoIds_(typoIds), typoPositions_(typoPositions), shift_(shift), stepNum_(stepNum) {}

		WordTypo operator*() const noexcept {
			assertrx_dbg(shift_ < typoIds_.size());
			WordTypo res;
			res.word = typoIds_[shift_];
			res.word.b.step_num = stepNum_;

			if (typoPositions_[2 * shift_ + 1] != 255) {
				res.positions = TyposVec(typoPositions_[2 * shift_], typoPositions_[2 * shift_ + 1]);
			} else if (typoPositions_[2 * shift_] != 255) {
				res.positions = TyposVec(typoPositions_[2 * shift_]);
			}

			return res;
		}

		Iterator& operator++() noexcept {
			++shift_;
			return *this;
		}

		bool operator==(const Iterator& other) const noexcept { return shift_ == other.shift_; }
		bool operator!=(const Iterator& other) const noexcept { return shift_ != other.shift_; }

	private:
		const std::vector<WordIdType>& typoIds_;
		const std::vector<uint8_t>& typoPositions_;
		size_t shift_ = 0;
		uint32_t stepNum_ = 0;
	};

	std::pair<Iterator, Iterator> TyposRange(uint32_t hash) const noexcept {
		if (groupShifts.empty()) {
			return {Iterator(typoIds_, typoPositions_, 0, stepNum_), Iterator(typoIds_, typoPositions_, 0, stepNum_)};
		}

		uint32_t group = getGroup(hash);
		uint32_t hashReduced = getHashBitsToStore(hash);

		uint32_t groupBegin = groupShifts[group];
		uint32_t groupEnd = typoIds_.size();
		if (group + 1 < groupShifts.size()) {
			groupEnd = groupShifts[group + 1];
		}

		while (groupBegin < groupEnd && typoIds_[groupBegin].b.step_num != hashReduced) {
			++groupBegin;
		}

		while (groupBegin < groupEnd && typoIds_[groupEnd - 1].b.step_num != hashReduced) {
			--groupEnd;
		}

		return {Iterator(typoIds_, typoPositions_, groupBegin, stepNum_), Iterator(typoIds_, typoPositions_, groupEnd, stepNum_)};
	}

	size_t heap_size() const noexcept {
		return groupShifts.capacity() * sizeof(uint32_t) + typoIds_.capacity() * sizeof(WordIdType) + typoPositions_.capacity();
	}

	size_t size() const noexcept { return typoIds_.size(); }

	void clear() {
		groupShifts.clear();
		typoIds_.clear();
		typoPositions_.clear();
	}

private:
	uint32_t getGroup(uint32_t hash) const noexcept {
		assertrx_dbg(std::popcount(groupShifts.size()) == 1);
		return (hash >> kStepBits) & (groupShifts.size() - 1);
	}

	uint32_t getHashBitsToStore(uint32_t hash) const noexcept { return hash & ((1U << kStepBits) - 1); }

	void buildGroup(std::vector<WordTypo>& typosOrderedByGroup, size_t groupShift, size_t groupEnd) noexcept {
		uint32_t hashValuesShifts[kMaxStepNum] = {0};

		for (size_t i = groupShift; i < groupEnd; ++i) {
			hashValuesShifts[typosOrderedByGroup[i].word.b.step_num]++;
		}

		uint32_t shift = hashValuesShifts[0];
		hashValuesShifts[0] = 0;
		for (size_t i = 1; i < kMaxStepNum; i++) {
			uint32_t tmp = hashValuesShifts[i];
			hashValuesShifts[i] = shift;
			shift += tmp;
		}

		for (size_t i = groupShift; i < groupEnd; ++i) {
			uint32_t hashReduced = typosOrderedByGroup[i].word.b.step_num;
			uint32_t sh = groupShift + hashValuesShifts[hashReduced]++;

			typoIds_[sh] = typosOrderedByGroup[i].word;
			typoPositions_[2 * sh] = (typosOrderedByGroup[i].positions.size() > 0) ? typosOrderedByGroup[i].positions[0] : 255U;
			typoPositions_[2 * sh + 1] = (typosOrderedByGroup[i].positions.size() > 1) ? typosOrderedByGroup[i].positions[1] : 255U;
		}
	}

	std::vector<uint32_t> groupShifts;
	std::vector<WordIdType> typoIds_;
	std::vector<uint8_t> typoPositions_;

	uint32_t stepNum_ = 0;
};

}  // namespace reindexer
