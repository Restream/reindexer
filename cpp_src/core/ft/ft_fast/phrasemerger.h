#pragma once
#include "core/ft/areaholder.h"
#include "core/index/ft_preselect.h"
#include "dataholder.h"
#include "estl/dynamic_bitset.h"

namespace reindexer {

namespace ft {

RX_ALWAYS_INLINE void InitFrom(PositionsVector&& source, PositionsVector& dest) {
	if (source.size() > PositionsVector::kHoldSize) {
		dest = std::move(source);
	} else {
		dest.resize(0);
		for (auto& p : source) {
			dest.emplace_back(std::move(p));
		}
	}
}

RX_ALWAYS_INLINE void InitFrom(const PositionsVector& source, PositionsVector& dest) { dest = source; }

template <typename PosTypeT>
int MergePositionsWithDist(const PositionsVector& positions, const PositionsVector& newWordPos, unsigned int dist, PosTypeT& res,
						   [[maybe_unused]] const char* inf) {
	unsigned int minDist = std::numeric_limits<int>::max();
	auto rightIt = newWordPos.begin();
	const auto leftEnd = positions.end();
	const auto rightEnd = newWordPos.end();
	for (auto leftIt = positions.begin(); leftIt != leftEnd; ++leftIt) {
		while (rightIt != rightEnd && rightIt->fullPos() < leftIt->fullPos()) {
			++rightIt;
		}
		if (rightIt == rightEnd) {
			break;
		}

		while (rightIt != rightEnd && rightIt->fullField() == leftIt->fullField() && rightIt->fullPos() - leftIt->fullPos() <= dist) {
			minDist = std::min(rightIt->fullPos() - leftIt->fullPos(), minDist);
			if constexpr (std::is_same_v<PosTypeT, PositionsVector>) {
				res.emplace_back(*rightIt);
			} else if constexpr (std::is_same_v<PosTypeT, h_vector<std::pair<PosType, int>, 3>>) {
				res.emplace_back(*rightIt, leftIt - positions.begin());
			} else if constexpr (std::is_same_v<PosTypeT, h_vector<std::pair<PosTypeDebug, int>, 3>>) {
				res.emplace_back(PosTypeDebug{*rightIt, inf}, leftIt - positions.begin());
			} else {
				static_assert(!sizeof(PosTypeT), "incorrect PosType type ");
			}
			++rightIt;
		}
	}
	return minDist;
}

typedef fast_hash_map<WordIdType, float, WordIdTypeHash, WordIdTypeEqual, WordIdTypeLess> FoundWordsProcsType;

template <typename IdCont>
struct [[nodiscard]] SubtermResults {
	const IdCont* vids;		   // indexes of documents (vdoc) containing the given word + position + field
	std::string_view pattern;  // word,translit,.....
	WordIdType patternId;
	float proc = 0.0;
};

// text search results for a single token (word) in a search query
template <typename IdCont>
class [[nodiscard]] TermResults {
public:
	TermResults() = default;
	TermResults(const FtDSLEntry& t) : term(t) {}
	TermResults(FtDSLEntry&& t) : term(std::move(t)) {}

	const OpType& Op() const noexcept { return term.Opts().op; }
	OpType& Op() noexcept { return term.Opts().op; }
	int GroupNum() const noexcept { return term.Opts().groupNum; }
	int QPos() const noexcept { return term.Opts().qpos; }
	int Distance() const noexcept { return term.Opts().distance; }

	uint32_t idsCnt = 0;
	FtDSLEntry term;
	std::vector<size_t> synonymsGroups;

	h_vector<SubtermResults<IdCont>, 8> subtermsResults;

	void UpdateProcs(const FoundWordsProcsType& finalProcs) {
		for (SubtermResults<IdCont>& sr : subtermsResults) {
			if (auto it = finalProcs.find(sr.patternId); it != finalProcs.end()) {
				sr.proc = it->second;
			}
		}
	}
};

// Final information about found document
struct [[nodiscard]] MergeInfo {
	IdType id = 0;				 // Virtual id of merged document (index in vdocs)
	float proc = 0;				 // Rank of document
	uint8_t field = 0;			 // Field index, where was match
	uint8_t normalizedProc = 0;	 // Normalized rank of document;
};

struct [[nodiscard]] MergeInfoAreas {
	IdType id = 0;	 // Virtual id of merged document (index in vdocs)
	float proc = 0;	 // Rank of document
	uint32_t areaIndex = std::numeric_limits<uint32_t>::max();
	uint8_t field = 0;			 // Field index, where was match
	uint8_t normalizedProc = 0;	 // Normalized rank of document;
};

struct [[nodiscard]] MergeData : public std::vector<MergeInfo> {
	using InfoType = MergeInfo;

	MergeData() = default;
	MergeData(const MergeData& other) = delete;
	MergeData(MergeData&& other) = default;
};

template <typename AreaType>
struct [[nodiscard]] MergeDataAreas : public std::vector<MergeInfoAreas> {
	using InfoType = MergeInfoAreas;
	using AT = AreaType;

	MergeDataAreas() = default;
	MergeDataAreas(const MergeDataAreas& other) = delete;
	MergeDataAreas(MergeDataAreas&& other) = default;

	std::vector<AreasInDocument<AreaType>> vectorAreas;
};

struct [[nodiscard]] TermRankInfo {
	float termRank = 0;
	float bm25Norm = 0.0;
	float termLenBoost = 0.0;
	float positionRank = 0.0;
	float normDist = 0.0;
	float proc = 0.0;
	float fullMatchBoost = 0.0;
	std::string_view pattern;
	std::string ftDslTerm;

	std::string ToString() const {
		return fmt::format(
			R"json({{term_rank:{}, term:{}, pattern:{}, bm25_norm:{}, term_len_boost:{}, position_rank:{}, norm_dist:{}, proc:{}, full_match_boost:{}}} )json",
			termRank, ftDslTerm, pattern, bm25Norm, termLenBoost, positionRank, normDist, proc, fullMatchBoost);
	}
};

template <class MergeDataType>
struct PhraseMergerDocumentData;

// Intermediate information about document found at current phrase merge step. Used only for phrases with 2 or more terms
template <>
struct PhraseMergerDocumentData<MergeData> {
	explicit PhraseMergerDocumentData(PositionsVector&& positions, float termRank, const std::wstring&, const TermRankInfo&) noexcept
		: nextPhrasePositions(std::move(positions)), rank(termRank) {}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const PositionsVector& additionalPositions, const std::wstring&, const TermRankInfo&) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.size());

		for (const auto& p : additionalPositions) {
			nextPhrasePositions.emplace_back(p);
		}
	}

	int MergeWithDist(const PositionsVector& nextTermPositions, unsigned int dist, const std::wstring&, const TermRankInfo&) {
		return MergePositionsWithDist(lastPhrasePositions, nextTermPositions, dist, nextPhrasePositions, "");
	}

	void SwitchPositions() {
		boost::sort::pdqsort_branchless(nextPhrasePositions.begin(), nextPhrasePositions.end());
		auto last = std::unique(nextPhrasePositions.begin(), nextPhrasePositions.end());
		nextPhrasePositions.resize(last - nextPhrasePositions.begin());

		lastPhrasePositions.swap(nextPhrasePositions);
		nextPhrasePositions.resize(0);
	}

	PositionsVector lastPhrasePositions;
	PositionsVector nextPhrasePositions;
	float rank = -1;
};

template <>
struct PhraseMergerDocumentData<MergeDataAreas<Area>> {
	PhraseMergerDocumentData(const PositionsVector& termPositions, float termRank, const std::wstring&, const TermRankInfo&)
		: rank(termRank) {
		nextPhrasePositions.reserve(termPositions.size());
		for (const auto& p : termPositions) {
			nextPhrasePositions.emplace_back(p, -1);
		}
	}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const PositionsVector& additionalPositions, const std::wstring&, const TermRankInfo&) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.size());
		for (const auto& p : additionalPositions) {
			nextPhrasePositions.emplace_back(p, -1);
		}
	}

	int MergeWithDist(const PositionsVector& subtermPositions, unsigned int dist, const std::wstring&, const TermRankInfo&) {
		return MergePositionsWithDist(lastPhrasePositions, subtermPositions, dist, nextPhrasePositions, "");
	}

	void SwitchPositions() {
		boost::sort::pdqsort_branchless(nextPhrasePositions.begin(), nextPhrasePositions.end(),
										[](const auto& l, const auto& r) noexcept { return l.first < r.first; });

		auto last = std::unique(nextPhrasePositions.begin(), nextPhrasePositions.end());
		nextPhrasePositions.resize(last - nextPhrasePositions.begin());

		lastPhrasePositions.resize(0);
		for (const auto& p : nextPhrasePositions) {
			lastPhrasePositions.emplace_back(p.first);
		}
		wordPosForChain.emplace_back(std::move(nextPhrasePositions));
		nextPhrasePositions.clear();
	}

	AreasInDocument<Area> CreateAreas(size_t maxAreasInDoc) const {
		AreasInDocument<Area> area;
		if (wordPosForChain.empty()) {
			return area;
		}
		for (const auto& v : wordPosForChain.back()) {
			PosType cur = v.first;
			int prevIndex = v.second;

			PosType last = cur, first = cur;
			int indx = int(wordPosForChain.size()) - 2;
			while (indx >= 0 && prevIndex != -1) {
				auto pos = wordPosForChain[indx][prevIndex].first;
				prevIndex = wordPosForChain[indx][prevIndex].second;
				first = pos;
				--indx;
			}
			assertrx_throw(first.field() == last.field());
			if (area.InsertArea(Area(first.pos(), last.pos() + 1, cur.arrayIdx()), cur.field(), rank, maxAreasInDoc)) {
				area.UpdateRank(rank);
			}
		}
		return area;
	}

	PositionsVector lastPhrasePositions;
	h_vector<std::pair<PosType, int>, 3> nextPhrasePositions;  // For phrases only. Collect all positions for subpatterns and
															   // the index in the vector with which we merged
	float rank = -1;

	h_vector<h_vector<std::pair<PosType, int>, 3>, 2> wordPosForChain;
};

template <>
struct PhraseMergerDocumentData<MergeDataAreas<AreaDebug>> {
	PhraseMergerDocumentData(const PositionsVector& termPositions, float termRank, const std::wstring& termPattern, TermRankInfo& termInf)
		: rank(termRank) {
		nextPhrasePositions.reserve(termPositions.size());
		for (const auto& p : termPositions) {
			utf16_to_utf8(termPattern, termInf.ftDslTerm);
			nextPhrasePositions.emplace_back(PosTypeDebug(p, termInf.ToString()), -1);
		}
	}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const PositionsVector& additionalPositions, const std::wstring& termPattern, TermRankInfo& termInf) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.size());
		utf16_to_utf8(termPattern, termInf.ftDslTerm);
		for (const auto& p : additionalPositions) {
			nextPhrasePositions.emplace_back(PosTypeDebug(p, termInf.ToString()), -1);
		}
	}

	int MergeWithDist(const PositionsVector& subtermPositions, unsigned int dist, const std::wstring& termPattern, TermRankInfo& termInf) {
		utf16_to_utf8(termPattern, termInf.ftDslTerm);
		const std::string infoStr = termInf.ToString();
		return MergePositionsWithDist(lastPhrasePositions, subtermPositions, dist, nextPhrasePositions, infoStr.c_str());
	}

	void SwitchPositions() {
		boost::sort::pdqsort_branchless(nextPhrasePositions.begin(), nextPhrasePositions.end(),
										[](const auto& l, const auto& r) noexcept { return l.first < r.first; });

		auto last = std::unique(nextPhrasePositions.begin(), nextPhrasePositions.end());
		nextPhrasePositions.resize(last - nextPhrasePositions.begin());

		lastPhrasePositions.resize(0);
		for (const auto& p : nextPhrasePositions) {
			lastPhrasePositions.emplace_back(p.first);
		}
		wordPosForChain.emplace_back(std::move(nextPhrasePositions));
		nextPhrasePositions.clear();
	}

	AreasInDocument<AreaDebug> CreateAreas(size_t) const {
		AreasInDocument<AreaDebug> area;
		if (wordPosForChain.empty()) {
			return area;
		}
		for (const auto& v : wordPosForChain.back()) {
			PosTypeDebug cur = v.first;
			int prevIndex = v.second;

			int indx = int(wordPosForChain.size()) - 1;
			while (indx >= 0 && prevIndex != -1) {
				PosTypeDebug pos = wordPosForChain[indx][prevIndex].first;
				prevIndex = wordPosForChain[indx][prevIndex].second;
				AreaDebug::PhraseMode mode = AreaDebug::PhraseMode::None;
				if (indx == int(wordPosForChain.size()) - 1) {
					mode = AreaDebug::PhraseMode::End;
				} else if (indx == 0) {
					mode = AreaDebug::PhraseMode::Start;
				}
				if (area.InsertArea(AreaDebug(pos.pos(), pos.pos() + 1, cur.arrayIdx(), std::move(pos.info), mode), cur.field(), rank,
									-1)) {
					area.UpdateRank(float(rank));
				}

				--indx;
			}
		}
		return area;
	}

	PositionsVector lastPhrasePositions;
	h_vector<std::pair<PosTypeDebug, int>, 3>
		nextPhrasePositions;  // For group only. Collect all positions for subpatterns and the index in the vector with which we merged
	h_vector<h_vector<std::pair<PosTypeDebug, int>, 3>, 2> wordPosForChain;

	float rank = -1;
};

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] PhraseMerger {
public:
	using InfoType = typename MergeDataType::InfoType;
	using DocumentDataType = PhraseMergerDocumentData<MergeDataType>;
	using BitsetType = DynamicBitset<64>;

	PhraseMerger(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc, bool inTransaction, const RdxContext& ctx)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc), inTransaction_(inTransaction), ctx_(ctx) {}

	template <typename Bm25T>
	void Merge(std::vector<TermResults<IdCont>>& rawResults, size_t from, size_t to);

	size_t NumDocsMerged() const noexcept { return mergeData_.size(); }
	const InfoType& GetMergeData(size_t mergedDocIdx) const noexcept { return mergeData_[mergedDocIdx]; }
	const DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) const noexcept { return mergeDataExtended_[mergedDocIdx]; }
	InfoType& GetMergeData(size_t mergedDocIdx) noexcept { return mergeData_[mergedDocIdx]; }
	DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) noexcept { return mergeDataExtended_[mergedDocIdx]; }
	MergeDataType& GetMergeData() noexcept { return mergeData_; }
	const MergeDataType& GetMergeData() const noexcept { return mergeData_; }

	void GetMergedDocsScores(BitsetType& bm, std::vector<uint16_t>& scores) const {
		bm.resize(0);
		bm.resize(holder_.vdocs_.size(), false);

		for (const auto& md : mergeData_) {
			if (md.proc > 0) {
				bm.set(md.id);

				if (phraseProc_ <= std::numeric_limits<uint16_t>::max() - scores[md.id]) {
					scores[md.id] += phraseProc_;
				}
			}
		}
	}

private:
	void init(std::vector<TermResults<IdCont>>& rawResults, size_t from, size_t to) {
		assertrx_throw(from < to);
		assertrx_throw(to <= rawResults.size());
		preselectedDocs_.resize(holder_.vdocs_.size(), true);
		nextTermDocs_.resize(holder_.vdocs_.size(), false);
		// upper estimate number of documents
		uint32_t idsMaxCnt = rawResults[from].idsCnt;
		maxMergedDocs_ = std::min(holder_.cfg_->mergeLimit, idsMaxCnt);
		mergeData_.reserve(maxMergedDocs_);
		mergeDataExtended_.reserve(maxMergedDocs_);

		if (to - from > 1) {
			idoffsets_.resize(holder_.vdocs_.size(), maxMergedDocs_);
		}

		maxDocId_ = holder_.vdocs_.size();

		long long sumProc = 0.0;
		for (size_t idx = from; idx < to; idx++) {
			if (rawResults[idx].subtermsResults.size() > 0) {
				sumProc += rawResults[idx].subtermsResults[0].proc;
			}
		}

		assertrx_throw(sumProc >= 0 && sumProc < std::numeric_limits<uint16_t>::max());
		phraseProc_ = static_cast<uint16_t>(sumProc);
	}

	void preselectDocsContainingAllTerms(std::vector<TermResults<IdCont>>& rawResults, size_t from, size_t to);

	template <typename Bm25T>
	void mergePhraseTerm(TermResults<IdCont>& termRes, bool firstTerm);

	uint16_t phraseProc_ = 0;

	MergeDataType mergeData_;
	std::vector<DocumentDataType> mergeDataExtended_;

	std::vector<MergeOffsetT> idoffsets_;
	BitsetType preselectedDocs_;
	BitsetType nextTermDocs_;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;
	uint32_t maxMergedDocs_ = 0;
	index_t maxDocId_ = 0;

	bool inTransaction_ = false;
	const RdxContext& ctx_;
};

}  // namespace ft
}  // namespace reindexer
