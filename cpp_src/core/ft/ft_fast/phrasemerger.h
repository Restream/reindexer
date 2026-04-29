#pragma once
#include "core/ft/areaholder.h"
#include "core/index/ft_preselect.h"
#include "dataholder.h"
#include "querymergedata.h"

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

// Final information about found document
struct [[nodiscard]] MergeInfo {
	IdType id = IdType::Zero();	 // Virtual id of merged document (index in vdocs)
	float proc = 0;				 // Rank of document
	uint8_t field = 0;			 // Field index, where was match
	uint8_t normalizedProc = 0;	 // Normalized rank of document;
};

struct [[nodiscard]] MergeInfoAreas {
	IdType id = IdType::Zero();	 // Virtual id of merged document (index in vdocs)
	float proc = 0;				 // Rank of document
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
struct [[nodiscard]] PhraseMergerDocumentData;

// Intermediate information about document found at current phrase merge step. Used only for phrases with 2 or more terms
template <>
struct [[nodiscard]] PhraseMergerDocumentData<MergeData> {
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
struct [[nodiscard]] PhraseMergerDocumentData<MergeDataAreas<Area>> {
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

	AreasInDocument<Area> CreateAreas(float proc, size_t maxAreasInDoc) const {
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
			if (area.InsertArea(Area(first.pos(), last.pos() + 1, cur.arrayIdx()), cur.field(), proc, maxAreasInDoc)) {
				area.UpdateRank(proc);
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
struct [[nodiscard]] PhraseMergerDocumentData<MergeDataAreas<AreaDebug>> {
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

	AreasInDocument<AreaDebug> CreateAreas(float proc, size_t) const {
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
				if (area.InsertArea(AreaDebug(pos.pos(), pos.pos() + 1, cur.arrayIdx(), std::move(pos.info), mode), cur.field(), proc,
									-1)) {
					area.UpdateRank(float(proc));
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

	PhraseMerger(DataHolder<IdCont>& holder, FtMergeStatuses::Statuses& docsExcluded, size_t fieldSize, int maxAreasInDoc,
				 bool inTransaction, const RdxContext& ctx)
		: holder_(holder),
		  docsExcluded_(docsExcluded),
		  fieldSize_(fieldSize),
		  maxAreasInDoc_(maxAreasInDoc),
		  inTransaction_(inTransaction),
		  ctx_(ctx) {}

	template <typename Bm25T>
	void Merge(PhraseResults<IdCont>& phrase);

	size_t NumDocsMerged() const noexcept { return mergeData_.size(); }
	const InfoType& GetMergeData(size_t mergedDocIdx) const noexcept { return mergeData_[mergedDocIdx]; }
	const DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) const noexcept { return mergeDataExtended_[mergedDocIdx]; }
	InfoType& GetMergeData(size_t mergedDocIdx) noexcept { return mergeData_[mergedDocIdx]; }
	DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) noexcept { return mergeDataExtended_[mergedDocIdx]; }
	MergeDataType& GetMergeData() noexcept { return mergeData_; }
	const MergeDataType& GetMergeData() const noexcept { return mergeData_; }

	void GetMergedDocsBitmask(BitsetType& bm) const {
		bm.ResizeAndReset(holder_.VDocsNumberInIndex());

		for (const auto& md : mergeData_) {
			if (md.proc > 0) {
				bm.set(md.id.ToNumber());
			}
		}
	}

	void ExcludeMergedDocsFromBitmask(BitsetType& bm) const {
		for (const auto& md : mergeData_) {
			if (md.proc > 0) {
				bm.reset(md.id.ToNumber());
			}
		}
	}

	void GetMergedDocsScore(std::vector<uint16_t>& scores) const {
		for (const auto& md : mergeData_) {
			if (md.proc > 0) {
				scores[md.id.ToNumber()] +=
					std::min<uint16_t>(phraseProc_, std::numeric_limits<uint16_t>::max() - scores[md.id.ToNumber()]);
			}
		}
	}

private:
	void init(PhraseResults<IdCont>& phrase) {
		assertrx_throw(phrase.NumTerms() > 0);

		preselectedDocs_.ResizeAndSet(holder_.VDocsNumberInIndex());
		nextTermDocs_.ResizeAndReset(holder_.VDocsNumberInIndex());

		maxMergedDocs_ = std::min(holder_.cfg_->mergeLimit, phrase.Term(0).MaxVDocs());
		maxMergedDocs_ = std::min<uint32_t>(maxMergedDocs_, std::numeric_limits<MergeOffsetT>::max());
		mergeData_.reserve(maxMergedDocs_);
		mergeDataExtended_.reserve(maxMergedDocs_);

		if (phrase.NumTerms() > 1) {
			idoffsets_.resize(holder_.VDocsNumberInIndex(), maxMergedDocs_);
		}

		maxDocId_ = holder_.VDocsNumberInIndex();
		phraseProc_ = phrase.CalcProc16();
	}

	void preselectDocsContainingAllTerms(PhraseResults<IdCont>& phrase);

	template <typename Bm25T>
	void mergePhraseTerm(TermResults<IdCont>& termRes, bool firstTerm);

	uint16_t phraseProc_ = 0;

	MergeDataType mergeData_;
	std::vector<DocumentDataType> mergeDataExtended_;

	std::vector<MergeOffsetT> idoffsets_;
	BitsetType preselectedDocs_;
	BitsetType nextTermDocs_;

	DataHolder<IdCont>& holder_;
	const FtMergeStatuses::Statuses& docsExcluded_;

	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;
	uint32_t maxMergedDocs_ = 0;
	index_t maxDocId_ = 0;

	bool inTransaction_ = false;
	const RdxContext& ctx_;
};

}  // namespace ft
}  // namespace reindexer
