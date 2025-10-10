#pragma once
#include "core/ft/areaholder.h"
#include "core/index/ft_preselect.h"
#include "dataholder.h"

namespace reindexer {

template <typename PosTypeT>
int MergePositionsWithDist(const h_vector<IdRelType::PosType, 3>& positions, const h_vector<IdRelType::PosType, 3>& newWordPos,
						   unsigned int dist, PosTypeT& res, [[maybe_unused]] const char* inf) {
	unsigned int minDist = std::numeric_limits<int>::max();
	auto rightIt = newWordPos.begin();
	const auto leftEnd = positions.end();
	const auto rightEnd = newWordPos.end();
	for (auto leftIt = positions.begin(); leftIt != leftEnd; ++leftIt) {
		while (rightIt != rightEnd && rightIt->fpos < leftIt->fpos) {
			++rightIt;
		}
		// here right pos > left pos
		if (rightIt == rightEnd) {
			break;
		}
		if (rightIt->field() != leftIt->field()) {
			continue;
		}

		while (rightIt != rightEnd && rightIt->field() == leftIt->field() && rightIt->fpos - leftIt->fpos <= dist) {
			minDist = std::min(rightIt->fpos - leftIt->fpos, minDist);
			if constexpr (std::is_same_v<PosTypeT, h_vector<IdRelType::PosType, 3>>) {
				res.emplace_back(*rightIt);
			} else if constexpr (std::is_same_v<PosTypeT, h_vector<std::pair<IdRelType::PosType, int>, 3>>) {
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

RX_ALWAYS_INLINE int PositionsDistance(const h_vector<IdRelType::PosType, 3>& positions,
									   const h_vector<IdRelType::PosType, 3>& otherPositions, int max) {
	for (auto i = positions.begin(), j = otherPositions.begin(); i != positions.end() && j != otherPositions.end();) {
		// fpos - field number + word position in the field
		bool sign = i->fpos > j->fpos;
		int cur = sign ? i->fpos - j->fpos : j->fpos - i->fpos;
		if (cur < max && cur < (1 << IdRelType::PosType::posBits)) {
			max = cur;
			if (max <= 1) {
				break;
			}
		}
		(sign) ? j++ : i++;
	}
	return max;
}

typedef fast_hash_map<WordIdType, std::pair<size_t, size_t>, WordIdTypeHash, WordIdTypeEqual, WordIdTypeLess> FoundWordsType;

template <typename IdCont>
struct [[nodiscard]] TextSearchResult {
	const IdCont* vids;		   // indexes of documents (vdoc) containing the given word + position + field
	std::string_view pattern;  // word,translit,.....
	int proc;
};

// text search results for a single token (word) in a search query
template <typename IdCont>
class [[nodiscard]] TextSearchResults : public h_vector<TextSearchResult<IdCont>, 8> {
public:
	TextSearchResults(FtDSLEntry&& t, FoundWordsType* fwPtr) : term(std::move(t)), foundWords(fwPtr) { assertrx(foundWords); }
	void SwitchToInternalWordsMap() noexcept {
		if (!foundWordsPersonal_) {
			foundWordsPersonal_ = std::make_unique<FoundWordsType>();
		}
		foundWords = foundWordsPersonal_.get();
	}

	const OpType& Op() const noexcept { return term.opts.op; }
	OpType& Op() noexcept { return term.opts.op; }
	int GroupNum() const noexcept { return term.opts.groupNum; }
	int QPos() const noexcept { return term.opts.qpos; }
	int Distance() const noexcept { return term.opts.distance; }

	uint32_t idsCnt = 0;
	FtDSLEntry term;
	std::vector<size_t> synonyms;
	std::vector<size_t> synonymsGroups;
	FoundWordsType* foundWords = nullptr;

private:
	// Internal words map.
	// This map will be used instead of shared version for terms with 'irrelevant' variants
	std::unique_ptr<FoundWordsType> foundWordsPersonal_;
};

// Final information about found document
struct [[nodiscard]] MergeInfo {
	IdType id = 0;	   // Virtual id of merged document (index in vdocs)
	int32_t proc = 0;  // Rank of document
	int8_t field = 0;  // Field index, where was match
};

struct [[nodiscard]] MergeInfoAreas {
	IdType id = 0;	   // Virtual id of merged document (index in vdocs)
	int32_t proc = 0;  // Rank of document
	uint32_t areaIndex = std::numeric_limits<uint32_t>::max();
	int8_t field = 0;  // Field index, where was match
};

struct [[nodiscard]] MergeData : public std::vector<MergeInfo> {
	using InfoType = MergeInfo;

	MergeData() = default;
	MergeData(const MergeData& other) = delete;
	MergeData(MergeData&& other) = default;

	int maxRank = 0;
};

template <typename AreaType>
struct [[nodiscard]] MergeDataAreas : public std::vector<MergeInfoAreas> {
	using InfoType = MergeInfoAreas;
	using AT = AreaType;

	MergeDataAreas() = default;
	MergeDataAreas(const MergeDataAreas& other) = delete;
	MergeDataAreas(MergeDataAreas&& other) = default;

	int maxRank = 0;
	std::vector<AreasInDocument<AreaType>> vectorAreas;
};

template <class MergeDataType>
struct PhraseMergerDocumentData;

// Intermediate information about document found at current merge step. Used only for queries with 2 or more terms
struct [[nodiscard]] MergerDocumentData {
	explicit MergerDocumentData(IdRelType&& c, int r, int q) noexcept : nextTermPositions(std::move(c.Pos())), rank(r), qpos(q) {}
	explicit MergerDocumentData(int r, int q) noexcept : rank(r), qpos(q) {}
	MergerDocumentData(MergerDocumentData&&) noexcept = default;
	h_vector<IdRelType::PosType, 3> lastTermPositions;	// positions of matched document of current step
	h_vector<IdRelType::PosType, 3> nextTermPositions;	// positions of matched document of next step
	int32_t rank = 0;									// Rank of current matched document
	int32_t qpos = 0;									// Position in query
};

struct [[nodiscard]] TermRankInfo {
	int32_t termRank = 0;
	double bm25Norm = 0.0;
	double termLenBoost = 0.0;
	double positionRank = 0.0;
	double normDist = 0.0;
	double proc = 0.0;
	double fullMatchBoost = 0.0;
	std::string_view pattern;
	std::string ftDslTerm;

	std::string ToString() const {
		return fmt::format(
			R"json({{term_rank:{}, term:{}, pattern:{}, bm25_norm:{}, term_len_boost:{}, position_rank:{}, norm_dist:{}, proc:{}, full_match_boost:{}}} )json",
			termRank, ftDslTerm, pattern, bm25Norm, termLenBoost, positionRank, normDist, proc, fullMatchBoost);
	}
};

// Intermediate information about document found at current phrase merge step. Used only for phrases with 2 or more terms
template <>
struct PhraseMergerDocumentData<MergeData> {
	explicit PhraseMergerDocumentData(IdRelType&& termPositions, int termRank, const std::wstring&, const TermRankInfo&) noexcept
		: nextPhrasePositions(std::move(termPositions.Pos())), rank(termRank) {}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const IdRelType& additionalPositions, const std::wstring&, const TermRankInfo&) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.Size());

		for (const auto& p : additionalPositions.Pos()) {
			nextPhrasePositions.emplace_back(p);
		}
	}

	int MergeWithDist(const IdRelType& nextTermPositions, unsigned int dist, const std::wstring&, const TermRankInfo&) {
		return MergePositionsWithDist(lastPhrasePositions, nextTermPositions.Pos(), dist, nextPhrasePositions, "");
	}

	void SwitchPositions() {
		boost::sort::pdqsort_branchless(nextPhrasePositions.begin(), nextPhrasePositions.end());
		auto last = std::unique(nextPhrasePositions.begin(), nextPhrasePositions.end());
		nextPhrasePositions.resize(last - nextPhrasePositions.begin());

		lastPhrasePositions.swap(nextPhrasePositions);
		nextPhrasePositions.resize(0);
	}

	h_vector<IdRelType::PosType, 3> lastPhrasePositions;
	h_vector<IdRelType::PosType, 3> nextPhrasePositions;
	int32_t rank = -1;
};

template <>
struct PhraseMergerDocumentData<MergeDataAreas<Area>> {
	PhraseMergerDocumentData(const IdRelType& termPositions, int termRank, const std::wstring&, const TermRankInfo&) noexcept
		: rank(termRank) {
		nextPhrasePositions.reserve(termPositions.Size());
		for (const auto& p : termPositions.Pos()) {
			nextPhrasePositions.emplace_back(p, -1);
		}
	}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const IdRelType& additionalPositions, const std::wstring&, const TermRankInfo&) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.Size());
		for (const auto& p : additionalPositions.Pos()) {
			nextPhrasePositions.emplace_back(p, -1);
		}
	}

	int MergeWithDist(const IdRelType& subtermPositions, unsigned int dist, const std::wstring&, const TermRankInfo&) {
		return MergePositionsWithDist(lastPhrasePositions, subtermPositions.Pos(), dist, nextPhrasePositions, "");
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
			IdRelType::PosType cur = v.first;
			int prevIndex = v.second;

			IdRelType::PosType last = cur, first = cur;
			int indx = int(wordPosForChain.size()) - 2;
			while (indx >= 0 && prevIndex != -1) {
				auto pos = wordPosForChain[indx][prevIndex].first;
				prevIndex = wordPosForChain[indx][prevIndex].second;
				first = pos;
				--indx;
			}
			assertrx_throw(first.field() == last.field());
			if (area.InsertArea(Area(first.pos(), last.pos() + 1), cur.field(), rank, maxAreasInDoc)) {
				area.UpdateRank(float(rank));
			}
		}
		return area;
	}

	h_vector<IdRelType::PosType, 3> lastPhrasePositions;
	h_vector<std::pair<IdRelType::PosType, int>, 3> nextPhrasePositions;  // For phrases only. Collect all positions for subpatterns and
																		  // the index in the vector with which we merged
	int32_t rank = -1;

	h_vector<h_vector<std::pair<IdRelType::PosType, int>, 3>, 2> wordPosForChain;
};

template <>
struct PhraseMergerDocumentData<MergeDataAreas<AreaDebug>> {
	PhraseMergerDocumentData(IdRelType&& termPositions, int termRank, const std::wstring& termPattern, TermRankInfo& termInf) noexcept
		: rank(termRank) {
		nextPhrasePositions.reserve(termPositions.Size());
		for (const auto& p : termPositions.Pos()) {
			utf16_to_utf8(termPattern, termInf.ftDslTerm);
			nextPhrasePositions.emplace_back(PosTypeDebug(p, termInf.ToString()), -1);
		}
	}
	PhraseMergerDocumentData(PhraseMergerDocumentData&&) noexcept = default;

	void AddPositions(const IdRelType& additionalPositions, const std::wstring& termPattern, TermRankInfo& termInf) {
		nextPhrasePositions.reserve(nextPhrasePositions.size() + additionalPositions.Size());
		utf16_to_utf8(termPattern, termInf.ftDslTerm);
		for (const auto& p : additionalPositions.Pos()) {
			nextPhrasePositions.emplace_back(PosTypeDebug(p, termInf.ToString()), -1);
		}
	}

	int MergeWithDist(const IdRelType& subtermPositions, unsigned int dist, const std::wstring& termPattern, TermRankInfo& termInf) {
		utf16_to_utf8(termPattern, termInf.ftDslTerm);
		const std::string infoStr = termInf.ToString();
		return MergePositionsWithDist(lastPhrasePositions, subtermPositions.Pos(), dist, nextPhrasePositions, infoStr.c_str());
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
				if (area.InsertArea(AreaDebug(pos.pos(), pos.pos() + 1, std::move(pos.info), mode), cur.field(), rank, -1)) {
					area.UpdateRank(float(rank));
				}

				--indx;
			}
		}
		return area;
	}

	h_vector<IdRelType::PosType, 3> lastPhrasePositions;
	h_vector<std::pair<PosTypeDebug, int>, 3>
		nextPhrasePositions;  // For group only. Collect all positions for subpatterns and the index in the vector with which we merged
	h_vector<h_vector<std::pair<PosTypeDebug, int>, 3>, 2> wordPosForChain;

	int32_t rank = -1;
};

template <typename AreaType>
void copyAreas(AreasInDocument<AreaType>& from, AreasInDocument<AreaType>& to, int32_t rank, size_t fieldSize, int maxAreasInDoc) {
	for (size_t f = 0; f < fieldSize; f++) {
		auto areas = from.GetAreas(f);
		if (areas) {
			areas->MoveAreas(to, f, rank, std::is_same_v<AreaType, AreaDebug> ? -1 : maxAreasInDoc);
		}
	}
}

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] Merger {
public:
	constexpr static bool kWithRegularAreas = std::is_same_v<MergeDataType, MergeDataAreas<Area>>;
	constexpr static bool kWithDebugAreas = std::is_same_v<MergeDataType, MergeDataAreas<AreaDebug>>;
	constexpr static bool kWithAreas = kWithRegularAreas || kWithDebugAreas;

	using InfoType = typename MergeDataType::InfoType;

	Merger(DataHolder<IdCont>& holder, FtMergeStatuses::Statuses& mergeStatuses, size_t fieldSize, int maxAreasInDoc, bool inTransaction,
		   const RdxContext& ctx)
		: holder_(holder),
		  fieldSize_(fieldSize),
		  maxAreasInDoc_(maxAreasInDoc),
		  mergeStatuses_(mergeStatuses),
		  inTransaction_(inTransaction),
		  ctx_(ctx) {
		assertrx_throw(mergeStatuses.size() == holder_.vdocs_.size());
	}

	template <typename Bm25Type>
	MergeDataType Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t totalORVids, const std::vector<size_t>& synonymsBounds,
						RankSortType rankSortType);

private:
	const InfoType& getMergeData(index_t docId) const noexcept { return mergeData_[idoffsets_[docId]]; }
	const MergerDocumentData& getMergeDataExtended(index_t docId) const noexcept { return mergeDataExtended_[idoffsets_[docId]]; }
	InfoType& getMergeData(index_t docId) noexcept { return mergeData_[idoffsets_[docId]]; }
	MergerDocumentData& getMergeDataExtended(index_t docId) noexcept { return mergeDataExtended_[idoffsets_[docId]]; }

	void init(const std::vector<TextSearchResults<IdCont>>& rawResults, size_t docsSize, size_t maxMergedSize) {
		mergeData_.reserve(maxMergedSize);
		if (rawResults.size() > 1) {
			idoffsets_.resize(docsSize);
			mergeDataExtended_.reserve(maxMergedSize);

			if constexpr (kWithAreas) {
				mergeData_.vectorAreas.reserve(maxMergedSize);
			}
		}

		if (rawResults.size() == 1 && rawResults[0].size() > 1) {
			idoffsets_.resize(docsSize);
		}
	}

	template <typename Bm25Type>
	MergeDataType mergeSimple(TextSearchResults<IdCont>& singleTermRes, RankSortType rankSortType);
	template <typename Bm25Type>
	void mergeTermResults(TextSearchResults<IdCont>& termRes, index_t termIndex, std::vector<bool>* bitmask);

	template <typename Bm25T>
	void mergePhraseResults(std::vector<TextSearchResults<IdCont>>& rawResults, size_t phraseBegin, size_t phraseEnd, OpType phraseOp,
							int phraseQPos);

	void addFullMatchBoost(size_t numTerms) {
		const auto& vdocs = holder_.vdocs_;
		for (size_t idx = 0; idx < mergeData_.size(); ++idx) {
			auto& md = mergeData_[idx];
			if (size_t(vdocs[md.id].wordsCount[md.field]) == numTerms) {
				md.proc *= holder_.cfg_->fullMatchBoost;
			}

			if (mergeData_.maxRank < md.proc) {
				mergeData_.maxRank = md.proc;
			}
		}
	}

	void sortResults(RankSortType rankSortType) {
		switch (rankSortType) {
			case RankSortType::RankOnly:
			case RankSortType::IDAndPositions:
				boost::sort::pdqsort_branchless(mergeData_.begin(), mergeData_.end(),
												[](const InfoType& l, const InfoType& r) noexcept { return l.proc > r.proc; });
				return;
			case RankSortType::RankAndID:
			case RankSortType::IDOnly:
				return;
			case RankSortType::ExternalExpression:
				throw Error(errLogic, "RankSortType::ExternalExpression not implemented.");
				break;
		}
	}

	size_t numDocs() const noexcept { return mergeData_.size(); }

	void addDoc(int docId, index_t mergeStatus, int32_t proc, int8_t field) {
		InfoType info{.id = docId, .proc = proc, .field = field};

		if constexpr (kWithAreas) {
			auto& area = mergeData_.vectorAreas.emplace_back();
			area.ReserveField(fieldSize_);
			info.areaIndex = mergeData_.vectorAreas.size() - 1;
		}

		mergeData_.push_back(std::move(info));
		mergeStatuses_[docId] = mergeStatus;
		if (!idoffsets_.empty()) {
			idoffsets_[docId] = mergeData_.size() - 1;
		}
	}

	void addDoc(int docId, index_t mergeStatus, int32_t proc, int field, IdRelType positions, int qpos, TermRankInfo& subtermInf,
				const std::wstring& pattern) {
		addDoc(docId, mergeStatus, proc, field);
		addLastDocAreas(positions, proc, subtermInf, pattern);
		mergeDataExtended_.emplace_back(std::move(positions), proc, qpos);
	}

	void addDocAreas(int docId, const IdRelType& positions, int32_t rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			auto& md = getMergeData(docId);
			addAreas(md.areaIndex, positions, rank, termInf, pattern);
		}
	}

	void addLastDocAreas(const IdRelType& positions, int32_t rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithAreas) {
			size_t lastAreaIdx = mergeData_.vectorAreas.size() - 1;
			addAreas(lastAreaIdx, positions, rank, termInf, pattern);
		}
	}

	void addAreas(size_t areaIdx, const IdRelType& positionsInDoc, int32_t rank, TermRankInfo& termInf, const std::wstring& pattern) {
		if constexpr (kWithRegularAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			for (auto pos : positionsInDoc.Pos()) {
				if (!docAreas.AddWord(Area(pos.pos(), pos.pos() + 1), pos.field(), rank, maxAreasInDoc_)) {
					break;
				}
			}
			docAreas.UpdateRank(rank);
		} else if constexpr (kWithDebugAreas) {
			auto& docAreas = mergeData_.vectorAreas[areaIdx];
			utf16_to_utf8(pattern, const_cast<std::string&>(termInf.ftDslTerm));
			for (auto pos : positionsInDoc.Pos()) {
				if (!docAreas.AddWord(AreaDebug(pos.pos(), pos.pos() + 1, termInf.ToString(), AreaDebug::PhraseMode::None), pos.field(),
									  termInf.termRank, -1)) {
					break;
				}
			}
			docAreas.UpdateRank(termInf.termRank);
		}
	}

	void switchToNextWord() {
		for (auto& mdExt : mergeDataExtended_) {
			if (mdExt.nextTermPositions.size()) {
				mdExt.lastTermPositions.swap(mdExt.nextTermPositions);
				mdExt.nextTermPositions.resize(0);
				mdExt.rank = 0;
			}
		}
	}

	void removeDocs(const MergeDataType& docsToRemove) {
		for (const auto& mdToRemove : docsToRemove) {
			if (mdToRemove.proc == 0) {
				continue;
			}
			int docId = mdToRemove.id;
			if (mergeStatuses_[docId] != 0 && mergeStatuses_[docId] != FtMergeStatuses::kExcluded) {
				getMergeData(docId).proc = 0;
			}
			mergeStatuses_[docId] = FtMergeStatuses::kExcluded;
		}
	}

	void removeAllDocsExcept(const std::vector<bool>& docsMask) noexcept {
		for (auto& md : mergeData_) {
			if (!docsMask[md.id]) {
				mergeStatuses_[md.id] = FtMergeStatuses::kExcluded;
				md.proc = 0;
			}
		}
	}

private:
	MergeDataType mergeData_;
	std::vector<MergerDocumentData> mergeDataExtended_;
	std::vector<MergeOffsetT> idoffsets_;
	bool hasBeenAnd_ = false;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;

	FtMergeStatuses::Statuses& mergeStatuses_;

	bool inTransaction_ = false;
	const RdxContext& ctx_;
};

template <typename IdCont, typename MergeDataType, typename MergeOffsetT>
class [[nodiscard]] PhraseMerger {
public:
	using InfoType = typename MergeDataType::InfoType;
	using DocumentDataType = PhraseMergerDocumentData<MergeDataType>;

	PhraseMerger(DataHolder<IdCont>& holder, size_t fieldSize, int maxAreasInDoc, bool inTransaction, const RdxContext& ctx)
		: holder_(holder), fieldSize_(fieldSize), maxAreasInDoc_(maxAreasInDoc), inTransaction_(inTransaction), ctx_(ctx) {}

	template <typename Bm25T>
	void Merge(std::vector<TextSearchResults<IdCont>>& rawResults, size_t from, size_t to);

	size_t NumDocsMerged() const noexcept { return mergeData_.size(); }
	const InfoType& GetMergeData(size_t mergedDocIdx) const noexcept { return mergeData_[mergedDocIdx]; }
	const DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) const noexcept { return mergeDataExtended_[mergedDocIdx]; }
	InfoType& GetMergeData(size_t mergedDocIdx) noexcept { return mergeData_[mergedDocIdx]; }
	DocumentDataType& GetMergeDataExtended(size_t mergedDocIdx) noexcept { return mergeDataExtended_[mergedDocIdx]; }
	MergeDataType& GetMergeData() noexcept { return mergeData_; }
	const MergeDataType& GetMergeData() const noexcept { return mergeData_; }

private:
	void init(std::vector<TextSearchResults<IdCont>>& rawResults, size_t from, size_t to) {
		assertrx_throw(from < to);
		assertrx_throw(to <= rawResults.size());
		mergeStatuses_.resize(holder_.vdocs_.size(), 0);
		// upper estimate number of documents
		uint32_t idsMaxCnt = rawResults[from].idsCnt;
		uint32_t maxMergedDocs = std::min(holder_.cfg_->mergeLimit, idsMaxCnt);
		mergeData_.reserve(maxMergedDocs);
		mergeDataExtended_.reserve(maxMergedDocs);

		if (to - from > 1) {
			idoffsets_.resize(holder_.vdocs_.size());
		}
	}

	template <typename Bm25T>
	void mergePhraseTerm(TextSearchResults<IdCont>& termRes, index_t termIdx, bool firstTerm);

	MergeDataType mergeData_;
	std::vector<DocumentDataType> mergeDataExtended_;

	std::vector<MergeOffsetT> idoffsets_;
	FtMergeStatuses::Statuses mergeStatuses_;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_ = 0;
	int maxAreasInDoc_ = 0;

	bool inTransaction_ = false;
	const RdxContext& ctx_;
};

}  // namespace reindexer
