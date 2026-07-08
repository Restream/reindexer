#include "indextext.h"
#include <memory>
#include "core/dbconfig.h"
#include "core/formatters/id_type_fmt.h"
#include "core/ft/ft_fast/selecterimpl.h"
#include "core/ft/functions/ft_function.h"
#include "core/ft/variants/kblayout.h"
#include "core/ft/variants/synonyms.h"
#include "core/ft/variants/translit.h"
#include "core/nsselecter/ranks_holder.h"
#include "core/rdxcontext.h"
#include "estl/contexted_locks.h"
#include "estl/smart_lock.h"
#include "sort/pdqsort.hpp"
#include "tools/clock.h"
#include "tools/errors.h"
#include "tools/logger.h"

namespace {
// Available stemmers for languages
const char* stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};
}  // namespace

namespace reindexer {

using std::chrono::duration_cast;
using std::chrono::milliseconds;

static FtCtx::Ptr createFtCtx(const Index::SelectContext& selectCtx) {
	assertrx_throw(selectCtx.selectFuncCtx);
	assertrx_dbg(!selectCtx.selectFuncCtx->ranks);
	selectCtx.selectFuncCtx->ranks = make_intrusive<RanksHolder>();
	return selectCtx.selectFuncCtx->selectFunc.CreateCtx(selectCtx.selectFuncCtx->indexNo, selectCtx.selectFuncCtx->ranks);
}

template <typename StoreType>
void IndexText<StoreType>::initSearchers() {
	size_t jsonPathIdx = 0;

	if (this->payloadType_) {
		const auto& fields = this->Fields();
		for (unsigned i = 0, s = fields.size(); i < s; i++) {
			auto fieldIdx = fields[i];
			if (fieldIdx == IndexValueType::SetByJsonPath) {
				assertrx(jsonPathIdx < fields.getJsonPathsLength());
				ftFields_.emplace(fields.getJsonPath(jsonPathIdx++), FtIndexFieldPros{.isIndexed = false, .fieldNumber = i});
			} else {
				ftFields_.emplace(this->payloadType_->Field(fieldIdx).Name(), FtIndexFieldPros{.isIndexed = true, .fieldNumber = i});
			}
		}
		if (ftFields_.size() != fields.size()) [[unlikely]] {
			throw Error(errParams, "Composite fulltext index '{}' contains duplicated fields", this->name_);
		}
		if (ftFields_.size() > kMaxFtCompositeFields) [[unlikely]] {
			throw Error(errParams, "Unable to create composite fulltext '{}' index with {} fields. Fields count limit is {}", this->name_,
						ftFields_.size(), kMaxFtCompositeFields);
		}
	}
}

template <typename StoreType>
void IndexText<StoreType>::initTermBoosts(FTConfig& cfg) {
	holder_->stemmedTermsBoost.clear();
	std::string stemstr;
	for (auto& [term, boost] : cfg.termsBoost) {
		holder_->stemmedTermsBoost[term] = std::max(holder_->stemmedTermsBoost[term], boost);
		for (auto& st : holder_->stemmers_) {
			stemstr.resize(0);
			st.second.stem(term, stemstr);
			if (getUTF8StringCharactersCount(stemstr) >= kMinStemRelevantLen) {
				holder_->stemmedTermsBoost[stemstr] = std::max(holder_->stemmedTermsBoost[stemstr], boost);
			}
		}
	}
}

template <typename StoreType>
void IndexText<StoreType>::initHolder(FTConfig& cfg) {
	switch (cfg.optimization) {
		case FTConfig::Optimization::Memory:
			holder_ = std::make_unique<DataHolder<PackedIdRelVec>>(&cfg);
			break;
		case FTConfig::Optimization::CPU:
			holder_ = std::make_unique<DataHolder<IdRelVec>>(&cfg);
			break;
		default:
			assertrx(0);
	}

	holder_->stemmers_.clear();
	holder_->translit_ = std::make_unique<Translit>();
	holder_->kbLayout_ = std::make_unique<KbLayout>();
	holder_->synonyms_ = std::make_unique<Synonyms>();
	for (const char** lang = stemLangs; *lang; ++lang) {
		holder_->stemmers_.emplace(*lang, *lang);
	}

	vdocsIndexed_ = 0;
	vdocsCommited_ = 0;

	initTermBoosts(cfg);
}

template <typename StoreType>
void IndexText<StoreType>::initConfig(const FTConfig* cfg) {
	if (cfg) {
		this->cfg_ = std::make_unique<FTConfig>(*cfg);
	} else {
		this->cfg_ = std::make_unique<FTConfig>(this->ftFields_.size());
		this->cfg_->parse(this->opts_.Config(), this->ftFields_);
	}
	initHolder(*cfg_);	// -V522
	this->holder_->synonyms_->SetConfig(this->cfg_.get());
}

template <typename StoreType>
IndexText<StoreType>::IndexText(const IndexText<StoreType>& other)
	: Base(other),
	  cache_ft_(other.cacheMaxSize_, other.hitsToCache_),
	  cacheMaxSize_(other.cacheMaxSize_),
	  hitsToCache_(other.hitsToCache_),
	  rowId2Vdoc_(other.rowId2Vdoc_),
	  vdocs_(other.vdocs_),
	  vdocSet_(other.vdocSet_.begin(), other.vdocSet_.end(), other.vdocSet_.bucket_count(), hash_vdoc(payloadType_, fields_, vdocs_),
			   equal_vdoc(payloadType_, fields_, vdocs_)) {
	cache_ft_.CopyInternalPerfStatsFrom(other.cache_ft_);

	vdocsIndexed_ = 0;
	vdocsCommited_ = 0;

	initSearchers();
	initConfig(other.cfg_.get());

	for (const auto& vd : vdocs_) {
		vdocsHeapSize_ += vd.heap_size();
		stringsHeapSize_ += vd.strings_heap_size();
	}
}

template <typename StoreType>
IndexText<StoreType>::IndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
								const NamespaceCacheConfigData& cacheCfg)
	: Base(idef, std::move(payloadType), std::move(fields)),
	  cache_ft_(cacheCfg.ftIdxCacheSize, cacheCfg.ftIdxHitsToCache),
	  cacheMaxSize_(cacheCfg.ftIdxCacheSize),
	  hitsToCache_(cacheCfg.ftIdxHitsToCache),
	  vdocSet_(1000, hash_vdoc(payloadType_, fields_, vdocs_), equal_vdoc(payloadType_, fields_, vdocs_)) {
	initSearchers();
	initConfig();

	static StoreType a;
	this->keyType_ = Variant(a).Type();
	this->selectKeyType_ = KeyValueType::String{};

	// create empty vdoc
	vdocs_.emplace_back();
	static_assert(kEmptyVDocId == 0);
	vdocsHeapSize_ += vdocs_[0].heap_size();
}

template <typename StoreType>
void IndexText<StoreType>::SetOpts(const IndexOpts& opts) {
	FTConfig oldCfg = *cfg_;

	std::string oldOptsCfg = this->opts_.Config();
	this->opts_ = opts;

	if (oldOptsCfg != opts.Config()) {
		try {
			cfg_->parse(this->opts_.Config(), ftFields_);
		} catch (...) {
			this->opts_.SetConfig(this->Type(), std::move(oldOptsCfg));
			cfg_->parse(this->opts_.Config(), ftFields_);
			throw;
		}
	}

	bool stopWordsEq = (oldCfg.stopWords.size() == cfg_->stopWords.size() &&
						std::equal(oldCfg.stopWords.begin(), oldCfg.stopWords.end(), cfg_->stopWords.begin()));

	if (!stopWordsEq || oldCfg.stemmers != cfg_->stemmers || oldCfg.maxTypoLen != cfg_->maxTypoLen ||
		oldCfg.enableNumbersSearch != cfg_->enableNumbersSearch || oldCfg.splitOptions != cfg_->splitOptions ||
		oldCfg.maxTypos != cfg_->maxTypos || oldCfg.optimization != cfg_->optimization || oldCfg.splitterType != cfg_->splitterType) {
		logFmt(LogInfo, "FulltextIndex config changed, it will be rebuilt on next search");
		this->isBuilt_ = false;
		if (oldCfg.optimization != cfg_->optimization || oldCfg.splitterType != cfg_->splitterType ||
			oldCfg.splitOptions != cfg_->splitOptions) {
			initHolder(*cfg_);
		} else {
			holder_->Clear();
		}

		holder_->status_ = FullRebuild;
		cache_ft_.Clear();
	} else {
		logFmt(LogInfo, "FulltextIndex config changed, cache cleared");
		cache_ft_.Clear();
	}

	holder_->synonyms_->SetConfig(cfg_.get());
	initTermBoosts(*cfg_);
}

template <typename StoreType>
void IndexText<StoreType>::ReconfigureCache(const NamespaceCacheConfigData& cacheCfg) {
	if (cacheMaxSize_ != cacheCfg.ftIdxCacheSize || hitsToCache_ != cacheCfg.ftIdxHitsToCache) {
		cacheMaxSize_ = cacheCfg.ftIdxCacheSize;
		hitsToCache_ = cacheCfg.ftIdxHitsToCache;
		if (cache_ft_.IsActive()) {
			cache_ft_.Reinitialize(cacheMaxSize_, hitsToCache_);
		}
	}
}

template <typename StoreType>
IndexMemStat IndexText<StoreType>::GetMemStat(const RdxContext& ctx) const {
	contexted_shared_lock lck(this->mtx_, ctx);
	IndexMemStat ret;
	ret.name = name_;
	ret.indexingStructSize = this->holder_->GetMemStat();
	ret.indexingStructSize += rowId2Vdoc_.capacity() * sizeof(uint32_t);
	ret.idsetCache = this->cache_ft_.GetMemStat();
	ret.isBuilt = this->isBuilt_;

	ret.indexingStructSize += vdocsHeapSize_ + vdocs_.capacity() * sizeof(VDoc<StoreType>);
	ret.dataSize += stringsHeapSize_;

	return ret;
}

template <typename StoreType>
template <typename DataType>
void IndexText<StoreType>::excludeFromVdoc(IdType rowId, DataType& dataDetached) {
	if (static_cast<size_t>(rowId.ToNumber()) >= rowId2Vdoc_.size()) {
		return;
	}

	uint32_t vdocId = rowId2Vdoc_[rowId.ToNumber()];
	if (vdocId == kEmptyVDocId) {
		return;
	}
	rowId2Vdoc_[rowId.ToNumber()] = kEmptyVDocId;
	auto& vdoc = vdocs_[vdocId];
	vdocsHeapSize_ -= vdoc.heap_size();
	if (vdoc.NumRows() == 1) {
		// removing final row, need to remove vdoc
		vdocSet_.erase(vdocId);
		stringsHeapSize_ -= vdoc.strings_heap_size();
	}

	vdoc.RemoveRow(rowId, dataDetached);
	vdocsHeapSize_ += vdoc.heap_size();
}

template <typename StoreType>
template <typename DataType>
bool IndexText<StoreType>::setVdocId(IdType rowId, const DataType& data) {
	if (rowId2Vdoc_.size() <= static_cast<size_t>(rowId.ToNumber())) {
		rowId2Vdoc_.resize(static_cast<size_t>(rowId.ToNumber()) + 1, kEmptyVDocId);
	}

	assertrx_dbg(rowId2Vdoc_[rowId.ToNumber()] == kEmptyVDocId);

	vdocs_.emplace_back();
	vdocs_.back().AddRow(rowId, data);
	auto res = vdocSet_.insert(vdocs_.size() - 1);
	const bool inserted = res.second;

	if (inserted) {
		rowId2Vdoc_[rowId.ToNumber()] = vdocs_.size() - 1;
		vdocsHeapSize_ += vdocs_.back().heap_size();
		stringsHeapSize_ += vdocs_.back().strings_heap_size();
		return true;
	} else {
		// same vdoc already exists
		vdocs_.pop_back();
		uint32_t vdocId = *res.first;
		rowId2Vdoc_[rowId.ToNumber()] = vdocId;
		vdocs_[vdocId].AddRow(rowId, data);
		return false;
	}
}

template <typename StoreType>
Variant IndexText<StoreType>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	VariantArray keys;
	keys.emplace_back(key);
	VariantArray result;

	IndexText<StoreType>::Upsert(result, keys, id, clearCache);
	assertrx_dbg(result.size() == 1);
	return result[0];
}

template <>
void IndexText<key_string>::Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) {
	if (rowId2Vdoc_.size() <= static_cast<size_t>(id.ToNumber())) {
		rowId2Vdoc_.resize(static_cast<size_t>(id.ToNumber()) + 1, kEmptyVDocId);
	}

	const bool hasNonNulls = std::ranges::any_of(keys, [](const auto& key) noexcept { return !key.IsNullValue(); });
	if (!hasNonNulls) [[unlikely]] {
		return;
	}

	clearCache = true;
	isBuilt_ = false;
	cache_ft_.Clear();

	h_vector<key_string, 1> data;
	excludeFromVdoc(id, data);
	size_t dataIdx = data.size();

	for (auto& key : keys) {
		if (!key.Type().Is<KeyValueType::Null>()) [[likely]] {
			data.emplace_back(static_cast<key_string>(key));
		}
	}

	std::ignore = setVdocId(id, data);
	auto& vdocData = vdocs_[rowId2Vdoc_[id.ToNumber()]].DataRef();

	result.reserve(keys.size());
	for (auto& key : keys) {
		if (!key.Type().Is<KeyValueType::Null>()) [[likely]] {
			result.emplace_back(Variant(vdocData[dataIdx++]));
		} else {
			result.emplace_back(Variant());
		}
	}
}

template <>
void IndexText<PayloadValue>::Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) {
	assertrx_dbg(keys.size() == 1 && !keys[0].Type().Is<KeyValueType::Null>());

	if (rowId2Vdoc_.size() <= static_cast<size_t>(id.ToNumber())) {
		rowId2Vdoc_.resize(static_cast<size_t>(id.ToNumber()) + 1, kEmptyVDocId);
	}

	PayloadValue data;
	excludeFromVdoc(id, data);

	clearCache = true;
	isBuilt_ = false;
	cache_ft_.Clear();

	assertrx_dbg(rowId2Vdoc_[id.ToNumber()] == kEmptyVDocId);
	const PayloadValue& pv = static_cast<const PayloadValue&>(keys[0]);
	std::ignore = setVdocId(id, pv);

	result.emplace_back(keys[0]);
}

template <>
bool IndexText<key_string>::RefreshCompositeKey(const Variant& /*key*/, IdType /*id*/) noexcept {
	assertrx_dbg(false);
	return false;
}

template <>
bool IndexText<PayloadValue>::RefreshCompositeKey(const Variant& key, IdType id) noexcept {
	assertrx_dbg(static_cast<size_t>(id.ToNumber()) < rowId2Vdoc_.size() && rowId2Vdoc_[id.ToNumber()] != kEmptyVDocId);
	vdocs_[rowId2Vdoc_[id.ToNumber()]].UpdateRowData(id, static_cast<const PayloadValue&>(key));
	cache_ft_.Clear();
	return true;
}

template <typename StoreType>
void IndexText<StoreType>::Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder& strHolder, bool& clearCache) {
	VariantArray keys;
	keys.emplace_back(key);
	IndexText<StoreType>::Delete(keys, id, mustExist, strHolder, clearCache);
}

template <>
void IndexText<key_string>::Delete(const VariantArray& keys, IdType id, MustExist /*mustExist*/, StringsHolder& strHolder,
								   bool& clearCache) {
	assertrx_dbg(static_cast<size_t>(id.ToNumber()) < rowId2Vdoc_.size());

	size_t numNotNulls = 0;
	for (auto& key : keys) {
		if (!key.IsNullValue()) {
			++numNotNulls;
		}
	}
	if (!numNotNulls) [[unlikely]] {
		return;
	}

	uint32_t vdocId = rowId2Vdoc_[id.ToNumber()];
	bool anythingDeletedFromTheRow = false;
	equal_key_string eq;

	// ToDo only all keys could be deleted after #2390
	for (key_string& st : vdocs_[vdocId].DataRef()) {
		for (auto& key : keys) {
			if (!key.IsNullValue() && eq(static_cast<key_string>(key), st)) {
				anythingDeletedFromTheRow = true;
				break;
			}
		}

		if (anythingDeletedFromTheRow) {
			break;
		}
	}

	assertrx_dbg(anythingDeletedFromTheRow);
	assertrx_dbg(vdocs_[vdocId].DataRef().size() >= numNotNulls);

	h_vector<key_string, 1> data;
	excludeFromVdoc(id, data);
	bool vdocRemoved = vdocs_[vdocId].Removed();

	h_vector<key_string, 1> remainingElements;
	// ToDo only all keys could be deleted after #2390
	for (key_string& st : data) {
		bool found = false;

		for (auto& key : keys) {
			if (!key.IsNullValue() && eq(st, static_cast<key_string>(key))) {
				found = true;
				if (vdocRemoved) {
					stringsHeapSize_ -= st.heap_size();
					strHolder.Add(std::move(st));
				}
				break;
			}
		}

		if (!found) {
			remainingElements.emplace_back(st);
		}
	}

	this->isBuilt_ = false;
	this->cache_ft_.Clear();
	clearCache = true;
	bool newVdocCreated = setVdocId(id, remainingElements);
	if (vdocRemoved && !newVdocCreated) {
		for (key_string& st : remainingElements) {
			stringsHeapSize_ -= st.heap_size();
			strHolder.Add(std::move(st));
		}
	}
}

// for composite text indexes
template <>
void IndexText<PayloadValue>::Delete([[maybe_unused]] const VariantArray& keys, IdType id, MustExist /*mustExist*/,
									 StringsHolder& /*strHolder*/, bool& clearCache) {
	assertrx_dbg(static_cast<size_t>(id.ToNumber()) < rowId2Vdoc_.size() && keys.size() == 1 && !keys[0].IsNullValue());

	PayloadValue data;
	excludeFromVdoc(id, data);
	this->isBuilt_ = false;
	this->cache_ft_.Clear();
	clearCache = true;
}

template <typename StoreType>
void IndexText<StoreType>::build(const RdxContext& rdxCtx) {
	smart_lock lckNonUnique(mtx_, rdxCtx, NonUnique);
	if (!this->isBuilt_) {
		// non atomic upgrade mutex to unique
		lckNonUnique.unlock();
		smart_lock lckUnique(mtx_, rdxCtx, Unique);
		if (!this->isBuilt_) {
			CommitFulltext();
		}
	}
}

static bool lessRank(RankT lhs, RankT rhs) noexcept { return lhs < rhs; }
static bool lessRank(RanksHolder::RankPos lhs, RanksHolder::RankPos rhs) noexcept { return lhs.rank < rhs.rank; }

template <typename StoreType>
template <auto(RanksHolder::*rankGetter)>
void IndexText<StoreType>::sortAfterSelect(IdSetPlain& mergedIds, RanksHolder& ranks, RankSortType rankSortType) {
	std::vector<size_t> sortIds;
	sortIds.reserve(mergedIds.Size());
	for (size_t i = 0; i < mergedIds.Size(); ++i) {
		sortIds.emplace_back(i);
	}

	if (rankSortType == RankSortType::RankAndID) {
		boost::sort::pdqsort(sortIds.begin(), sortIds.end(), [&ranks, &mergedIds](size_t i1, size_t i2) {
			const auto p1 = (ranks.*rankGetter)(i1);
			const auto p2 = (ranks.*rankGetter)(i2);
			if (lessRank(p2, p1)) {
				return true;
			} else if (lessRank(p1, p2)) {
				return false;
			} else {
				return mergedIds[i1] < mergedIds[i2];
			}
		});
	} else {
		boost::sort::pdqsort(sortIds.begin(), sortIds.end(), [&mergedIds](size_t i1, size_t i2) { return mergedIds[i1] < mergedIds[i2]; });
	}

	for (size_t i = 0; i < mergedIds.Size(); i++) {
		const auto vm = mergedIds[i];
		const auto vp = (ranks.*rankGetter)(i);
		size_t j = i;
		while (true) {
			size_t k = sortIds[j];
			sortIds[j] = j;
			if (k == i) {
				break;
			}
			mergedIds[j] = mergedIds[k];
			ranks.Set(j, (ranks.*rankGetter)(k));
			j = k;
		}
		mergedIds[j] = vm;
		ranks.Set(j, vp);
	}
}

template <typename StoreType>
template <typename MergeType>
IdSetPlain::Ptr IndexText<StoreType>::afterSelect(FtCtx& ftCtx, MergeType&& mergeData, RankSortType rankSortType,
												  FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt) {
	IdSetPlain::Ptr mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>();

	if (mergeData.empty()) {
		return mergedIds;
	}

	size_t rowsToReserve = 0;
	for (auto& md : mergeData) {
		rowsToReserve += vdocs_[md.id.ToNumber()].RowIds().size();
	}

	mergedIds->reserve(rowsToReserve);
	if constexpr (std::is_same_v<ft::MergeData, MergeType>) {
		for (auto& md : mergeData) {
			for (IdType rowId : vdocs_[md.id.ToNumber()].RowIds()) {
				if (useExternSt == FtUseExternStatuses::Yes && !statuses.rowIds[rowId.ToNumber()]) {
					continue;
				}

				ftCtx.Add(rowId, RankT(md.proc));
				mergedIds->AddUnordered(rowId);
			}
		}
	} else if constexpr (std::is_same_v<ft::MergeDataAreas<Area>, MergeType> || std::is_same_v<ft::MergeDataAreas<AreaDebug>, MergeType>) {
		for (auto& md : mergeData) {
			if (useExternSt == FtUseExternStatuses::No) {
				ftCtx.Add(vdocs_[md.id.ToNumber()].RowIds().begin(), vdocs_[md.id.ToNumber()].RowIds().end(), RankT(md.proc),
						  std::move(mergeData.vectorAreas[md.areaIndex]));

				for (IdType rowId : vdocs_[md.id.ToNumber()].RowIds()) {
					mergedIds->AddUnordered(rowId);
				}
			} else {
				ftCtx.Add(vdocs_[md.id.ToNumber()].RowIds().begin(), vdocs_[md.id.ToNumber()].RowIds().end(), RankT(md.proc),
						  statuses.rowIds, std::move(mergeData.vectorAreas[md.areaIndex]));

				for (IdType rowId : vdocs_[md.id.ToNumber()].RowIds()) {
					if (statuses.rowIds[rowId.ToNumber()]) {
						mergedIds->AddUnordered(rowId);
					}
				}
			}
		}
	} else {
		static_assert(!sizeof(MergeType), "incorrect MergeType");
	}

	assertrx_throw(mergedIds->Size() == ftCtx.Ranks().Size());

	auto& ranks = ftCtx.Ranks();
	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Total merge out: {} ids", mergedIds->Size());
		std::string str;
		for (size_t i = 0; i < ranks.Size();) {
			size_t j = i;
			for (; j < ranks.Size() && ranks.GetRank(i) == ranks.GetRank(j); j++);
			str += std::to_string(ranks.GetRank(i).Value()) + '%';
			if (j - i > 1) {
				str += '(';
				str += std::to_string(j - i);
				str += ')';
			}
			str += ' ';
			i = j;
		}
		logFmt(LogInfo, "Relevancy({}): {}", ranks.Size(), str);
	}

	switch (rankSortType) {
		case RankSortType::RankAndID:
		case RankSortType::IDOnly:
			sortAfterSelect<&RanksHolder::GetRank>(*mergedIds, ranks, rankSortType);
			break;
		case RankSortType::IDAndPositions:
			ranks.InitRRFPositions();
			sortAfterSelect<&RanksHolder::GetRankPos>(*mergedIds, ranks, rankSortType);
			break;
		case RankSortType::RankOnly:
			break;
		case RankSortType::ExternalExpression:
			throw Error(errLogic, "RankSortType::ExternalExpression not implemented.");
		default:
			throw_as_assert;
	}
	return mergedIds;
}

template <typename StoreType>
template <typename VectorType>
IdSetPlain::Ptr IndexText<StoreType>::applyCtxTypeAndSelect(DataHolder<VectorType>* d, FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction,
															RankSortType rankSortType, FtMergeStatuses&& statuses,
															FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	Selector<VectorType> selector{*d, holder_->cfg_->splitOptions, this->Fields().size(), holder_->cfg_->maxAreasInDoc};
	ftCtx.SetSplitter(this->holder_->GetSplitter());

	switch (ftCtx.Type()) {
		case FtCtxType::kFtCtx: {
			ft::MergeData mergeData = selector.template Process<ft::MergeData>(vdocsIndexed_, std::move(dsl), inTransaction, rankSortType,
																			   std::move(statuses.docsExcluded), rdxCtx, *this);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kFtArea: {
			ft::MergeDataAreas<Area> mergeData = selector.template Process<ft::MergeDataAreas<Area>>(
				vdocsIndexed_, std::move(dsl), inTransaction, rankSortType, std::move(statuses.docsExcluded), rdxCtx, *this);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kFtAreaDebug: {
			ft::MergeDataAreas<AreaDebug> mergeData = selector.template Process<ft::MergeDataAreas<AreaDebug>>(
				vdocsIndexed_, std::move(dsl), inTransaction, rankSortType, std::move(statuses.docsExcluded), rdxCtx, *this);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kNotSet:
		default:
			throw_assert(false);
	}
}

template <typename StoreType>
IdSetPlain::Ptr IndexText<StoreType>::Select(FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction, RankSortType rankSortType,
											 FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	switch (holder_->cfg_->optimization) {
		case FTConfig::Optimization::Memory: {
			DataHolder<PackedIdRelVec>* d = dynamic_cast<DataHolder<PackedIdRelVec>*>(holder_.get());
			assertrx_throw(d);
			return applyCtxTypeAndSelect<PackedIdRelVec>(d, ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(statuses),
														 useExternSt, rdxCtx);
		}
		case FTConfig::Optimization::CPU: {
			DataHolder<IdRelVec>* d = dynamic_cast<DataHolder<IdRelVec>*>(holder_.get());
			assertrx_throw(d);
			return applyCtxTypeAndSelect<IdRelVec>(d, ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(statuses), useExternSt,
												   rdxCtx);
		}
		default:
			throw_assert(false);
	}
}

template <typename StoreType>
SelectKeyResults IndexText<StoreType>::resultFromCache(std::string_view key, FtIdSetCache::Iterator&& it, FtCtx& ftCtx,
													   RanksHolder::Ptr& ranks) {
	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Get search results for '{}' in '{}' from cache", key, this->payloadType_ ? this->payloadType_->Name() : "");
	}
	assertrx(it.val.ctx);
	ftCtx.SetData(std::move(it.val.ctx));
	ranks = ftCtx.RanksPtr();
	return SelectKeyResult{{SingleSelectKeyResult{std::move(it.val.ids)}}};
}

template <typename StoreType>
SelectKeyResults IndexText<StoreType>::doSelectKey(std::string_view key, FtDSLQuery&& dsl, std::optional<IdSetCacheKey>&& ckey,
												   FtMergeStatuses&& mergeStatuses, FtUseExternStatuses useExternSt, bool inTransaction,
												   RankSortType rankSortType, FtCtx& ftCtx, const RdxContext& rdxCtx) {
	if (cfg_->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Searching for '{}' in '{}' {}", key, this->payloadType_ ? this->payloadType_->Name() : "",
			   ckey ? "(will cache)" : "");
	}

	IdSetPlain::Ptr mergedIds = Select(ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(mergeStatuses), useExternSt, rdxCtx);
	SelectKeyResult res;
	if (mergedIds) {
		auto ftCtxDataBase = ftCtx.GetData();
		bool need_put = (useExternSt == FtUseExternStatuses::No) && ckey.has_value();
		// count the number of Areas and determine whether the request should be cached
		if (ftCtx.Type() == FtCtxType::kFtArea && need_put && mergedIds->Size()) {
			auto ftCtxDataArea = static_ctx_pointer_cast<FtCtxAreaData<Area>>(ftCtxDataBase);

			if (cfg_ && cfg_->maxTotalAreasToCache >= 0) {
				size_t totalAreas = 0;
				assertrx_throw(ftCtxDataArea->holders.has_value());
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				for (auto& area : ftCtxDataArea->holders.value()) {
					totalAreas += ftCtxDataArea->area[area.second].GetAreasCount();
				}

				if (totalAreas > unsigned(cfg_->maxTotalAreasToCache)) {
					need_put = false;
				}
			}
			if (need_put && ftCtxDataArea->holders.has_value()) {
				// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
				for (auto& area : ftCtxDataArea->holders.value()) {
					if (auto& aData = ftCtxDataArea->area[area.second]; !aData.IsCommitted()) {
						aData.Commit();
					}
				}
			}
		}
		if (need_put && mergedIds->Size()) {
			cache_ft_.Put(*ckey, FtIdSetCacheVal{IdSetPlain::Ptr(mergedIds), std::move(ftCtxDataBase)});
		}

		res.emplace_back(std::move(mergedIds));
	}
	return SelectKeyResults(std::move(res));
}

template <typename StoreType>
SelectKeyResults IndexText<StoreType>::SelectKey(const VariantArray& keys, CondType condition, SortType /*sortId*/,
												 const Index::SelectContext& selectCtx, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());

	if (condition == CondAny && selectCtx.opts.distinct) {
		if (IsComposite(this->Type())) [[unlikely]] {
			throw Error(errParams, "Composite full text index ({}) does not support DISTINCT", Index::Name());
		}
		return ComparatorIndexed<StoreType>{Name(),		  condition,	   keys,
											nullptr,	  opts_.IsArray(), IsDistinct(selectCtx.opts.distinct),
											payloadType_, Fields(),		   opts_.collateOpts_};
	}
	if (selectCtx.opts.distinct) [[unlikely]] {
		throw Error(errParams, "Unexpected condition '{}' with DISTINCT in full text index ({})", CondTypeToStr(condition), Index::Name());
	}
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) [[unlikely]] {
		throw Error(errParams, "Full text index ({}) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}

	// Parse search query DSL here to perform strict mode validation before cache check
	FtDSLQuery dsl(this->ftFields_, cfg_->stopWords, cfg_->splitOptions, selectCtx.opts.strictMode);
	const std::string_view key = keys[0].As<p_string>();
	dsl.Parse(keys[0].As<p_string>());

	auto mergeStatuses = this->GetFtMergeStatuses(rdxCtx);
	bool needPutCache = false;
	const auto rankSortType = RankSortType(selectCtx.opts.rankSortType);
	IdSetCacheKey ckey{keys, condition, rankSortType};
	auto cache_ft = cache_ft_.Get(ckey);
	FtCtx::Ptr ftCtx = createFtCtx(selectCtx);
	if (cache_ft.valid) {
		if (!cache_ft.val.IsInitialized()) {
			needPutCache = true;
		} else if (ftCtx->Type() == FtCtxType::kFtArea && (!cache_ft.val.ctx || cache_ft.val.ctx->type != FtCtxType::kFtArea)) {
			needPutCache = true;
		} else {
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			return resultFromCache(key, std::move(cache_ft), *ftCtx, selectCtx.selectFuncCtx->ranks);
		}
	}

	return doSelectKey(key, std::move(dsl), needPutCache ? std::optional{std::move(ckey)} : std::nullopt, std::move(mergeStatuses),
					   FtUseExternStatuses::No, selectCtx.opts.inTransaction, rankSortType, *ftCtx, rdxCtx);
}

template <typename StoreType>
SelectKeyResults IndexText<StoreType>::SelectKey(const VariantArray& keys, CondType condition, const Index::SelectContext& selectCtx,
												 FtPreselectT&& preselect, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (keys.size() < 1 || (condition != CondEq && condition != CondSet)) [[unlikely]] {
		throw Error(errParams, "Full text index ({}) support only EQ or SET condition with 1 or 2 parameter", Index::Name());
	}
	// Parse search query DSL here to perform strict mode validation before cache check
	FtDSLQuery dsl(this->ftFields_, cfg_->stopWords, cfg_->splitOptions, selectCtx.opts.strictMode);
	const std::string_view key = keys[0].As<p_string>();
	dsl.Parse(keys[0].As<p_string>());

	FtCtx::Ptr ftCtx = createFtCtx(selectCtx);
	return doSelectKey(key, std::move(dsl), std::nullopt, std::move(preselect), FtUseExternStatuses::Yes, selectCtx.opts.inTransaction,
					   RankSortType(selectCtx.opts.rankSortType), *ftCtx, rdxCtx);
}

template <typename StoreType>
void IndexText<StoreType>::cleanRemovedVdocs() {
	std::vector<uint32_t> newVdocsIds_(vdocs_.size(), 0);
	size_t nextId = 1;
	for (size_t vdocId = 1; vdocId < vdocs_.size(); ++vdocId) {
		if (vdocs_[vdocId].Removed()) {
			continue;
		}

		newVdocsIds_[vdocId] = nextId;
		if (vdocId != nextId) {
			vdocs_[nextId] = std::move(vdocs_[vdocId]);
		}

		++nextId;
	}

	vdocs_.resize(nextId);
	vdocs_.shrink_to_fit();

	for (uint32_t& vdocId : rowId2Vdoc_) {
		vdocId = newVdocsIds_[vdocId];
	}

	for (uint32_t& vdocId : vdocSet_) {
		vdocId = newVdocsIds_[vdocId];
	}
}

template <typename StoreType>
void IndexText<StoreType>::commitFulltextImpl() {
	try {
		holder_->StartCommit(false);
		auto tm0 = system_clock_w::now();
		FieldsGetter gt(this->Fields(), this->payloadType_, this->KeyType());

		switch (holder_->status_) {
			case CreateNew:
				vdocsCommited_ = vdocsIndexed_;
				break;
			case RecommitLast:
				vdocsIndexed_ = vdocsCommited_;
				break;
			case FullRebuild:
				vdocsIndexed_ = 0;
				vdocsCommited_ = 0;
				cleanRemovedVdocs();
				break;
			default:
				assertrx(false);
		}

		std::vector<h_vector<std::pair<std::string_view, uint32_t>, 8>> vdocsTexts;
		std::vector<uint32_t> vdocsIds;
		vdocsTexts.reserve(vdocs_.size() - vdocsIndexed_);
		vdocsIds.reserve(vdocs_.size() - vdocsIndexed_);
		std::vector<std::unique_ptr<std::string>> bufStrs;

		for (uint32_t vdocId = vdocsIndexed_; vdocId < vdocs_.size(); ++vdocId) {
			if (vdocs_[vdocId].NumRows() == 0) {
				continue;
			}
			vdocsIds.emplace_back(vdocId);
			vdocsTexts.emplace_back(gt.getDocFields(vdocs_[vdocId].DataRef(), bufStrs));
		}

		auto tm1 = system_clock_w::now();

		std::vector<h_vector<float, 3>> wordCounts;
		holder_->Process(vdocsTexts, vdocsIds, vdocs_.size(), Fields().size(), *!this->opts_.IsDense(), wordCounts);
		size_t idx = 0;
		for (uint32_t vdocId = vdocsIndexed_; vdocId < vdocs_.size(); ++vdocId) {
			if (vdocs_[vdocId].NumRows() == 0) {
				continue;
			}
			vdocs_[vdocId].wordCounts_ = wordCounts[idx++];
		}

		// Calculate avg words count per document for bm25 calculation
		avgWordsCount_.resize(Fields().size(), 0);
		for (unsigned i = 0; i < Fields().size(); i++) {
			avgWordsCount_[i] = 0;
			size_t nonEmptyCnt = 0;
			for (auto& vdoc : vdocs_) {
				if (vdoc.NumRows() > 0) {
					avgWordsCount_[i] += vdoc.wordCounts_[i];
					++nonEmptyCnt;
				}
			}
			if (nonEmptyCnt > 0) {
				avgWordsCount_[i] /= nonEmptyCnt;
			}
		}

		vdocsIndexed_ = vdocs_.size();

		if (cfg_->logLevel >= LogInfo) [[unlikely]] {
			auto tm2 = system_clock_w::now();
			logFmt(LogInfo, "IndexText::Commit elapsed {} ms total [ build vdocs {} ms,  process data {} ms ]",
				   duration_cast<milliseconds>(tm2 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
				   duration_cast<milliseconds>(tm2 - tm1).count());
		}
	} catch (Error& e) {
		logFmt(LogError, "IndexText::Commit exception: '{}'. Index will be rebuilt on the next query", e.what());
		holder_->steps.clear();
		holder_->stepsWords_.clear();
		holder_->lastStepWords_.clear();
		throw;
	} catch (std::exception& e) {
		logFmt(LogError, "IndexText::Commit exception: '{}'. Index will be rebuilt on the next query", e.what());
		holder_->steps.clear();
		holder_->stepsWords_.clear();
		holder_->lastStepWords_.clear();
		throw;
	} catch (...) {
		logFmt(LogError, "IndexText::Commit exception: <unknown error>. Index will be rebuilt on the next query");
		holder_->steps.clear();
		holder_->stepsWords_.clear();
		holder_->lastStepWords_.clear();
		throw;
	}
}

std::unique_ptr<Index> IndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									 const NamespaceCacheConfigData& cacheCfg) {
	if (idef.IndexType() == IndexFastFT) {
		return std::make_unique<IndexText<key_string>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
	}

	if (idef.IndexType() == IndexCompositeFastFT) {
		return std::make_unique<IndexText<PayloadValue>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
	}

	throw_as_assert;
}

template class IndexText<PayloadValue>;
template class IndexText<key_string>;

}  // namespace reindexer
