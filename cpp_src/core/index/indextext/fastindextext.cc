#include "fastindextext.h"
#include <memory>
#include "core/ft/filters/compositewordssplitter.h"
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/synonyms.h"
#include "core/ft/filters/translit.h"
#include "core/ft/ft_fast/selecter.h"
#include "core/nsselecter/ranks_holder.h"
#include "core/rdxcontext.h"
#include "estl/contexted_locks.h"
#include "sort/pdqsort.hpp"
#include "tools/clock.h"
#include "tools/logger.h"

namespace {
// Available stemmers for languages
const char* stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};
}  // namespace

namespace reindexer {

using std::chrono::duration_cast;
using std::chrono::milliseconds;

template <typename T>
void FastIndexText<T>::initHolder(FtFastConfig& cfg) {
	switch (cfg.optimization) {
		case FtFastConfig::Optimization::Memory:
			holder_ = std::make_unique<DataHolder<PackedIdRelVec>>(&cfg);
			break;
		case FtFastConfig::Optimization::CPU:
			holder_ = std::make_unique<DataHolder<IdRelVec>>(&cfg);
			break;
		default:
			assertrx(0);
	}
	holder_->stemmers_.clear();
	holder_->translit_ = std::make_unique<Translit>();
	holder_->kbLayout_ = std::make_unique<KbLayout>();
	holder_->compositeWordsSplitter_ = std::make_unique<CompositeWordsSplitter>(cfg.splitOptions);
	holder_->synonyms_ = std::make_unique<Synonyms>();
	for (const char** lang = stemLangs; *lang; ++lang) {
		holder_->stemmers_.emplace(*lang, *lang);
	}
}

template <typename T>
Variant FastIndexText<T>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	if (key.Type().Is<KeyValueType::Null>()) [[unlikely]] {
		if (this->empty_ids_.Unsorted().Add(id, IdSet::Auto, 0)) {
			this->isBuilt_ = false;
		}
		// Return invalid ref
		return Variant();
	}

	auto keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({static_cast<key_type>(key), typename T::mapped_type()}).first;
		this->tracker_.markUpdated(this->idx_map, keyIt, false);
	} else {
		this->delMemStat(keyIt);
	}
	if (keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto, 0)) {
		this->isBuilt_ = false;
		this->cache_ft_.Clear();
		clearCache = true;
	}
	this->addMemStat(keyIt);

	return Variant{keyIt->first};
}

template <typename T>
void FastIndexText<T>::Delete(const Variant& key, IdType id, [[maybe_unused]] MustExist mustExist, StringsHolder& strHolder,
							  bool& clearCache) {
	if (key.Type().Is<KeyValueType::Null>()) [[unlikely]] {
		std::ignore = this->empty_ids_.Unsorted().Erase(id);
		this->isBuilt_ = false;
		return;
	}

	auto keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == this->idx_map.end()) {
		return;
	}
	this->isBuilt_ = false;

	this->delMemStat(keyIt);
	int delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt || !mustExist,
			"Delete non-existent id from index '{}' id={}, key={}", this->name_, id, key.As<std::string>());

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		if (keyIt->second.VDocID() != FtKeyEntryData::ndoc) {
			assertrx(keyIt->second.VDocID() < int(this->holder_->vdocs_.size()));
			this->holder_->vdocs_[keyIt->second.VDocID()].keyEntry = nullptr;
		}
		if constexpr (is_str_map_v<T>) {
			this->idx_map.template erase<StringMapEntryCleaner<false>>(keyIt,
																	   {strHolder, this->KeyType().template Is<KeyValueType::String>()});
		} else {
			static_assert(is_payload_map_v<T>);
			this->idx_map.template erase<no_deep_clean>(keyIt, strHolder);
		}
	} else {
		this->addMemStat(keyIt);
	}
	this->cache_ft_.Clear();
	clearCache = true;
}

template <typename T>
IndexMemStat FastIndexText<T>::GetMemStat(const RdxContext& ctx) {
	auto ret = IndexUnordered<T>::GetMemStat(ctx);

	contexted_shared_lock lck(this->mtx_, ctx);
	ret.indexingStructSize = this->holder_->GetMemStat();
	ret.idsetCache = this->cache_ft_.GetMemStat();
	ret.isBuilt = this->isBuilt_;
	return ret;
}

static bool lessRank(RankT lhs, RankT rhs) noexcept { return lhs < rhs; }
static bool lessRank(RanksHolder::RankPos lhs, RanksHolder::RankPos rhs) noexcept { return lhs.rank < rhs.rank; }

template <typename T>
template <auto(RanksHolder::*rankGetter)>
void FastIndexText<T>::sortAfterSelect(IdSet& mergedIds, RanksHolder& ranks, RankSortType rankSortType) {
	std::vector<size_t> sortIds;
	const size_t nItems = mergedIds.size();
	sortIds.reserve(nItems);
	for (size_t i = 0; i < nItems; ++i) {
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
	for (size_t i = 0; i < nItems; i++) {
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

template <typename T>
template <typename MergeType>
IdSet::Ptr FastIndexText<T>::afterSelect(FtCtx& ftCtx, MergeType&& mergeData, RankSortType rankSortType, FtMergeStatuses&& statuses,
										 FtUseExternStatuses useExternSt) {
	// convert vids(uniq documents id) to ids (real ids)
	IdSet::Ptr mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();

	auto& holder = *this->holder_;
	if (mergeData.empty()) {
		return mergedIds;
	}
	int cnt = 0;
	size_t relevantDocs = 0;

	switch (rankSortType) {
		case RankSortType::RankAndID:
		case RankSortType::IDOnly:
		case RankSortType::IDAndPositions:
		case RankSortType::RankOnly:
			for (auto& vid : mergeData) {
				auto& vdoc = holder.vdocs_[vid.id];
				assertrx_throw(!vdoc.keyEntry->Unsorted().empty());
				cnt += vdoc.keyEntry->Sorted(0).size();
				++relevantDocs;
			}
			break;
		case RankSortType::ExternalExpression:
			throw Error(errLogic, "RankSortType::ExternalExpression not implemented.");
		default:
			throw_as_assert;
	}

	std::optional<fast_hash_set<IdType>> uniqueIds;
	bool needToDeduplicate = Index::opts_.IsArray() == IsArray_True;
	if (needToDeduplicate) {
		uniqueIds.emplace();
	}
	std::vector<IdType> idsFiltered;

	mergedIds->reserve(cnt);
	if constexpr (std::is_same_v<MergeData, MergeType>) {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(mergeData, relevantDocs, uniqueIds, idsFiltered,
							[&ftCtx, &mergedIds](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
								ftCtx.Add(ebegin, eend, RankT(vid.proc));
								mergedIds->Append(ebegin, eend, IdSet::Unordered);
							});
		} else {
			appendMergedIds(mergeData, relevantDocs, uniqueIds, idsFiltered,
							[&ftCtx, &mergedIds, &statuses](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
								ftCtx.Add(ebegin, eend, RankT(vid.proc), statuses.rowIds);
								mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
							});
		}
	} else if constexpr (std::is_same_v<MergeDataAreas<Area>, MergeType> || std::is_same_v<MergeDataAreas<AreaDebug>, MergeType>) {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(
				mergeData, relevantDocs, uniqueIds, idsFiltered,
				[&ftCtx, &mergedIds, &mergeData](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfoAreas& vid) {
					ftCtx.Add(ebegin, eend, RankT(vid.proc), std::move(mergeData.vectorAreas[vid.areaIndex]));
					mergedIds->Append(ebegin, eend, IdSet::Unordered);
				});

		} else {
			appendMergedIds(mergeData, relevantDocs, uniqueIds, idsFiltered,
							[&ftCtx, &mergedIds, &statuses, &mergeData](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend,
																		const MergeInfoAreas& vid) {
								ftCtx.Add(ebegin, eend, RankT(vid.proc), statuses.rowIds, std::move(mergeData.vectorAreas[vid.areaIndex]));
								mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
							});
		}
	} else {
		static_assert(!sizeof(MergeType), "incorrect MergeType");
	}

	auto& ranks = ftCtx.Ranks();
	if (getConfig()->logLevel >= LogInfo) [[unlikely]] {
		logFmt(LogInfo, "Total merge out: {} ids", mergedIds->size());
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
	assertrx_throw(mergedIds->size() == ranks.Size());
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
		default:
			throw_as_assert;
	}
	return mergedIds;
}

template <typename T>
template <typename VectorType, FtUseExternStatuses useExternalStatuses>
IdSet::Ptr FastIndexText<T>::applyCtxTypeAndSelect(DataHolder<VectorType>* d, FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction,
												   RankSortType rankSortType, FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt,
												   const RdxContext& rdxCtx) {
	Selector<VectorType> selector{*d, this->Fields().size(), holder_->cfg_->maxAreasInDoc};
	ftCtx.SetWordPosition(true);
	ftCtx.SetSplitter(this->holder_->GetSplitter());

	switch (ftCtx.Type()) {
		case FtCtxType::kFtCtx: {
			MergeData mergeData = selector.template Process<useExternalStatuses, MergeData>(std::move(dsl), inTransaction, rankSortType,
																							std::move(statuses.statuses), rdxCtx);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kFtArea: {
			MergeDataAreas<Area> mergeData = selector.template Process<useExternalStatuses, MergeDataAreas<Area>>(
				std::move(dsl), inTransaction, rankSortType, std::move(statuses.statuses), rdxCtx);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kFtAreaDebug: {
			MergeDataAreas<AreaDebug> mergeData = selector.template Process<useExternalStatuses, MergeDataAreas<AreaDebug>>(
				std::move(dsl), inTransaction, rankSortType, std::move(statuses.statuses), rdxCtx);
			return afterSelect(ftCtx, std::move(mergeData), rankSortType, std::move(statuses), useExternSt);
		}
		case FtCtxType::kNotSet:
		default:
			throw_assert(false);
	}
}

template <typename T>
template <typename VectorType>
IdSet::Ptr FastIndexText<T>::applyOptimizationAndSelect(DataHolder<VectorType>* d, FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction,
														RankSortType rankSortType, FtMergeStatuses&& statuses,
														FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	if (useExternSt == FtUseExternStatuses::Yes) {
		return applyCtxTypeAndSelect<VectorType, FtUseExternStatuses::Yes>(d, ftCtx, std::move(dsl), inTransaction, rankSortType,
																		   std::move(statuses), useExternSt, rdxCtx);
	} else {
		return applyCtxTypeAndSelect<VectorType, FtUseExternStatuses::No>(d, ftCtx, std::move(dsl), inTransaction, rankSortType,
																		  std::move(statuses), useExternSt, rdxCtx);
	}
}

template <typename T>
IdSet::Ptr FastIndexText<T>::Select(FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction, RankSortType rankSortType,
									FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	switch (holder_->cfg_->optimization) {
		case FtFastConfig::Optimization::Memory: {
			DataHolder<PackedIdRelVec>* d = dynamic_cast<DataHolder<PackedIdRelVec>*>(holder_.get());
			assertrx_throw(d);
			return applyOptimizationAndSelect<PackedIdRelVec>(d, ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(statuses),
															  useExternSt, rdxCtx);
		}
		case FtFastConfig::Optimization::CPU: {
			DataHolder<IdRelVec>* d = dynamic_cast<DataHolder<IdRelVec>*>(holder_.get());
			assertrx_throw(d);
			return applyOptimizationAndSelect<IdRelVec>(d, ftCtx, std::move(dsl), inTransaction, rankSortType, std::move(statuses),
														useExternSt, rdxCtx);
		}
		default:
			throw_assert(false);
	}
}

template <typename T>
void FastIndexText<T>::commitFulltextImpl() {
	try {
		this->holder_->StartCommit(this->tracker_.isCompleteUpdated());

		auto tm0 = system_clock_w::now();

		if (this->holder_->status_ == FullRebuild) {
			buildVdocs(this->idx_map);
		} else {
			buildVdocs(this->tracker_.updated());
		}
		auto tm1 = system_clock_w::now();

		this->holder_->Process(this->Fields().size(), *!this->opts_.IsDense());
		if (this->holder_->NeedClear(this->tracker_.isCompleteUpdated())) {
			this->tracker_.clear();
		}
		this->holder_->rowId2Vdoc_.clear();
		this->holder_->rowId2Vdoc_.reserve(this->holder_->vdocs_.size());
		for (size_t i = 0, s = this->holder_->vdocs_.size(); i < s; ++i) {
			const auto& vdoc = this->holder_->vdocs_[i];
			if (vdoc.keyEntry) {
				for (const auto id : vdoc.keyEntry->Unsorted()) {
					if (static_cast<size_t>(id) >= this->holder_->rowId2Vdoc_.size()) {
						this->holder_->rowId2Vdoc_.resize(id + 1, FtMergeStatuses::kEmpty);
					}
					this->holder_->rowId2Vdoc_[id] = i;
				}
			}
		}
		if (getConfig()->logLevel >= LogInfo) [[unlikely]] {
			auto tm2 = system_clock_w::now();
			logFmt(LogInfo, "FastIndexText::Commit elapsed {} ms total [ build vdocs {} ms,  process data {} ms ]",
				   duration_cast<milliseconds>(tm2 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
				   duration_cast<milliseconds>(tm2 - tm1).count());
		}
	} catch (Error& e) {
		logFmt(LogError, "FastIndexText::Commit exception: '{}'. Index will be rebuilt on the next query", e.what());
		this->holder_->steps.clear();
		throw;
	} catch (std::exception& e) {
		logFmt(LogError, "FastIndexText::Commit exception: '{}'. Index will be rebuilt on the next query", e.what());
		this->holder_->steps.clear();
		throw;
	} catch (...) {
		logFmt(LogError, "FastIndexText::Commit exception: <unknown error>. Index will be rebuilt on the next query");
		this->holder_->steps.clear();
		throw;
	}
}

template <typename T>
template <class Container>
void FastIndexText<T>::buildVdocs(Container& data) {
	// buffer strings, for printing non text fields
	auto& bufStrs = this->holder_->bufStrs_;
	// array with pointers to docs fields text
	// Prepare vdocs -> addressable array all docs in the index

	this->holder_->szCnt = 0;
	auto& vdocs = this->holder_->vdocs_;
	auto& vdocsTexts = this->holder_->vdocsTexts;

	vdocs.reserve(vdocs.size() + data.size());
	vdocsTexts.reserve(data.size());

	auto gt = this->Getter();

	auto status = this->holder_->status_;

	if (status == CreateNew) {
		this->holder_->cur_vdoc_pos_ = vdocs.size();
	} else if (status == RecommitLast) {
		vdocs.erase(vdocs.begin() + this->holder_->cur_vdoc_pos_, vdocs.end());
	}
	this->holder_->vdocsOffset_ = vdocs.size();

	typename T::iterator doc;
	for (auto it = data.begin(); it != data.end(); ++it) {
		if constexpr (std::is_same<Container, typename UpdateTracker<T>::hash_map>()) {
			doc = this->idx_map.find(*it);
			assertrx(doc != this->idx_map.end());
		} else {
			doc = it;
		}
		vdocsTexts.emplace_back(gt.getDocFields(doc->first, bufStrs));
#ifdef REINDEX_FT_EXTRA_DEBUG
		std::string text(vdocsTexts.back()[0].first);
		vdocs.push_back({(text.length() > 48) ? text.substr(0, 48) + "..." : text, doc->second.get(), {}, {}});
#else
		vdocs.push_back({doc->second.get(), {}, {}});
#endif
		// Set VDocID after actual doc emplacing
		doc->second.SetVDocID(vdocs.size() - 1);

		if (getConfig()->logLevel <= LogInfo) [[unlikely]] {
			for (auto& f : vdocsTexts.back()) {
				this->holder_->szCnt += f.first.length();
			}
		}
	}
	if (status == FullRebuild) {
		this->holder_->cur_vdoc_pos_ = vdocs.size();
	}
}

template <typename T>
template <typename MergeType, typename F>
RX_ALWAYS_INLINE void FastIndexText<T>::appendMergedIds(MergeType& mergeData, size_t relevantDocs,
														std::optional<fast_hash_set<IdType>>& uniqueIds, std::vector<IdType>& idsFiltered,
														F&& appender) {
	auto& holder = *this->holder_;
	for (size_t i = 0; i < relevantDocs; i++) {
		auto& vid = mergeData[i];
		auto& vdoc = holder.vdocs_[vid.id];

		const auto& realIds = vdoc.keyEntry->Sorted(0);
		if (uniqueIds.has_value()) {
			idsFiltered.resize(0);
			for (IdType id : realIds) {
				if (uniqueIds->insert(id).second) {
					idsFiltered.push_back(id);
				}
			}

			if (!idsFiltered.empty()) {
				const std::span<const IdType> sp(idsFiltered);
				appender(sp.begin(), sp.end(), vid);
			}
		} else {
			appender(realIds.begin(), realIds.end(), vid);
		}
	}
}

template <typename T>
void FastIndexText<T>::initConfig(const FtFastConfig* cfg) {
	if (cfg) {
		this->cfg_ = std::make_unique<FtFastConfig>(*cfg);
	} else {
		this->cfg_ = std::make_unique<FtFastConfig>(this->ftFields_.size());
		this->cfg_->parse(this->opts_.Config(), this->ftFields_);
	}
	initHolder(*getConfig());  // -V522
	this->holder_->synonyms_->SetConfig(this->cfg_.get());
}

template <typename Container>
bool eq_c(Container& c1, Container& c2) {
	return c1.size() == c2.size() && std::equal(c1.begin(), c1.end(), c2.begin());
}

template <typename T>
void FastIndexText<T>::SetOpts(const IndexOpts& opts) {
	auto oldCfg = *getConfig();
	IndexText<T>::SetOpts(opts);
	auto& newCfg = *getConfig();

	if (!eq_c(oldCfg.stopWords, newCfg.stopWords) || oldCfg.stemmers != newCfg.stemmers || oldCfg.maxTypoLen != newCfg.maxTypoLen ||
		oldCfg.enableNumbersSearch != newCfg.enableNumbersSearch || oldCfg.splitOptions != newCfg.splitOptions ||
		oldCfg.synonyms != newCfg.synonyms || oldCfg.maxTypos != newCfg.maxTypos || oldCfg.optimization != newCfg.optimization ||
		oldCfg.splitterType != newCfg.splitterType) {
		logFmt(LogInfo, "FulltextIndex config changed, it will be rebuilt on next search");
		this->isBuilt_ = false;
		if (oldCfg.optimization != newCfg.optimization || oldCfg.splitterType != newCfg.splitterType ||
			oldCfg.splitOptions != newCfg.splitOptions) {
			initHolder(newCfg);
		} else {
			this->holder_->Clear();
		}
		this->holder_->status_ = FullRebuild;
		this->cache_ft_.Clear();
		for (auto& idx : this->idx_map) {
			idx.second.SetVDocID(FtKeyEntryData::ndoc);
		}
	} else {
		logFmt(LogInfo, "FulltextIndex config changed, cache cleared");
		this->cache_ft_.Clear();
	}
	this->holder_->synonyms_->SetConfig(&newCfg);
}

template <typename T>
reindexer::FtPreselectT FastIndexText<T>::FtPreselect(const RdxContext& rdxCtx) {
	this->build(rdxCtx);
	return FtMergeStatuses{FtMergeStatuses::Statuses(holder_->vdocs_.size(), FtMergeStatuses::kExcluded),
						   std::vector<bool>(holder_->rowId2Vdoc_.size(), false), &holder_->rowId2Vdoc_};
}

std::unique_ptr<Index> FastIndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										 const NamespaceCacheConfigData& cacheCfg) {
	switch (idef.IndexType()) {
		case IndexFastFT:
			return std::make_unique<FastIndexText<unordered_str_map<FtKeyEntry>>>(idef, std::move(payloadType), std::move(fields),
																				  cacheCfg);
		case IndexCompositeFastFT:
			return std::make_unique<FastIndexText<unordered_payload_map<FtKeyEntry, true>>>(idef, std::move(payloadType), std::move(fields),
																							cacheCfg);
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFuzzyFT:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
		case IndexCompositeFuzzyFT:
		case IndexTtl:
		case IndexRTree:
		case IndexUuidHash:
		case IndexUuidStore:
		case IndexHnsw:
		case IndexVectorBruteforce:
		case IndexIvf:
		case IndexDummy:
			break;
	}
	throw_as_assert;
}

}  // namespace reindexer
