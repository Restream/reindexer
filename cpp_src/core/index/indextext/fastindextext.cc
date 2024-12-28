#include "fastindextext.h"
#include <memory>
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/synonyms.h"
#include "core/ft/filters/translit.h"
#include "core/ft/ft_fast/frisosplitter.h"
#include "core/ft/ft_fast/splitter.h"
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
	holder_->translit_.reset(new Translit);
	holder_->kbLayout_.reset(new KbLayout);
	holder_->synonyms_.reset(new Synonyms);
	for (const char** lang = stemLangs; *lang; ++lang) {
		holder_->stemmers_.emplace(*lang, *lang);
	}
}

template <typename T>
Variant FastIndexText<T>::Upsert(const Variant& key, IdType id, bool& clearCache) {
	if rx_unlikely (key.Type().Is<KeyValueType::Null>()) {
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
void FastIndexText<T>::Delete(const Variant& key, IdType id, StringsHolder& strHolder, bool& clearCache) {
	if rx_unlikely (key.Type().Is<KeyValueType::Null>()) {
		this->empty_ids_.Unsorted().Erase(id);	// ignore result
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
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt, "Delete non-existent id from index '%s' id=%d,key=%s", this->name_,
			id, key.As<std::string>());

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
	ret.fulltextSize = this->holder_->GetMemStat();
	ret.idsetCache = this->cache_ft_.GetMemStat();
	return ret;
}
template <typename T>
template <typename MergeType>
typename MergeType::iterator FastIndexText<T>::unstableRemoveIf(MergeType& md, int minRelevancy, double scalingFactor, size_t& relevantDocs,
																int& cnt) {
	if (md.empty()) {
		return md.begin();
	}
	auto& holder = *this->holder_;
	auto first = md.begin();
	auto last = md.end();
	while (true) {
		while (true) {
			if (first == last) {
				return first;
			}
			first->proc *= scalingFactor;
			if (first->proc < minRelevancy) {
				break;
			}
			auto& vdoc = holder.vdocs_[first->id];
			assertrx_throw(!vdoc.keyEntry->Unsorted().empty());
			cnt += vdoc.keyEntry->Sorted(0).size();
			++relevantDocs;

			++first;
		}
		while (true) {
			--last;
			if (first == last) {
				return first;
			}
			last->proc *= scalingFactor;
			if (last->proc >= minRelevancy) {
				break;
			}
		}
		auto& vdoc = holder.vdocs_[last->id];
		assertrx_throw(!vdoc.keyEntry->Unsorted().empty());
		cnt += vdoc.keyEntry->Sorted(0).size();
		++relevantDocs;

		*first = std::move(*last);
		++first;
	}
}

template <typename T>
template <typename MergeType>
IdSet::Ptr FastIndexText<T>::afterSelect(FtCtx& fctx, MergeType&& mergeData, FtSortType ftSortType, FtMergeStatuses&& statuses,
										 FtUseExternStatuses useExternSt) {
	// convert vids(uniq documents id) to ids (real ids)
	IdSet::Ptr mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();

	auto& holder = *this->holder_;
	if (mergeData.empty()) {
		return mergedIds;
	}
	int cnt = 0;
	const double scalingFactor = mergeData.maxRank > 255 ? 255.0 / mergeData.maxRank : 1.0;
	const int minRelevancy = getConfig()->minRelevancy * 100 * scalingFactor;

	size_t relevantDocs = 0;
	switch (ftSortType) {
		case FtSortType::RankAndID: {
			auto itF = unstableRemoveIf(mergeData, minRelevancy, scalingFactor, relevantDocs, cnt);
			mergeData.erase(itF, mergeData.end());
			break;
		}
		case FtSortType::RankOnly: {
			for (auto& vid : mergeData) {
				auto& vdoc = holder.vdocs_[vid.id];
				vid.proc *= scalingFactor;
				if (vid.proc <= minRelevancy) {
					break;
				}
				assertrx_throw(!vdoc.keyEntry->Unsorted().empty());
				cnt += vdoc.keyEntry->Sorted(0).size();
				++relevantDocs;
			}
			break;
		}
		case FtSortType::ExternalExpression: {
			throw Error(errLogic, "FtSortType::ExternalExpression not implemented.");
		}
	}

	mergedIds->reserve(cnt);
	if constexpr (std::is_same_v<MergeDataBase, MergeType>) {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(mergeData, relevantDocs,
							[&fctx, &mergedIds](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
								fctx.Add(ebegin, eend, vid.proc);
								mergedIds->Append(ebegin, eend, IdSet::Unordered);
							});
		} else {
			appendMergedIds(mergeData, relevantDocs,
							[&fctx, &mergedIds, &statuses](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
								fctx.Add(ebegin, eend, vid.proc, statuses.rowIds);
								mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
							});
		}
	} else if constexpr (std::is_same_v<MergeData<Area>, MergeType> || std::is_same_v<MergeData<AreaDebug>, MergeType>) {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(mergeData, relevantDocs,
							[&fctx, &mergedIds, &mergeData](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
								fctx.Add(ebegin, eend, vid.proc, std::move(mergeData.vectorAreas[vid.areaIndex]));
								mergedIds->Append(ebegin, eend, IdSet::Unordered);
							});

		} else {
			appendMergedIds(
				mergeData, relevantDocs,
				[&fctx, &mergedIds, &statuses, &mergeData](IdSetCRef::iterator ebegin, IdSetCRef::iterator eend, const MergeInfo& vid) {
					fctx.Add(ebegin, eend, vid.proc, statuses.rowIds, std::move(mergeData.vectorAreas[vid.areaIndex]));
					mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
				});
		}
	} else {
		static_assert(!sizeof(MergeType), "incorrect MergeType");
	}

	if rx_unlikely (getConfig()->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Total merge out: %d ids", mergedIds->size());
		std::string str;
		for (size_t i = 0; i < fctx.Size();) {
			size_t j = i;
			for (; j < fctx.Size() && fctx.Proc(i) == fctx.Proc(j); j++);
			str += std::to_string(fctx.Proc(i)) + "%";
			if (j - i > 1) {
				str += "(";
				str += std::to_string(j - i);
				str += ")";
			}
			str += " ";
			i = j;
		}
		logPrintf(LogInfo, "Relevancy(%d): %s", fctx.Size(), str);
	}
	assertrx_throw(mergedIds->size() == fctx.Size());
	if (ftSortType == FtSortType::RankAndID) {
		std::vector<size_t> sortIds;
		size_t nItems = mergedIds->size();
		sortIds.reserve(mergedIds->size());
		for (size_t i = 0; i < nItems; i++) {
			sortIds.emplace_back(i);
		}
		std::vector<int16_t>& proc = fctx.GetData()->proc;
		boost::sort::pdqsort(sortIds.begin(), sortIds.end(), [&proc, mergedIds](size_t i1, size_t i2) {
			int p1 = proc[i1];
			int p2 = proc[i2];
			if (p1 > p2) {
				return true;
			} else if (p1 < p2) {
				return false;
			} else {
				return (*mergedIds)[i1] < (*mergedIds)[i2];
			}
		});

		for (size_t i = 0; i < nItems; i++) {
			auto vm = (*mergedIds)[i];
			auto vp = proc[i];
			size_t j = i;
			while (true) {
				size_t k = sortIds[j];
				sortIds[j] = j;
				if (k == i) {
					break;
				}
				(*mergedIds)[j] = (*mergedIds)[k];
				proc[j] = proc[k];
				j = k;
			}
			(*mergedIds)[j] = vm;
			proc[j] = vp;
		}
	}
	return mergedIds;
}

template <typename T>
template <typename VectorType, FtUseExternStatuses useExternalStatuses>
IdSet::Ptr FastIndexText<T>::applyCtxTypeAndSelect(DataHolder<VectorType>* d, const BaseFunctionCtx::Ptr& bctx, FtDSLQuery&& dsl,
												   bool inTransaction, FtSortType ftSortType, FtMergeStatuses&& statuses,
												   FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	Selector<VectorType> selector{*d, this->Fields().size(), holder_->cfg_->maxAreasInDoc};
	intrusive_ptr<FtCtx> fctx = static_ctx_pointer_cast<FtCtx>(bctx);
	assertrx_throw(fctx);
	fctx->SetWordPosition(true);
	fctx->SetSplitter(this->holder_->GetSplitter());

	switch (bctx->type) {
		case BaseFunctionCtx::CtxType::kFtCtx: {
			MergeDataBase mergeData = selector.template Process<useExternalStatuses, MergeDataBase>(
				std::move(dsl), inTransaction, ftSortType, std::move(statuses.statuses), rdxCtx);
			return afterSelect(*fctx.get(), std::move(mergeData), ftSortType, std::move(statuses), useExternSt);
		}
		case BaseFunctionCtx::CtxType::kFtArea: {
			MergeData<Area> mergeData = selector.template Process<useExternalStatuses, MergeData<Area>>(
				std::move(dsl), inTransaction, ftSortType, std::move(statuses.statuses), rdxCtx);
			return afterSelect(*fctx.get(), std::move(mergeData), ftSortType, std::move(statuses), useExternSt);
		}
		case BaseFunctionCtx::CtxType::kFtAreaDebug: {
			MergeData<AreaDebug> mergeData = selector.template Process<useExternalStatuses, MergeData<AreaDebug>>(
				std::move(dsl), inTransaction, ftSortType, std::move(statuses.statuses), rdxCtx);
			return afterSelect(*fctx.get(), std::move(mergeData), ftSortType, std::move(statuses), useExternSt);
		}
		default:
			throw_assert(false);
	}
}

template <typename T>
template <typename VectorType>
IdSet::Ptr FastIndexText<T>::applyOptimizationAndSelect(DataHolder<VectorType>* d, BaseFunctionCtx::Ptr bctx, FtDSLQuery&& dsl,
														bool inTransaction, FtSortType ftSortType, FtMergeStatuses&& statuses,
														FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	if (useExternSt == FtUseExternStatuses::Yes) {
		return applyCtxTypeAndSelect<VectorType, FtUseExternStatuses::Yes>(d, std::move(bctx), std::move(dsl), inTransaction, ftSortType,
																		   std::move(statuses), useExternSt, rdxCtx);
	} else {
		return applyCtxTypeAndSelect<VectorType, FtUseExternStatuses::No>(d, std::move(bctx), std::move(dsl), inTransaction, ftSortType,
																		  std::move(statuses), useExternSt, rdxCtx);
	}
}

template <typename T>
IdSet::Ptr FastIndexText<T>::Select(FtCtx::Ptr bctx, FtDSLQuery&& dsl, bool inTransaction, FtSortType ftSortType,
									FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt, const RdxContext& rdxCtx) {
	switch (holder_->cfg_->optimization) {
		case FtFastConfig::Optimization::Memory: {
			DataHolder<PackedIdRelVec>* d = dynamic_cast<DataHolder<PackedIdRelVec>*>(holder_.get());
			assertrx_throw(d);

			return applyOptimizationAndSelect<PackedIdRelVec>(d, bctx, std::move(dsl), inTransaction, ftSortType, std::move(statuses),
															  useExternSt, rdxCtx);
		}
		case FtFastConfig::Optimization::CPU: {
			DataHolder<IdRelVec>* d = dynamic_cast<DataHolder<IdRelVec>*>(holder_.get());
			assertrx_throw(d);

			return applyOptimizationAndSelect<IdRelVec>(d, bctx, std::move(dsl), inTransaction, ftSortType, std::move(statuses),
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

		this->holder_->Process(this->Fields().size(), !this->opts_.IsDense());
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
		if rx_unlikely (getConfig()->logLevel >= LogInfo) {
			auto tm2 = system_clock_w::now();
			logPrintf(LogInfo, "FastIndexText::Commit elapsed %d ms total [ build vdocs %d ms,  process data %d ms ]",
					  duration_cast<milliseconds>(tm2 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
					  duration_cast<milliseconds>(tm2 - tm1).count());
		}
	} catch (Error& e) {
		logPrintf(LogError, "FastIndexText::Commit exception: '%s'. Index will be rebuilt on the next query", e.what());
		this->holder_->steps.clear();
		throw;
	} catch (std::exception& e) {
		logPrintf(LogError, "FastIndexText::Commit exception: '%s'. Index will be rebuilt on the next query", e.what());
		this->holder_->steps.clear();
		throw;
	} catch (...) {
		logPrintf(LogError, "FastIndexText::Commit exception: <unknown error>. Index will be rebuilt on the next query");
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
			assertrx(it != data.end());
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

		if rx_unlikely (getConfig()->logLevel <= LogInfo) {
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
RX_ALWAYS_INLINE void FastIndexText<T>::appendMergedIds(MergeType& mergeData, size_t relevantDocs, F&& appender) {
	auto& holder = *this->holder_;
	for (size_t i = 0; i < relevantDocs; i++) {
		auto& vid = mergeData[i];
		auto& vdoc = holder.vdocs_[vid.id];
		appender(vdoc.keyEntry->Sorted(0).begin(), vdoc.keyEntry->Sorted(0).end(), vid);
	}
}

template <typename T>
void FastIndexText<T>::initConfig(const FtFastConfig* cfg) {
	if (cfg) {
		this->cfg_.reset(new FtFastConfig(*cfg));
	} else {
		this->cfg_.reset(new FtFastConfig(this->ftFields_.size()));
		this->cfg_->parse(this->opts_.config, this->ftFields_);
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
		oldCfg.enableNumbersSearch != newCfg.enableNumbersSearch || oldCfg.extraWordSymbols != newCfg.extraWordSymbols ||
		oldCfg.synonyms != newCfg.synonyms || oldCfg.maxTypos != newCfg.maxTypos || oldCfg.optimization != newCfg.optimization ||
		oldCfg.splitterType != newCfg.splitterType) {
		logPrintf(LogInfo, "FulltextIndex config changed, it will be rebuilt on next search");
		this->isBuilt_ = false;
		if (oldCfg.optimization != newCfg.optimization || oldCfg.splitterType != newCfg.splitterType ||
			oldCfg.extraWordSymbols != newCfg.extraWordSymbols) {
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
		logPrintf(LogInfo, "FulltextIndex config changed, cache cleared");
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
	switch (idef.Type()) {
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
			break;
	}
	std::abort();
}

}  // namespace reindexer
