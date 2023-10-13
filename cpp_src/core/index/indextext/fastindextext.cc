#include "fastindextext.h"
#include <chrono>
#include <memory>
#include "core/ft/bm25.h"
#include "core/ft/filters/kblayout.h"
#include "core/ft/filters/synonyms.h"
#include "core/ft/filters/translit.h"
#include "core/ft/ft_fast/selecter.h"
#include "core/ft/numtotext.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/contexted_locks.h"
#include "tools/logger.h"

namespace {
// Available stemmers for languages
const char *stemLangs[] = {"en", "ru", "nl", "fin", "de", "da", "fr", "it", "hu", "no", "pt", "ro", "es", "sv", "tr", nullptr};
}  // namespace

namespace reindexer {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::high_resolution_clock;

template <typename T>
void FastIndexText<T>::initHolder(FtFastConfig &cfg) {
	switch (cfg.optimization) {
		case FtFastConfig::Optimization::Memory:
			holder_.reset(new DataHolder<PackedIdRelVec>);
			break;
		case FtFastConfig::Optimization::CPU:
			holder_.reset(new DataHolder<IdRelVec>);
			break;
		default:
			assertrx(0);
	}
	holder_->stemmers_.clear();
	holder_->translit_.reset(new Translit);
	holder_->kbLayout_.reset(new KbLayout);
	holder_->synonyms_.reset(new Synonyms);
	for (const char **lang = stemLangs; *lang; ++lang) {
		holder_->stemmers_.emplace(*lang, *lang);
	}
	holder_->SetConfig(&cfg);
}

template <typename T>
Variant FastIndexText<T>::Upsert(const Variant &key, IdType id, bool &clearCache) {
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
		if (this->cache_ft_) this->cache_ft_->Clear();
		clearCache = true;
	}
	this->addMemStat(keyIt);

	if (this->KeyType().template Is<KeyValueType::String>() && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<StoreIndexKeyType<T>>::Upsert(key, id, clearCache);
	}

	return Variant(keyIt->first);
}

template <typename T>
void FastIndexText<T>::Delete(const Variant &key, IdType id, StringsHolder &strHolder, bool &clearCache) {
	int delcnt = 0;
	if rx_unlikely (key.Type().Is<KeyValueType::Null>()) {
		delcnt = this->empty_ids_.Unsorted().Erase(id);
		assertrx(delcnt);
		this->isBuilt_ = false;
		return;
	}

	auto keyIt = this->idx_map.find(static_cast<ref_type>(key));
	if (keyIt == this->idx_map.end()) return;
	this->isBuilt_ = false;

	this->delMemStat(keyIt);
	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt, "Delete unexists id from index '%s' id=%d,key=%s", this->name_, id,
			key.As<std::string>());

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(keyIt);
		if (keyIt->second.VDocID() != FtKeyEntryData::ndoc) {
			assertrx(keyIt->second.VDocID() < int(this->holder_->vdocs_.size()));
			this->holder_->vdocs_[keyIt->second.VDocID()].keyEntry = nullptr;
		}
		if constexpr (is_str_map_v<T>) {
			this->idx_map.template erase<StringMapEntryCleaner<false>>(
				keyIt, {strHolder, this->KeyType().template Is<KeyValueType::String>() && this->opts_.GetCollateMode() == CollateNone});
		} else {
			static_assert(is_payload_map_v<T>);
			this->idx_map.template erase<no_deep_clean>(keyIt, strHolder);
		}
	} else {
		this->addMemStat(keyIt);
	}
	if (this->KeyType().template Is<KeyValueType::String>() && this->opts_.GetCollateMode() != CollateNone) {
		IndexStore<StoreIndexKeyType<T>>::Delete(key, id, strHolder, clearCache);
	}
	if (this->cache_ft_) this->cache_ft_->Clear();
	clearCache = true;
}

template <typename T>
IndexMemStat FastIndexText<T>::GetMemStat(const RdxContext &ctx) {
	auto ret = IndexUnordered<T>::GetMemStat(ctx);

	contexted_shared_lock lck(this->mtx_, &ctx);
	ret.fulltextSize = this->holder_->GetMemStat();
	if (this->cache_ft_) ret.idsetCache = this->cache_ft_->GetMemStat();
	return ret;
}

template <typename T>
IdSet::Ptr FastIndexText<T>::Select(FtCtx::Ptr fctx, FtDSLQuery &&dsl, bool inTransaction, FtMergeStatuses &&statuses,
									FtUseExternStatuses useExternSt, const RdxContext &rdxCtx) {
	fctx->GetData()->extraWordSymbols_ = this->getConfig()->extraWordSymbols;
	fctx->GetData()->isWordPositions_ = true;

	auto mergeData = this->holder_->Select(std::move(dsl), this->fields_.size(), fctx->NeedArea(), getConfig()->maxAreasInDoc,
										   inTransaction, std::move(statuses.statuses), useExternSt, rdxCtx);
	// convert vids(uniq documents id) to ids (real ids)
	IdSet::Ptr mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();
	auto &holder = *this->holder_;

	if (mergeData.empty()) {
		return mergedIds;
	}
	int cnt = 0;
	const double scalingFactor = mergeData.maxRank > 255 ? 255.0 / mergeData.maxRank : 1.0;
	const int minRelevancy = getConfig()->minRelevancy * 100 * scalingFactor;
	size_t releventDocs = 0;
	for (auto &vid : mergeData) {
		auto &vdoc = holder.vdocs_[vid.id];
		if (!vdoc.keyEntry) {
			continue;
		}
		vid.proc *= scalingFactor;
		if (vid.proc <= minRelevancy) break;

		assertrx_throw(!vdoc.keyEntry->Unsorted().empty());
		cnt += vdoc.keyEntry->Sorted(0).size();
		++releventDocs;
	}

	mergedIds->reserve(cnt);
	fctx->Reserve(cnt);
	if (!fctx->NeedArea()) {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(mergeData, releventDocs,
							[&fctx, &mergedIds](IdSetRef::iterator ebegin, IdSetRef::iterator eend, const IDataHolder::MergeInfo &vid) {
								fctx->Add(ebegin, eend, vid.proc);
								mergedIds->Append(ebegin, eend, IdSet::Unordered);
							});
		} else {
			appendMergedIds(
				mergeData, releventDocs,
				[&fctx, &mergedIds, &statuses](IdSetRef::iterator ebegin, IdSetRef::iterator eend, const IDataHolder::MergeInfo &vid) {
					fctx->Add(ebegin, eend, vid.proc, statuses.rowIds);
					mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
				});
		}
	} else {
		if (useExternSt == FtUseExternStatuses::No) {
			appendMergedIds(
				mergeData, releventDocs,
				[&fctx, &mergedIds, &mergeData](IdSetRef::iterator ebegin, IdSetRef::iterator eend, const IDataHolder::MergeInfo &vid) {
					assertrx_throw(vid.areaIndex != std::numeric_limits<uint32_t>::max());
					fctx->Add(ebegin, eend, vid.proc, std::move(mergeData.vectorAreas[vid.areaIndex]));
					mergedIds->Append(ebegin, eend, IdSet::Unordered);
				});
		} else {
			appendMergedIds(mergeData, releventDocs,
							[&fctx, &mergedIds, &mergeData, &statuses](IdSetRef::iterator ebegin, IdSetRef::iterator eend,
																	   const IDataHolder::MergeInfo &vid) {
								assertrx_throw(vid.areaIndex != std::numeric_limits<uint32_t>::max());
								fctx->Add(ebegin, eend, vid.proc, statuses.rowIds, std::move(mergeData.vectorAreas[vid.areaIndex]));
								mergedIds->Append(ebegin, eend, statuses.rowIds, IdSet::Unordered);
							});
		}
	}
	if rx_unlikely (getConfig()->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Total merge out: %d ids", mergedIds->size());

		std::string str;
		for (size_t i = 0; i < fctx->GetSize();) {
			size_t j = i;
			for (; j < fctx->GetSize() && fctx->Proc(i) == fctx->Proc(j); j++)
				;
			str += std::to_string(fctx->Proc(i)) + "%";
			if (j - i > 1) {
				str += "(";
				str += std::to_string(j - i);
				str += ")";
			}
			str += " ";
			i = j;
		}
		logPrintf(LogInfo, "Relevancy(%d): %s", fctx->GetSize(), str);
	}
	assertrx_throw(mergedIds->size() == fctx->GetSize());
	return mergedIds;
}
template <typename T>
void FastIndexText<T>::commitFulltextImpl() {
	this->holder_->StartCommit(this->tracker_.isCompleteUpdated());

	auto tm0 = high_resolution_clock::now();

	if (this->holder_->status_ == FullRebuild) {
		buildVdocs(this->idx_map);
	} else {
		buildVdocs(this->tracker_.updated());
	}
	auto tm1 = high_resolution_clock::now();

	this->holder_->Process(this->fields_.size(), !this->opts_.IsDense());
	if (this->holder_->NeedClear(this->tracker_.isCompleteUpdated())) {
		this->tracker_.clear();
	}
	this->holder_->rowId2Vdoc_.clear();
	this->holder_->rowId2Vdoc_.reserve(this->holder_->vdocs_.size());
	for (size_t i = 0, s = this->holder_->vdocs_.size(); i < s; ++i) {
		const auto &vdoc = this->holder_->vdocs_[i];
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
		auto tm2 = high_resolution_clock::now();
		logPrintf(LogInfo, "FastIndexText::Commit elapsed %d ms total [ build vdocs %d ms,  process data %d ms ]",
				  duration_cast<milliseconds>(tm2 - tm0).count(), duration_cast<milliseconds>(tm1 - tm0).count(),
				  duration_cast<milliseconds>(tm2 - tm1).count());
	}
}

template <typename T>
template <class Container>
void FastIndexText<T>::buildVdocs(Container &data) {
	// buffer strings, for printing non text fields
	auto &bufStrs = this->holder_->bufStrs_;
	// array with pointers to docs fields text
	// Prepare vdocs -> addresable array all docs in the index

	this->holder_->szCnt = 0;
	auto &vdocs = this->holder_->vdocs_;
	auto &vdocsTexts = this->holder_->vdocsTexts;

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
		doc->second.SetVDocID(vdocs.size());
		vdocsTexts.emplace_back(gt.getDocFields(doc->first, bufStrs));

#ifdef REINDEX_FT_EXTRA_DEBUG
		std::string text(vdocsTexts.back()[0].first);
		vdocs.push_back({(text.length() > 48) ? text.substr(0, 48) + "..." : text, doc->second.get(), {}, {}});
#else
		vdocs.push_back({doc->second.get(), {}, {}});
#endif

		if rx_unlikely (getConfig()->logLevel <= LogInfo) {
			for (auto &f : vdocsTexts.back()) this->holder_->szCnt += f.first.length();
		}
	}
	if (status == FullRebuild) {
		this->holder_->cur_vdoc_pos_ = vdocs.size();
	}
}

template <typename T>
template <typename F>
RX_ALWAYS_INLINE void FastIndexText<T>::appendMergedIds(IDataHolder::MergeData &mergeData, size_t releventDocs, F &&appender) {
	auto &holder = *this->holder_;
	for (size_t i = 0; i < releventDocs; ++i) {
		auto &vid = mergeData[i];
		auto &vdoc = holder.vdocs_[vid.id];
		if (vdoc.keyEntry) {
			appender(vdoc.keyEntry->Sorted(0).begin(), vdoc.keyEntry->Sorted(0).end(), vid);
		}
	}
}

template <typename T>
void FastIndexText<T>::initConfig(const FtFastConfig *cfg) {
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
bool eq_c(Container &c1, Container &c2) {
	return c1.size() == c2.size() && std::equal(c1.begin(), c1.end(), c2.begin());
}

template <typename T>
void FastIndexText<T>::SetOpts(const IndexOpts &opts) {
	auto oldCfg = *getConfig();
	IndexText<T>::SetOpts(opts);
	auto &newCfg = *getConfig();

	if (!eq_c(oldCfg.stopWords, newCfg.stopWords) || oldCfg.stemmers != newCfg.stemmers || oldCfg.maxTypoLen != newCfg.maxTypoLen ||
		oldCfg.enableNumbersSearch != newCfg.enableNumbersSearch || oldCfg.extraWordSymbols != newCfg.extraWordSymbols ||
		oldCfg.synonyms != newCfg.synonyms || oldCfg.maxTypos != newCfg.maxTypos || oldCfg.optimization != newCfg.optimization) {
		logPrintf(LogInfo, "FulltextIndex config changed, it will be rebuilt on next search");
		this->isBuilt_ = false;
		if (oldCfg.optimization != newCfg.optimization) {
			initHolder(newCfg);
		} else {
			this->holder_->Clear();
		}
		this->holder_->status_ = FullRebuild;
		if (this->cache_ft_) this->cache_ft_->Clear();
		for (auto &idx : this->idx_map) idx.second.SetVDocID(FtKeyEntryData::ndoc);
	} else {
		logPrintf(LogInfo, "FulltextIndex config changed, cache cleared");
		if (this->cache_ft_) this->cache_ft_->Clear();
	}
	this->holder_->synonyms_->SetConfig(&newCfg);
}

template <typename T>
reindexer::FtPreselectT FastIndexText<T>::FtPreselect(const RdxContext &rdxCtx) {
	this->build(rdxCtx);
	return FtMergeStatuses{FtMergeStatuses::Statuses(holder_->vdocs_.size(), FtMergeStatuses::kExcluded),
						   std::vector<bool>(holder_->rowId2Vdoc_.size(), false), &holder_->rowId2Vdoc_};
}

std::unique_ptr<Index> FastIndexText_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexFastFT:
			return std::unique_ptr<Index>{new FastIndexText<unordered_str_map<FtKeyEntry>>(idef, std::move(payloadType), fields)};
		case IndexCompositeFastFT:
			return std::unique_ptr<Index>{new FastIndexText<unordered_payload_map<FtKeyEntry, true>>(idef, std::move(payloadType), fields)};
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
