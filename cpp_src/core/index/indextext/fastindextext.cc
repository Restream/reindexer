#include "fastindextext.h"
#include <chrono>
#include <memory>
#include <thread>
#include "core/ft/bm25.h"
#include "core/ft/ft_fast/ftfastkeyentry.h"
#include "core/ft/ft_fast/selecter.h"
#include "core/ft/numtotext.h"
#include "tools/logger.h"

namespace reindexer {
using std::pair;

using std::thread;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::high_resolution_clock;
using std::make_shared;

template <typename T>
Index *FastIndexText<T>::Clone() {
	return new FastIndexText<T>(*this);
}

template <typename T>
Variant FastIndexText<T>::Upsert(const Variant &key, IdType id) {
	if (key.Type() == KeyValueNull) {
		this->empty_ids_.Unsorted().Add(id, IdSet::Auto);
		// Return invalid ref
		return Variant();
	}

	auto keyIt = this->find(key);
	if (keyIt == this->idx_map.end()) {
		keyIt = this->idx_map.insert({static_cast<typename T::key_type>(key), typename T::mapped_type()}).first;
		this->markUpdated(&*keyIt);
	}
	keyIt->second.Unsorted().Add(id, this->opts_.IsPK() ? IdSet::Ordered : IdSet::Auto);

	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		return IndexStore<typename T::key_type>::Upsert(key, id);
	}

	return Variant(keyIt->first);
}

template <typename T>
void FastIndexText<T>::Delete(const Variant &key, IdType id) {
	int delcnt = 0;
	if (key.Type() == KeyValueNull) {
		delcnt = this->empty_ids_.Unsorted().Erase(id);
		assert(delcnt);
		return;
	}

	auto keyIt = this->find(key);
	if (keyIt == this->idx_map.end()) return;

	delcnt = keyIt->second.Unsorted().Erase(id);
	(void)delcnt;
	// TODO: we have to implement removal of composite indexes (doesn't work right now)
	assertf(this->opts_.IsArray() || this->Opts().IsSparse() || delcnt, "Delete unexists id from index '%s' id=%d,key=%s",
			this->name_.c_str(), id, Variant(key).As<string>().c_str());

	if (keyIt->second.Unsorted().IsEmpty()) {
		this->tracker_.markDeleted(&*keyIt);
		if (this->holder_.vdocs_.size()) {
			assert(keyIt->second.vdoc_id_ < this->holder_.vdocs_.size());
			this->holder_.vdocs_[keyIt->second.vdoc_id_].keyEntry = nullptr;
		}
		this->idx_map.erase(keyIt);
	}
	if (this->KeyType() == KeyValueString && this->opts_.GetCollateMode() != CollateNone) {
		IndexStore<typename T::key_type>::Delete(key, id);
	}
}

template <typename T>
IndexMemStat FastIndexText<T>::GetMemStat() {
	auto ret = IndexUnordered<T>::GetMemStat();
	ret.fulltextSize = this->holder_.GetMemStat();
	if (this->cache_ft_) ret.idsetCache = this->cache_ft_->GetMemStat();
	return ret;
}
template <typename T>
const typename T::mapped_type *FastIndexText<T>::GetEntry(const void *entry) {
	return reinterpret_cast<const typename T::mapped_type *>(entry);
}

template <typename T>
IdSet::Ptr FastIndexText<T>::Select(FtCtx::Ptr fctx, FtDSLQuery &dsl) {
	fctx->GetData()->extraWordSymbols_ = this->GetConfig()->extraWordSymbols;
	fctx->GetData()->isWordPositions_ = true;

	auto merdeInfo = Selecter(this->holder_, *this->GetConfig(), this->fields_.size(), fctx->NeedArea()).Process(dsl);
	// convert vids(uniq documents id) to ids (real ids)
	IdSet::Ptr mergedIds = std::make_shared<IdSet>();
	auto &holder = this->holder_;
	auto &vdocs = holder.vdocs_;
	if (merdeInfo.empty()) return mergedIds;
	int cnt = 0;
	int minRelevancy = GetConfig()->minRelevancy * 100;
	for (auto &vid : merdeInfo) {
		assert(vid.id < int(vdocs.size()));
		if (!vdocs[vid.id].keyEntry) {
			continue;
		};
		if (vid.proc <= minRelevancy) break;
		cnt += GetEntry(vdocs[vid.id].keyEntry)->Sorted(0).size();
	}

	mergedIds->reserve(cnt);
	fctx->Reserve(cnt);
	for (auto &vid : merdeInfo) {
		auto id = vid.id;
		assert(id < IdType(vdocs.size()));

		if (!vdocs[id].keyEntry) {
			continue;
		};
		assert(!GetEntry(vdocs[id].keyEntry)->Unsorted().empty());
		if (vid.proc <= minRelevancy) break;
		int proc = std::min(255, vid.proc / merdeInfo.mergeCnt);
		fctx->Add(GetEntry(vdocs[id].keyEntry)->Sorted(0).begin(), GetEntry(vdocs[id].keyEntry)->Sorted(0).end(), proc,
				  std::move(vid.holder));
		mergedIds->Append(GetEntry(vdocs[id].keyEntry)->Sorted(0).begin(), GetEntry(vdocs[id].keyEntry)->Sorted(0).end(), IdSet::Unordered);
	}
	if (GetConfig()->logLevel >= LogInfo) {
		logPrintf(LogInfo, "Total merge out: %d ids", int(mergedIds->size()));

		string str;
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
		logPrintf(LogInfo, "Relevancy(%d): %s", int(fctx->GetSize()), str.c_str());
	}
	assert(mergedIds->size() == fctx->GetSize());
	return mergedIds;
}
template <typename T>
void FastIndexText<T>::Commit() {
	this->holder_.StartCommit(this->tracker_.completeUpdate_);
	if (this->holder_.status_ == FullRebuild) {
		BuildVdocs(this->idx_map);
	} else {
		BuildVdocs(this->tracker_.updated_);
	}
	DataProcessor dp(this->holder_, *this->GetConfig(), this->fields_.size());
	dp.Process(!this->opts_.IsDense());
	if (this->holder_.NeedClear(this->tracker_.completeUpdate_)) {
		this->tracker_.completeUpdate_ = false;
		this->tracker_.updated_.clear();
	}
}

// hack wothout c++14
pair<const key_string, FtFastKeyEntry> &GetPair(pair<const key_string, FtFastKeyEntry> &pair) { return pair; }
pair<const key_string, FtFastKeyEntry> &GetPair(pair<const key_string, FtFastKeyEntry> *pair) { return *pair; }
pair<const PayloadValue, FtFastKeyEntry> &GetPair(pair<const PayloadValue, FtFastKeyEntry> &pair) { return pair; }
pair<const PayloadValue, FtFastKeyEntry> &GetPair(pair<const PayloadValue, FtFastKeyEntry> *pair) { return *pair; }

template <typename T>
template <class Data>
void FastIndexText<T>::BuildVdocs(Data &data) {
	// buffer strings, for printing non text fields
	auto &bufStrs = this->holder_.bufStrs_;
	// array with pointers to docs fields text
	// Prepare vdocs -> addresable array all docs in the index

	this->holder_.vodcsOffset_ = this->holder_.vdocs_.size();
	this->holder_.szCnt = 0;
	auto &vdocs = this->holder_.vdocs_;
	auto &vdocsTexts = this->holder_.vdocsTexts;

	vdocs.reserve(vdocs.size() + data.size());
	vdocsTexts.reserve(data.size());

	auto gt = this->Getter();

	for (auto &doc : data) {
		// if constexpr(std::is_pointer<data>::value_type) - when we go to c++17
#ifdef REINDEX_FT_EXTRA_DEBUG

		vdocs.push_back({&GetPair(doc).first, &GetPair(doc).second, {}, {}});
#else

		GetPair(doc).second.vdoc_id_ = vdocs.size();

		vdocs.push_back({&GetPair(doc).second, {}, {}});

#endif

		vdocsTexts.emplace_back(gt.getDocFields(GetPair(doc).first, bufStrs));

		if (GetConfig()->logLevel <= LogInfo) {
			for (auto &f : vdocsTexts.back()) this->holder_.szCnt += f.first.length();
		}
	}
}

template <typename T>
FtFastConfig *FastIndexText<T>::GetConfig() const {
	return dynamic_cast<FtFastConfig *>(this->cfg_.get());
}
template <typename T>
void FastIndexText<T>::CreateConfig(const FtFastConfig *cfg) {
	if (cfg) {
		this->cfg_.reset(new FtFastConfig(*cfg));
		return;
	}
	this->cfg_.reset(new FtFastConfig());
	string config = this->opts_.config;
	this->cfg_->parse(&config[0]);
}

Index *FastIndexText_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields) {
	switch (idef.Type()) {
		case IndexFastFT:
			return new FastIndexText<unordered_str_map<FtFastKeyEntry>>(idef, payloadType, fields);
		case IndexCompositeFastFT:
			return new FastIndexText<unordered_payload_map<FtFastKeyEntry>>(idef, payloadType, fields);
		default:
			abort();
	}
}

}  // namespace reindexer
