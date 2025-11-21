#include "fuzzyindextext.h"
#include <stdio.h>
#include "core/rdxcontext.h"

namespace reindexer {

template <typename T>
IdSet::Ptr FuzzyIndexText<T>::Select(FtCtx& ftCtx, FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses&&,
									 FtUseExternStatuses withExternSt, const RdxContext& rdxCtx) {
	assertrx_throw(withExternSt == FtUseExternStatuses::No);
	(void)withExternSt;
	auto result = engine_.Search(dsl, inTransaction, rdxCtx);

	auto mergedIds = make_intrusive<intrusive_atomic_rc_wrapper<IdSet>>();

	mergedIds->reserve(result.data_->size() * 2);
	ftCtx.Ranks().Reserve(result.data_->size() * 2);
	double coof = 1;
	if (result.max_proc_ > 100) {
		coof = 100 / result.max_proc_;
	}
	size_t counter = 0;
	for (auto it = result.data_->begin(); it != result.data_->end(); ++it, ++counter) {
		it->rank_ *= coof;
		if (it->rank_ < getConfig()->minOkProc) {
			continue;
		}
		assertrx(it->id_ < this->vdocs_.size());
		const auto& id_set = this->vdocs_[it->id_].keyEntry->Sorted(0);
		ftCtx.Add(id_set.begin(), id_set.end(), RankT(it->rank_));
		mergedIds->Append(id_set.begin(), id_set.end(), IdSet::Unordered);
		if ((counter & 0xFF) == 0 && !inTransaction) {
			ThrowOnCancel(rdxCtx);
		}
	}

	return mergedIds;
}

template <typename T>
void FuzzyIndexText<T>::commitFulltextImpl() {
	std::vector<std::unique_ptr<std::string>> bufStrs;
	auto gt = this->Getter();
	for (auto& doc : this->idx_map) {
		auto res = gt.getDocFields(doc.first, bufStrs);
#ifdef REINDEX_FT_EXTRA_DEBUG
		std::string text(res[0].first);
		this->vdocs_.push_back({(text.length() > 48) ? text.substr(0, 48) + "..." : text, doc.second.get(), {}, {}});
#else
		this->vdocs_.push_back({doc.second.get(), {}, {}});
#endif
		for (size_t idx = 0, arrayIdx = 0; idx < res.size(); ++idx, ++arrayIdx) {
			const unsigned field = res[idx].second;
			if (idx > 0 && field != res[idx - 1].second) {
				arrayIdx = 0;
			}
			engine_.AddData(res[idx].first, this->vdocs_.size() - 1, field, arrayIdx, this->cfg_->splitOptions);
		}
	}
	engine_.Commit();
	this->isBuilt_ = true;
}

template <typename T>
void FuzzyIndexText<T>::createConfig(const FtFuzzyConfig* cfg) {
	if (cfg) {
		this->cfg_.reset(new FtFuzzyConfig(*cfg));
		return;
	}
	this->cfg_.reset(new FtFuzzyConfig());
	this->cfg_->parse(this->opts_.Config(), this->ftFields_);
}

std::unique_ptr<Index> FuzzyIndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										  const NamespaceCacheConfigData& cacheCfg) {
	switch (idef.IndexType()) {
		case IndexFuzzyFT:
			return std::make_unique<FuzzyIndexText<unordered_str_map<FtKeyEntry>>>(idef, std::move(payloadType), std::move(fields),
																				   cacheCfg);
		case IndexCompositeFuzzyFT:
			return std::make_unique<FuzzyIndexText<unordered_payload_map<FtKeyEntry, true>>>(idef, std::move(payloadType),
																							 std::move(fields), cacheCfg);
		case IndexStrHash:
		case IndexStrBTree:
		case IndexIntBTree:
		case IndexIntHash:
		case IndexInt64BTree:
		case IndexInt64Hash:
		case IndexDoubleBTree:
		case IndexFastFT:
		case IndexCompositeBTree:
		case IndexCompositeHash:
		case IndexCompositeFastFT:
		case IndexBool:
		case IndexIntStore:
		case IndexInt64Store:
		case IndexStrStore:
		case IndexDoubleStore:
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
