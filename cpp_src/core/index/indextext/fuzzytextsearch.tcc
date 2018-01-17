#include <stdio.h>

#include "fuzzytextsearch.h"
#include "tools/customlocal.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

using std::make_shared;

namespace reindexer {
using std::wstring;
using search_engine::MergedData;

template <typename T>
Index* FuzzyIndexText<T>::Clone() {
	return new FuzzyIndexText<T>(*this);
}

template <typename T>
IdSet::Ptr FuzzyIndexText<T>::Select(FullTextCtx::Ptr fctx, FtDSLQuery& dsl) {
	auto result = engine_.Search(dsl);

	auto mergedIds = make_shared<IdSet>();

	mergedIds->reserve(result.data_->size() * 2);
	fctx->Reserve(result.data_->size() * 2);
	double coof = 1;
	if (result.max_proc_ > 100) {
		coof = 100 / result.max_proc_;
	}
	for (auto it = result.data_->begin(); it != result.data_->end(); ++it) {
		it->proc_ *= coof;
		if (it->proc_ < GetConfig()->minOkProc) continue;
		assert(it->id_ < this->vdocs_.size());
		const auto& id_set = this->vdocs_[it->id_].keyEntry->Sorted(0);
		fctx->Add(id_set.begin(), id_set.end(), it->proc_);
		mergedIds->Append(id_set.begin(), id_set.end(), IdSet::Unordered);
	}

	return mergedIds;
}

template <typename T>
void FuzzyIndexText<T>::Commit() {
	vector<unique_ptr<string>> bufStrs;

	for (auto& doc : this->idx_map) {
		auto res = this->getDocFields(doc.first, bufStrs);
		this->vdocs_.push_back({&doc.second, {}, {}});
		for (auto& r : res) {
			engine_.AddData(r.first, this->vdocs_.size() - 1, r.second);
		}
	}
	engine_.Commit();
}
template <typename T>
FtFuzzyConfig* FuzzyIndexText<T>::GetConfig() const {
	return dynamic_cast<FtFuzzyConfig*>(this->cfg_.get());
}
template <typename T>
void FuzzyIndexText<T>::CreateConfig(const FtFuzzyConfig* cfg) {
	if (cfg) {
		this->cfg_.reset(new FtFuzzyConfig(*cfg));
		return;
	}
	this->cfg_.reset(new FtFuzzyConfig());
}
}  // namespace reindexer
