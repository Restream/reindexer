#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fuzzy/searchengine.h"
#include "core/ft/ftsetcashe.h"
#include "core/ft/idrelset.h"
#include "indextext.h"

namespace reindexer {
using search_engine::SearchEngine;
using std::pair;
using std::unique_ptr;

template <typename T>
class FuzzyIndexText : public IndexText<T> {
public:
	FuzzyIndexText(const FuzzyIndexText<T>& other) : IndexText<T>(other) { CreateConfig(other.GetConfig()); }

	FuzzyIndexText(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields) : IndexText<T>(idef, payloadType, fields) {
		CreateConfig();
	}

	Index* Clone() override;
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	void commitFulltext() override final;
	Variant Upsert(const Variant& key, IdType id) override final {
		this->isBuilt_ = false;
		return IndexText<T>::Upsert(key, id);
	}
	void Delete(const Variant& key, IdType id) override final {
		this->isBuilt_ = false;
		IndexText<T>::Delete(key, id);
	}

protected:
	FtFuzzyConfig* GetConfig() const;
	void CreateConfig(const FtFuzzyConfig* cfg = nullptr);

	SearchEngine engine_;
	vector<VDocEntry> vdocs_;

};  // namespace reindexer

Index* FuzzyIndexText_New(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields);

}  // namespace reindexer
