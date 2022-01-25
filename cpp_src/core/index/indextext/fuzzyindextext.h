#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fuzzy/searchengine.h"
#include "core/ft/ftsetcashe.h"
#include "core/ft/idrelset.h"
#include "indextext.h"

namespace reindexer {

template <typename T>
class FuzzyIndexText : public IndexText<T> {
public:
	FuzzyIndexText(const FuzzyIndexText<T>& other) : IndexText<T>(other) { CreateConfig(other.GetConfig()); }

	FuzzyIndexText(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields)
		: IndexText<T>(idef, std::move(payloadType), fields) {
		CreateConfig();
	}

	std::unique_ptr<Index> Clone() override;
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final {
		this->isBuilt_ = false;
		return IndexText<T>::Upsert(key, id, clearCache);
	}
	void Delete(const Variant& key, IdType id, StringsHolder& strHolder, bool& clearCache) override final {
		this->isBuilt_ = false;
		IndexText<T>::Delete(key, id, strHolder, clearCache);
	}

protected:
	void commitFulltextImpl() override final;
	FtFuzzyConfig* GetConfig() const;
	void CreateConfig(const FtFuzzyConfig* cfg = nullptr);

	search_engine::SearchEngine engine_;
	vector<VDocEntry> vdocs_;
};

std::unique_ptr<Index> FuzzyIndexText_New(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields);

}  // namespace reindexer
