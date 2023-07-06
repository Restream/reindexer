#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fuzzy/searchengine.h"
#include "core/ft/ftsetcashe.h"
#include "core/ft/idrelset.h"
#include "indextext.h"

namespace reindexer {

template <typename T>
class FuzzyIndexText : public IndexText<T> {
	using Base = IndexText<T>;

public:
	FuzzyIndexText(const FuzzyIndexText<T>& other) : Base(other) { CreateConfig(other.GetConfig()); }

	FuzzyIndexText(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields) : Base(idef, std::move(payloadType), fields) {
		CreateConfig();
	}

	SelectKeyResults SelectKey(const VariantArray& /*keys*/, CondType, Index::SelectOpts, const BaseFunctionCtx::Ptr&, FtPreselectT&&,
							   const RdxContext&) override final {
		assertrx(0);
		abort();
	}
	std::unique_ptr<Index> Clone() const override final { return std::unique_ptr<Index>{new FuzzyIndexText<T>(*this)}; }
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery&& dsl, bool inTransaction, FtMergeStatuses&&, FtUseExternStatuses,
					  const RdxContext&) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final {
		this->isBuilt_ = false;
		return Base::Upsert(key, id, clearCache);
	}
	void Delete(const Variant& key, IdType id, StringsHolder& strHolder, bool& clearCache) override final {
		this->isBuilt_ = false;
		Base::Delete(key, id, strHolder, clearCache);
	}
	FtMergeStatuses GetFtMergeStatuses(const RdxContext& rdxCtx) override final {
		this->build(rdxCtx);
		return {{}, {}, nullptr};
	}

protected:
	void commitFulltextImpl() override final;
	FtFuzzyConfig* GetConfig() const;
	void CreateConfig(const FtFuzzyConfig* cfg = nullptr);

	search_engine::SearchEngine engine_;
	std::vector<VDocEntry> vdocs_;
};

std::unique_ptr<Index> FuzzyIndexText_New(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields);

}  // namespace reindexer
