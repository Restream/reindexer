#pragma once

#include "core/ft/ft_fuzzy/searchengine.h"
#include "indextext.h"

namespace reindexer {

template <typename T>
class [[nodiscard]] FuzzyIndexText : public IndexText<T> {
	using Base = IndexText<T>;

public:
	FuzzyIndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: Base(idef, std::move(payloadType), std::move(fields), cacheCfg) {
		createConfig();
	}

	SelectKeyResults SelectKey(const VariantArray& /*keys*/, CondType, const Index::SelectContext&, FtPreselectT&&,
							   const RdxContext&) override final {
		assertrx(0);
		abort();
	}

	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override final {
		return std::unique_ptr<Index>(new FuzzyIndexText<T>(*this));
	}
	IdSet::Ptr Select(FtCtx& ctx, FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses&&, FtUseExternStatuses,
					  const RdxContext&) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final {
		this->isBuilt_ = false;
		return Base::Upsert(key, id, clearCache);
	}
	void Delete(const Variant& key, IdType id, MustExist mustExist, StringsHolder& strHolder, bool& clearCache) override final {
		this->isBuilt_ = false;
		Base::Delete(key, id, mustExist, strHolder, clearCache);
	}
	FtMergeStatuses GetFtMergeStatuses(const RdxContext& rdxCtx) override final {
		this->build(rdxCtx);
		return {{}, {}, nullptr};
	}

private:
	FuzzyIndexText(const FuzzyIndexText<T>& other) : Base(other) { createConfig(other.getConfig()); }

	void commitFulltextImpl() override final;
	FtFuzzyConfig* getConfig() const noexcept { return dynamic_cast<FtFuzzyConfig*>(this->cfg_.get()); }
	void createConfig(const FtFuzzyConfig* cfg = nullptr);

	search_engine::SearchEngine engine_;
	std::vector<VDocEntry> vdocs_;
};

std::unique_ptr<Index> FuzzyIndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										  const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
