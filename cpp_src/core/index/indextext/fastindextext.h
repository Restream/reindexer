#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "indextext.h"

namespace reindexer {

struct MergeData;

template <typename T>
class FastIndexText : public IndexText<T> {
	using Base = IndexText<T>;

public:
	using key_type = typename IndexUnordered<T>::key_type;
	using ref_type = typename IndexUnordered<T>::ref_type;

	FastIndexText(const FastIndexText& other) : Base(other) {
		initConfig(other.getConfig());
		for (auto& idx : this->idx_map) idx.second.SetVDocID(FtKeyEntryData::ndoc);
		this->CommitFulltext();
	}

	FastIndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: Base(idef, std::move(payloadType), std::move(fields), cacheCfg) {
		initConfig();
	}
	std::unique_ptr<Index> Clone() const override { return std::make_unique<FastIndexText<T>>(*this); }
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery&& dsl, bool inTransaction, FtMergeStatuses&&, FtUseExternStatuses,
					  const RdxContext&) override final;
	IndexMemStat GetMemStat(const RdxContext&) override final;
	Variant Upsert(const Variant& key, IdType id, bool& clearCache) override final;
	void Delete(const Variant& key, IdType id, StringsHolder&, bool& clearCache) override final;
	void SetOpts(const IndexOpts& opts) override final;
	FtMergeStatuses GetFtMergeStatuses(const RdxContext& rdxCtx) override final {
		this->build(rdxCtx);
		return {FtMergeStatuses::Statuses(holder_->vdocs_.size(), 0), std::vector<bool>(holder_->rowId2Vdoc_.size(), false),
				&holder_->rowId2Vdoc_};
	}
	reindexer::FtPreselectT FtPreselect(const RdxContext& rdxCtx) override final;
	bool EnablePreselectBeforeFt() const override final { return getConfig()->enablePreselectBeforeFt; }

private:
	void commitFulltextImpl() override final;
	FtFastConfig* getConfig() const noexcept { return dynamic_cast<FtFastConfig*>(this->cfg_.get()); }
	void initConfig(const FtFastConfig* = nullptr);
	void initHolder(FtFastConfig&);
	template <class Data>
	void buildVdocs(Data& data);
	template <typename F>
	void appendMergedIds(MergeData& merged, size_t releventDocs, F&& appender);

	std::unique_ptr<IDataHolder> holder_;
};

std::unique_ptr<Index> FastIndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										 const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
