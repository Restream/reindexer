#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "indextext.h"

namespace reindexer {

template <typename Map>
class FastIndexText : public IndexText<Map> {
	using Base = IndexText<Map>;

public:
	using key_type = typename IndexUnordered<Map>::key_type;
	using ref_type = typename IndexUnordered<Map>::ref_type;

	FastIndexText(const FastIndexText& other) : Base(other) {
		initConfig(other.getConfig());
		for (auto& idx : this->idx_map) {
			idx.second.SetVDocID(FtKeyEntryData::ndoc);
		}
	}

	FastIndexText(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: Base(idef, std::move(payloadType), std::move(fields), cacheCfg) {
		initConfig();
	}
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override {
		// Creates uncommited copy
		return std::make_unique<FastIndexText<Map>>(*this);
	}
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery&& dsl, bool inTransaction, RankSortType, FtMergeStatuses&&, FtUseExternStatuses,
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
	template <typename MergeType>
	IdSet::Ptr afterSelect(FtCtx& fctx, MergeType&& mergeData, RankSortType, FtMergeStatuses&& statuses, FtUseExternStatuses);

	template <typename VectorType>
	IdSet::Ptr applyOptimizationAndSelect(DataHolder<VectorType>* d, BaseFunctionCtx::Ptr bctx, FtDSLQuery&& dsl, bool inTransaction,
										  RankSortType, FtMergeStatuses&& statuses, FtUseExternStatuses, const RdxContext& rdxCtx);

	template <typename VectorType, FtUseExternStatuses useExternalStatuses>
	IdSet::Ptr applyCtxTypeAndSelect(DataHolder<VectorType>* d, const BaseFunctionCtx::Ptr& bctx, FtDSLQuery&& dsl, bool inTransaction,
									 RankSortType, FtMergeStatuses&& statuses, FtUseExternStatuses useExternSt, const RdxContext& rdxCtx);

	void commitFulltextImpl() override final;
	FtFastConfig* getConfig() const noexcept { return dynamic_cast<FtFastConfig*>(this->cfg_.get()); }
	void initConfig(const FtFastConfig* = nullptr);
	void initHolder(FtFastConfig&);
	template <class Data>
	void buildVdocs(Data& data);
	template <typename MergeType, typename F>
	void appendMergedIds(MergeType& merged, size_t relevantDocs, F&& appender);
	template <typename MergeType>
	typename MergeType::iterator unstableRemoveIf(MergeType& md, int minRelevancy, double scalingFactor, size_t& relevantDocs, int& cnt);

	std::unique_ptr<IDataHolder> holder_;
};

std::unique_ptr<Index> FastIndexText_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
										 const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
