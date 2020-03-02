#pragma once

#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ft_fast/dataprocessor.h"
#include "core/ft/typos.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "indextext.h"

namespace reindexer {
using std::pair;
using std::unique_ptr;

template <typename T>
class FastIndexText : public IndexText<T> {
public:
	FastIndexText(const FastIndexText<T>& other) : IndexText<T>(other) {
		CreateConfig(other.GetConfig());
		for (auto& idx : this->idx_map) idx.second.VDocID() = FtKeyEntryData::ndoc;
		commitFulltext();
		this->isBuilt_ = true;
	}

	FastIndexText(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields) : IndexText<T>(idef, payloadType, fields) {
		CreateConfig();
	}
	Index* Clone() override;
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	void commitFulltext() override final;
	IndexMemStat GetMemStat() override;
	Variant Upsert(const Variant& key, IdType id) override final;
	void Delete(const Variant& key, IdType id) override final;
	void SetOpts(const IndexOpts& opts) override final;

protected:
	FtFastConfig* GetConfig() const;
	void CreateConfig(const FtFastConfig* cfg = nullptr);

	template <class Data>
	void BuildVdocs(Data& data);
};

Index* FastIndexText_New(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields);

}  // namespace reindexer
