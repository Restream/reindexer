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
	FuzzyIndexText(IndexType _type, const string& _name, const IndexOpts& opts) : IndexText<T>(_type, _name, opts) { CreateConfig(); }
	FuzzyIndexText(const FuzzyIndexText<T>& other) : IndexText<T>(other) { CreateConfig(other.GetConfig()); }

	template <typename U = T>
	FuzzyIndexText(IndexType _type, const string& _name, const IndexOpts& opts, const PayloadType payloadType, const FieldsSet& fields,
				   typename std::enable_if<is_payload_unord_map_key<U>::value>::type* = 0)
		: IndexText<T>(_type, _name, opts, payloadType, fields) {
		CreateConfig();
	}

	Index* Clone() override;
	IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) override final;
	void Commit() override final;

protected:
	FtFuzzyConfig* GetConfig() const;
	void CreateConfig(const FtFuzzyConfig* cfg = nullptr);
	SearchEngine engine_;
};  // namespace reindexer

Index* FuzzyIndexText_New(IndexType type, const string& _name, const IndexOpts& opts, const PayloadType payloadType,
						  const FieldsSet& fields_);

}  // namespace reindexer
