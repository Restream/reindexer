#pragma once

#include <mutex>
#include "core/ft/config/baseftconfig.h"
#include "core/ft/ft_fast/dataholder.h"
#include "core/ft/ft_fuzzy/searchers/isearcher.h"
#include "core/ft/ftdsl.h"
#include "core/ft/ftsetcashe.h"
#include "core/ft/idrelset.h"
#include "core/ft/stemmer.h"
#include "core/index/indexunordered.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "estl/fast_hash_map.h"
#include "estl/flat_str_map.h"
#include "estl/shared_mutex.h"
#include "fieldsgetter.h"

//#define REINDEX_FT_EXTRA_DEBUG 1

namespace reindexer {
using std::pair;
using std::unique_ptr;

template <typename T>
class IndexText : public IndexUnordered<T> {
public:
	IndexText(const IndexText<T>& other);
	IndexText(const IndexDef& idef, const PayloadType payloadType, const FieldsSet& fields)
		: IndexUnordered<T>(idef, payloadType, fields), cache_ft_(new FtIdSetCache), isBuilt_(false) {
		this->selectKeyType_ = KeyValueString;
		initSearchers();
	}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType condition, SortType stype, Index::ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override final;
	void UpdateSortedIds(const UpdateSortedContext&) override {}
	virtual IdSet::Ptr Select(FtCtx::Ptr fctx, FtDSLQuery& dsl) = 0;
	void SetOpts(const IndexOpts& opts) override final;
	void Commit() override final;
	virtual void commitFulltext() = 0;
	void SetSortedIdxCount(int) override final{};

protected:
	void initSearchers();
	FieldsGetter<T> Getter();

	shared_ptr<FtIdSetCache> cache_ft_;
	fast_hash_map<string, int> ftFields_;
	unique_ptr<BaseFTConfig> cfg_;
	DataHolder holder_;
	shared_timed_mutex mtx_;
	bool isBuilt_;
};

}  // namespace reindexer
