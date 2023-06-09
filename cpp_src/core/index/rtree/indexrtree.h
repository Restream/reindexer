#pragma once

#include "core/index/indexunordered.h"
#include "rtree.h"

namespace reindexer {

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
class IndexRTree : public IndexUnordered<GeometryMap<KeyEntryT, Splitter, MaxEntries, MinEntries>> {
	using Map = GeometryMap<KeyEntryT, Splitter, MaxEntries, MinEntries>;

public:
	IndexRTree(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields)
		: IndexUnordered<Map>{idef, std::move(payloadType), fields} {}

	SelectKeyResults SelectKey(const VariantArray &keys, CondType, SortType, Index::SelectOpts, const BaseFunctionCtx::Ptr &,
							   const RdxContext &) override;
	using IndexUnordered<Map>::Upsert;
	void Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool &clearCache) override;
	using IndexUnordered<Map>::Delete;
	void Delete(const VariantArray &keys, IdType id, StringsHolder &, bool &clearCache) override;

	std::unique_ptr<Index> Clone() const override { return std::unique_ptr<Index>{new IndexRTree(*this)}; }
};

std::unique_ptr<Index> IndexRTree_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
