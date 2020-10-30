#pragma once

#include "core/index/indexunordered.h"
#include "rtree.h"

namespace reindexer {

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t> class Splitter>
class IndexRTree : public IndexUnordered<GeometryMap<KeyEntryT, Splitter>> {
	using Map = GeometryMap<KeyEntryT, Splitter>;

public:
	IndexRTree(const IndexDef &idef, const PayloadType &payloadType, const FieldsSet &fields)
		: IndexUnordered<Map>{idef, payloadType, fields} {}

	SelectKeyResults SelectKey(const VariantArray &keys, CondType, SortType, Index::SelectOpts, BaseFunctionCtx::Ptr,
							   const RdxContext &) override;
	using IndexUnordered<Map>::Upsert;
	void Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool needUpsertEmptyValue) override;
	using IndexUnordered<Map>::Delete;
	void Delete(const VariantArray &keys, IdType id) override;
};

Index *IndexRTree_New(const IndexDef &idef, const PayloadType &payloadType, const FieldsSet &fields);

}  // namespace reindexer
