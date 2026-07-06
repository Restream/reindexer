#pragma once

#include "core/index/indexunordered.h"
#include "rtree.h"

namespace reindexer {

template <typename KeyEntryT, template <typename, typename, typename, typename, size_t, size_t> class Splitter, size_t MaxEntries,
		  size_t MinEntries>
class [[nodiscard]] IndexRTree : public IndexUnordered<GeometryMap<KeyEntryT, Splitter, MaxEntries, MinEntries>> {
	using Map = GeometryMap<KeyEntryT, Splitter, MaxEntries, MinEntries>;

public:
	IndexRTree(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: IndexUnordered<Map>{idef, std::move(payloadType), std::move(fields), cacheCfg} {}

	SelectKeyResults SelectKey(const VariantArray& keys, CondType, SortType, const Index::SelectContext&, const RdxContext&) override;
	using IndexUnordered<Map>::Upsert;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) override;
	using IndexUnordered<Map>::Delete;
	void Delete(const VariantArray& keys, IdType id, MustExist mustExist, StringsHolder&, bool& clearCache) override;

	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new IndexRTree(*this)); }

private:
	IndexRTree(const IndexRTree&) = default;
};

std::unique_ptr<Index> IndexRTree_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									  const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
