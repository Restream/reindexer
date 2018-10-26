
#pragma once

#include "indexunordered.h"
namespace reindexer {

template <typename T>
class IndexOrdered : public IndexUnordered<T> {
public:
	IndexOrdered(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields)
		: IndexUnordered<T>(idef, payloadType, fields) {}

	SelectKeyResults SelectKey(const VariantArray &keys, CondType condition, SortType stype, Index::ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	Variant Upsert(const Variant &key, IdType id) override;
	void MakeSortOrders(UpdateSortedContext &ctx) override;
	Index *Clone() override;
	bool IsOrdered() const override;

protected:
	template <typename U = T, typename std::enable_if<is_string_map_key<U>::value>::type * = nullptr>
	typename T::iterator lower_bound(const Variant &key, bool &found);
	template <typename U = T, typename std::enable_if<!is_string_map_key<U>::value>::type * = nullptr>
	typename T::iterator lower_bound(const Variant &key, bool &found);
};

Index *IndexOrdered_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
