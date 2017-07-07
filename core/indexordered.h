
#pragma once

#include "core/indexunordered.h"
namespace reindexer {

template <typename T>
class IndexOrdered : public IndexUnordered<T> {
public:
	IndexOrdered(IndexType _type, const string &_name, const IndexOpts &opts) : IndexUnordered<T>(_type, _name, opts) {}

	template <typename U = T>
	IndexOrdered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType &payloadType, const FieldsSet &fields,
				 typename std::enable_if<is_payload_map_key<U>::value>::type * = 0)
		: IndexUnordered<T>(_type, _name, opts, payloadType, fields) {}
	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, Index::ResultType res_type) override;
	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void MakeSortOrders(UpdateSortedContext &ctx) override;
	bool IsSorted() const override { return true; }
	Index *Clone() override;

protected:
	typename T::iterator lower_bound(const KeyRef &key, bool &found);
};

}  // namespace reindexer
