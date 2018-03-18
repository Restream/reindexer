
#pragma once

#include "indexunordered.h"
namespace reindexer {

template <typename T>
class IndexOrdered : public IndexUnordered<T> {
public:
	IndexOrdered(IndexType _type, const string &_name, const IndexOpts &opts) : IndexUnordered<T>(_type, _name, opts) {}

	template <typename U = T>
	IndexOrdered(IndexType _type, const string &_name, const IndexOpts &opts, const PayloadType payloadType, const FieldsSet &fields,
				 typename std::enable_if<is_payload_map_key<U>::value>::type * = 0)
		: IndexUnordered<T>(_type, _name, opts, payloadType, fields) {}

	SelectKeyResults SelectKey(const KeyValues &keys, CondType condition, SortType stype, Index::ResultType res_type,
							   BaseFunctionCtx::Ptr ctx) override;
	KeyRef Upsert(const KeyRef &key, IdType id) override;
	void MakeSortOrders(UpdateSortedContext &ctx) override;
	Index *Clone() override;
	bool IsOrdered() const override;

protected:
	template <typename U = T, typename std::enable_if<is_string_map_key<U>::value>::type * = nullptr>
	typename T::iterator lower_bound(const KeyRef &key, bool &found);
	template <typename U = T, typename std::enable_if<!is_string_map_key<U>::value>::type * = nullptr>
	typename T::iterator lower_bound(const KeyRef &key, bool &found);
};

Index *IndexOrdered_New(IndexType type, const string &_name, const IndexOpts &opts, const PayloadType payloadType,
						const FieldsSet &fields_);

}  // namespace reindexer
