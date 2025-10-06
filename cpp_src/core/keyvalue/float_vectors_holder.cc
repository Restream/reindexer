#include "float_vectors_holder.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/queryresults/itemref.h"
#include "core/queryresults/localqueryresults.h"

namespace reindexer {

namespace {
constexpr uint32_t kLimitNumberProcessedElements = 5000;

void checkPayloadVectorField([[maybe_unused]] const Payload& payload, [[maybe_unused]] FloatVectorIndexData idx) {
#ifdef RX_WITH_STDLIB_DEBUG
	VariantArray buffer;
	payload.Get(idx.ptField, buffer);
	assertrx_dbg(buffer.size() == 1);
	assertrx_dbg(buffer[0].Type().Is<KeyValueType::FloatVector>());
	assertrx_dbg(ConstFloatVectorView(buffer[0]).IsStrippedOrEmpty());
#endif	// RX_WITH_STDLIB_DEBUG
}
}  // namespace

template <typename It>
// NOLINTNEXTLINE(performance-unnecessary-value-param)
void FloatVectorsHolderMap::Add(const NamespaceImpl& ns, const It& it, const It& end, const FieldsFilter& filter) {
	if (!ns.haveFloatVectorsIndexes() || !filter.HasVectors()) {
		return;
	}

	auto nsIt = std::find_if(vectorsByNs_.cbegin(), vectorsByNs_.cend(), [&ns](const auto& item) { return item.first == &ns; });
	if (nsIt == vectorsByNs_.end()) {
		auto vectIndexes = ns.getVectorIndexes();
		FloatVectorIndexList indexes;
		indexes.reserve(vectIndexes.size());
		for (const auto& index : vectIndexes) {
			indexes.emplace_back(index, index.ptr->GetKeeper().Register());
		}
		vectorsByNs_.emplace_back(&ns, std::move(indexes));
		nsIt = std::prev(vectorsByNs_.end());
	}

	std::vector<IdType> ids;
	std::vector<ConstFloatVectorView> vectorsData;
	ids.reserve(kLimitNumberProcessedElements);
	vectorsData.reserve(kLimitNumberProcessedElements);

	for (const auto& info : nsIt->second) {
		if (filter.ContainsVector(info.index.ptField)) {
			add(ns, info, it, end, ids, vectorsData);
		}
	}
}

bool FloatVectorsHolderMap::Empty() const noexcept {
	return !std::ranges::any_of(vectorsByNs_, [](const auto& info) { return !info.second.empty(); });
}

template <typename It>
void FloatVectorsHolderMap::updatePayload(const NamespaceImpl& ns, const FloatVectorIndexData& index, It it, const It& end,
										  std::span<ConstFloatVectorView> vectorsData) {
	const auto field = index.ptField;
	size_t idx = 0;
	for (; it != end; ++it) {
		ItemRef& itemRef = it.GetItemRef();
		const auto id = itemRef.Id();
		if (id >= 0) {
			itemRef.Value().Clone();
			Payload payload{ns.payloadType_, itemRef.Value()};
			checkPayloadVectorField(payload, index);
			assertrx_throw(idx < vectorsData.size());
			const auto& view = vectorsData[idx++];
			if (!view.IsEmpty()) {
				payload.Set(field, Variant{view});
			}
		}
	}
}

template <typename It>
void FloatVectorsHolderMap::add(const NamespaceImpl& ns, const FloatVectorIndexInfo& indexInfo, It it, const It& end,
								std::vector<IdType>& ids, std::vector<ConstFloatVectorView>& vectorsData) {
	const auto& tag = indexInfo.tag;
	const auto& index = indexInfo.index;
	auto& keeper = index.ptr->GetKeeper();

	ids.resize(0);

	It itCurr = it;
	for (; it != end; ++it) {
		ItemRef& itemRef = it.GetItemRef();
		const auto id = itemRef.Id();
		if (id >= 0) {
			ids.push_back(id);
			if (ids.size() >= kLimitNumberProcessedElements) {
				auto itNext = it;
				++itNext;
				keeper.GetFloatVectors(tag, ids, vectorsData);
				assertrx_throw(ids.size() == vectorsData.size());
				updatePayload(ns, index, itCurr, itNext, vectorsData);

				ids.resize(0);
				vectorsData.resize(0);
				itCurr = itNext;
			}
		}
	}

	if (!ids.empty()) {
		keeper.GetFloatVectors(tag, ids, vectorsData);
		assertrx_throw(ids.size() == vectorsData.size());
		updatePayload(ns, index, itCurr, end, vectorsData);

		ids.resize(0);
		vectorsData.resize(0);
	}
}

template void FloatVectorsHolderMap::Add(const NamespaceImpl&, const LocalQueryResults::Iterator&, const LocalQueryResults::Iterator&,
										 const FieldsFilter&);
template void FloatVectorsHolderMap::Add(const NamespaceImpl&, const JoinPreResult::Values::Iterator&,
										 const JoinPreResult::Values::Iterator&, const FieldsFilter&);
}  // namespace reindexer
