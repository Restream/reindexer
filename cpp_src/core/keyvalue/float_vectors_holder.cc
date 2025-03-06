#include "float_vectors_holder.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/queryresults/itemref.h"
#include "core/queryresults/localqueryresults.h"

namespace reindexer {

template <typename It>
// NOLINTNEXTLINE(performance-unnecessary-value-param)
void FloatVectorsHolderMap::Add(const NamespaceImpl& ns, It it, It end, const FieldsFilter& filter) {
	if (!ns.haveFloatVectorsIndexes() || !filter.HasVectors()) {
		return;
	}
	if (!vectorsByNs_.has_value()) {
		vectorsByNs_.emplace();
	}
	auto nsIt = vectorsByNs_->find(&ns);
	if (nsIt == vectorsByNs_->end()) {
		nsIt = vectorsByNs_->emplace(&ns, ns.getVectorIndexes()).first;
	}
	auto& nsFloatVectorsHolder = nsIt->second;
	for (; it != end; ++it) {
		ItemRef& itemRef = it.GetItemRef();
		if (itemRef.Id() < 0) {
			continue;
		}
		add(nsFloatVectorsHolder.fvIndexes, nsFloatVectorsHolder.vectorsById, itemRef, ns.payloadType_, filter);
	}
}

void checkPayloadVectorField([[maybe_unused]] const Payload& payload, [[maybe_unused]] FloatVectorIndexData idx) {
#ifdef RX_WITH_STDLIB_DEBUG
	VariantArray buffer;
	payload.Get(idx.ptField, buffer);
	assertrx_dbg(buffer.size() == 1);
	assertrx_dbg(buffer[0].Type().Is<KeyValueType::FloatVector>());
	assertrx_dbg(ConstFloatVectorView(buffer[0]).IsStrippedOrEmpty());
#endif	// RX_WITH_STDLIB_DEBUG
}

void FloatVectorsHolderMap::add(const FloatVectorsIndexes& floatVectorsIndexes, VectorsById& vectorsById, ItemRef& itemRef,
								const PayloadType& payloadType, const FieldsFilter& filter) {
	const auto id = itemRef.Id();
	const auto [idIt, isNew] = vectorsById.emplace(id, FloatVectorsHolderVector{});
	auto& floatVectorsHolder = idIt->second;
	itemRef.Value().Clone();
	Payload payload{payloadType, itemRef.Value()};
	if (isNew) {
		floatVectorsHolder.reserve(floatVectorsIndexes.size());
		for (const auto& idx : floatVectorsIndexes) {
			checkPayloadVectorField(payload, idx);
			if (filter.ContainsVector(idx.ptField)) {
				auto vect = idx.ptr->GetFloatVector(id);
				if (vect.IsEmpty()) {
					continue;
				}
				floatVectorsHolder.Add(std::move(vect));
				payload.Set(idx.ptField, Variant{ConstFloatVectorView{floatVectorsHolder.Back()}});
			}
		}
		if (floatVectorsHolder.empty()) {
			vectorsById.erase(idIt);
		}
	} else {
		for (size_t i = 0, s = floatVectorsIndexes.size(); i < s; ++i) {
			const auto& idx = floatVectorsIndexes[i];
			checkPayloadVectorField(payload, idx);
			if (filter.ContainsVector(idx.ptField)) {
				payload.Set(idx.ptField, Variant{floatVectorsHolder.Get(i)});
			}
		}
	}
}

template void FloatVectorsHolderMap::Add(const NamespaceImpl&, LocalQueryResults::Iterator, LocalQueryResults::Iterator,
										 const FieldsFilter&);
template void FloatVectorsHolderMap::Add(const NamespaceImpl&, JoinPreResult::Values::Iterator, JoinPreResult::Values::Iterator,
										 const FieldsFilter&);

}  // namespace reindexer
