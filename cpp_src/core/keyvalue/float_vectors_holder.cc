#include "float_vectors_holder.h"
#include "core/id_type.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/keyvalue/float_vectors_keeper.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/joins/items_processor.h"
#include "core/queryresults/itemref.h"
#include "core/queryresults/localqueryresults.h"

namespace reindexer {

namespace {
constexpr uint32_t kLimitNumberProcessedElements = 5000;

void checkPayloadVectorField([[maybe_unused]] const Payload& payload, [[maybe_unused]] const ConstPayload& nsPayload,
							 [[maybe_unused]] FloatVectorIndexData idx) {
#ifdef RX_WITH_STDLIB_DEBUG
	VariantArray buffer;
	payload.Get(idx.ptField, buffer);
	assertrx_dbg(buffer.size() == nsPayload.GetFieldLen(idx.ptField));
	for (const auto& v : buffer) {
		assertrx_dbg(v.Type().Is<KeyValueType::FloatVector>());
		assertrx_dbg(ConstFloatVectorView(v).IsStrippedOrEmpty());
	}
#endif	// RX_WITH_STDLIB_DEBUG
}
}  // namespace

struct FloatVectorsHolderMap::FloatVectorIndexInfo {
	FloatVectorIndexInfo() = delete;
	FloatVectorIndexInfo(const FloatVectorIndexData& i, FloatVectorsKeeper::KeeperTag&& t) noexcept : index(i), tag(std::move(t)) {}
	FloatVectorIndexInfo(FloatVectorIndexInfo&&) noexcept = default;
	FloatVectorIndexInfo(const FloatVectorIndexInfo&) noexcept = delete;
	FloatVectorIndexInfo& operator=(const FloatVectorIndexInfo&) noexcept = delete;
	FloatVectorIndexInfo& operator=(FloatVectorIndexInfo&&) noexcept = default;

	FloatVectorIndexData index;
	FloatVectorsKeeper::KeeperTag tag;
};

template <typename It>
void FloatVectorsHolderMap::Add(const NamespaceImpl& ns, const It& it, const It& end, const FieldsFilter& filter) {
	if (!ns.haveFloatVectorsIndexes() || !filter.HasVectors()) {
		return;
	}

	auto nsIt = std::ranges::find_if(vectorsByNs_, [&ns](const auto& item) noexcept { return item.ns == &ns; });
	if (nsIt == vectorsByNs_.end()) {
		auto vectIndexes = ns.getVectorIndexes();
		const auto indexCnt = vectIndexes.size();
		auto indexes = std::make_unique<std::optional<FloatVectorIndexInfo>[]>(indexCnt);
		for (size_t i = 0; i < indexCnt; ++i) {
			indexes[i].emplace(vectIndexes[i], vectIndexes[i].ptr->GetKeeper().Register());
		}
		vectorsByNs_.emplace_back(NsFloatVectorIndexes{.ns = &ns, .indexesCnt = indexCnt, .indexes = std::move(indexes)});
		nsIt = std::prev(vectorsByNs_.end());
	}

	std::vector<FloatVectorId> ids;
	std::vector<ConstFloatVectorView> vectors;
	ids.reserve(kLimitNumberProcessedElements);
	vectors.reserve(kLimitNumberProcessedElements);

	for (size_t i = 0; i < nsIt->indexesCnt; ++i) {
		assertrx_dbg(nsIt->indexes[i].has_value());
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		auto& info = *(nsIt->indexes[i]);
		if (filter.ContainsVector(info.index.ptField)) {
			add(ns, info, it, end, ids, vectors);
		}
	}
}

FloatVectorsHolderMap::FloatVectorsHolderMap() noexcept = default;
FloatVectorsHolderMap::FloatVectorsHolderMap(FloatVectorsHolderMap&&) noexcept = default;
FloatVectorsHolderMap& FloatVectorsHolderMap::operator=(FloatVectorsHolderMap&&) noexcept = default;
FloatVectorsHolderMap::~FloatVectorsHolderMap() = default;

bool FloatVectorsHolderMap::Empty() const noexcept {
	return !std::ranges::any_of(vectorsByNs_, [](const auto& info) { return info.indexesCnt != 0; });
}

template <typename It>
void FloatVectorsHolderMap::updatePayload(const NamespaceImpl& ns, const FloatVectorIndexData& index, It it, const It& end,
										  std::span<ConstFloatVectorView> vectors) {
	const auto field = index.ptField;
	size_t idx = 0;
	for (; it != end; ++it) {
		ItemRef& itemRef = it.GetItemRef();
		const auto id = itemRef.Id();
		if (id.IsValid()) {
			itemRef.Value().Clone();
			Payload payload{ns.payloadType_, itemRef.Value()};
			checkPayloadVectorField(payload, ConstPayload{ns.payloadType_, ns.items_[id]}, index);
			const auto count = payload.GetFieldLen(index.ptField);
			assertrx_throw(idx + count <= vectors.size());
			for (unsigned i = 0; i < count; ++i) {
				const auto& view = vectors[idx++];
				if (!view.IsEmpty()) {
					payload.Set(field, i, Variant{view});
				}
			}
		}
	}
}

void FloatVectorsKeeper::getFloatVectors(const KeeperTag& tag, std::span<FloatVectorId> ids, std::vector<ConstFloatVectorView>& vectorsData,
										 auto&& floatVectorGetter) {
	vectorsData.resize(0);
	vectorsData.reserve(ids.size());

	auto ownerID = tag.GetID();

	lock_guard lock(lock_);

	for (auto id : ids) {
		auto [itVector, newAdded] = map_.try_emplace(id, queue_.end());
		if (newAdded) {
			try {
				itVector->second = queue_.emplace(std::next(tag.Get()), ownerID, floatVectorGetter(id, index_), itVector);
			} catch (...) {
				assertrx_dbg(false);
				map_.erase(itVector);
				throw;
			}
		} else {
			// update owner
			if (itVector->second->owner < ownerID) {
				queue_.splice(std::next(tag.Get()), queue_, itVector->second);
				itVector->second = std::next(tag.Get());
				itVector->second->owner = ownerID;
				itVector->second->deleted = false;
			}
		}
		vectorsData.emplace_back(itVector->second->vect);
	}
}

template <typename It>
void FloatVectorsHolderMap::add(const NamespaceImpl& ns, const FloatVectorIndexInfo& indexInfo, It it, const It& end,
								std::vector<FloatVectorId>& ids, std::vector<ConstFloatVectorView>& vectors) {
	const auto& tag = indexInfo.tag;
	const auto& index = indexInfo.index;
	auto& keeper = index.ptr->GetKeeper();

	ids.clear();
	vectors.clear();

	auto floatVectorGetter = [&ns](FloatVectorId id, const FloatVectorIndex& index) { return FloatVector{ns.getFloatVector(id, index)}; };

	It itCurr = it;
	for (; it != end; ++it) {
		ItemRef& itemRef = it.GetItemRef();
		const auto id = itemRef.Id();
		if (id.IsValid()) {
			ConstPayload payload{ns.payloadType_, ns.items_[id]};
			for (unsigned i = 0, count = payload.GetFieldLen(index.ptField); i < count; ++i) {
				ids.emplace_back(id, i);
			}
			if (ids.size() >= kLimitNumberProcessedElements) {
				auto itNext = it;
				++itNext;
				keeper.getFloatVectors(tag, ids, vectors, floatVectorGetter);
				assertrx_throw(ids.size() == vectors.size());
				updatePayload(ns, index, itCurr, itNext, vectors);

				ids.clear();
				vectors.resize(0);
				itCurr = itNext;
			}
		}
	}

	if (!ids.empty()) {
		keeper.getFloatVectors(tag, ids, vectors, floatVectorGetter);
		assertrx_throw(ids.size() == vectors.size());
		updatePayload(ns, index, itCurr, end, vectors);

		ids.clear();
		vectors.clear();
	}
}

template void FloatVectorsHolderMap::Add(const NamespaceImpl&, const LocalQueryResults::Iterator&, const LocalQueryResults::Iterator&,
										 const FieldsFilter&);
template void FloatVectorsHolderMap::Add(const NamespaceImpl&, const joins::PreSelect::Values::Iterator&,
										 const joins::PreSelect::Values::Iterator&, const FieldsFilter&);
}  // namespace reindexer
