#include "float_vector_index.h"
#include "tools/assertrx.h"

namespace reindexer {

FloatVectorIndex::FloatVectorIndex(const FloatVectorIndex& other)
	: Index(other), memStat_(other.memStat_), emptyValues_(other.emptyValues_), metric_(other.metric_) {}

FloatVectorIndex::FloatVectorIndex(const IndexDef& idef, PayloadType&& pt, FieldsSet&& fields)
	: Index{idef, std::move(pt), std::move(fields)} {
	assertrx_dbg(idef.Opts().IsFloatVector());
	assertrx_throw(!idef.Opts().IsArray());
	keyType_ = selectKeyType_ = KeyValueType::FloatVector{};
	memStat_.name = name_;
	metric_ = idef.Opts().FloatVector().Metric();
}

void FloatVectorIndex::Delete(const VariantArray& keys, IdType id, StringsHolder& stringsHolder, bool& clearCache) {
	assertrx_dbg(keys.size() == 1);
	// Intentionally do not locking emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	const auto it = emptyValues_.find(id);
	if (it == emptyValues_.end()) {
		Delete(keys[0], id, stringsHolder, clearCache);
	} else {
		emptyValues_.erase(it);
		memStat_.uniqKeysCount = emptyValues_.empty() ? 0 : 1;
	}
}

SelectKeyResults FloatVectorIndex::SelectKey(const VariantArray&, CondType, SortType, SelectOpts, const BaseFunctionCtx::Ptr&,
											 const RdxContext&) {
	throw_as_assert;
}

void FloatVectorIndex::Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) {
	assertrx_dbg(keys.size() == 1);
	result.emplace_back(Upsert(keys[0], id, clearCache));
}

Variant FloatVectorIndex::Upsert(const Variant& key, IdType id, bool& clearCache) {
	const ConstFloatVectorView vect{key};
	if (vect.IsEmpty()) {
		std::unique_lock lck(emptyValuesInsertionMtx_, std::defer_lock_t{});
		if (IsSupportMultithreadTransactions()) {
			lck.lock();
		}
		emptyValues_.insert(id);
		memStat_.uniqKeysCount = 1;
		return Variant{ConstFloatVectorView{}};
	}
	if (vect.Dimension() != Dimension()) {
		throw Error{errNotValid, "Attempt to upsert vector of dimension {} in a float vector index of dimension {}",
					vect.Dimension().Value(), Dimension().Value()};
	}
	return upsert(vect, id, clearCache);
}

SelectKeyResult FloatVectorIndex::Select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx) const {
	if (key.IsEmpty()) {
		throw Error{errNotValid, "Attempt to search knn by empty float vector"};
	}
	if (key.Dimension() != Dimension()) {
		throw Error{errNotValid, "Attempt to search knn by float vector of dimension {} in float vector index of dimension {}",
					key.Dimension().Value(), Dimension().Value()};
	}
	return select(key, p, ctx);
}

IndexMemStat FloatVectorIndex::GetMemStat(const RdxContext&) noexcept {
	// Intentionally do not locking emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	memStat_.indexingStructSize = size_t(float(emptyValues_.size()) / emptyValues_.load_factor()) * sizeof(IdType);
	return memStat_;
}

FloatVector FloatVectorIndex::GetFloatVector(IdType id) const {
	// Intentionally do not locking emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	const auto it = emptyValues_.find(id);
	if (it == emptyValues_.end()) {
		return getFloatVector(id);
	} else {
		return {};
	}
}

ConstFloatVectorView FloatVectorIndex::GetFloatVectorView(IdType id) const {
	// Intentionally do not locking emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	const auto it = emptyValues_.find(id);
	if (it == emptyValues_.end()) {
		return getFloatVectorView(id);
	} else {
		return {};
	}
}

void FloatVectorIndex::WriterBase::writePK(IdType id) {
	VariantArray pks = getPK_(id);
	if (!isCompositePK_) {
		ser_.PutVariant(pks[0]);
	} else {
		ser_.PutVarUint(pks.size());
		for (auto& v : pks) {
			ser_.PutVariant(v);
		}
	}
}

}  // namespace reindexer
