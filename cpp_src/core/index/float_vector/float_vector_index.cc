#include "float_vector_index.h"
#include "core/rdxcontext.h"
#include "tools/assertrx.h"
#include "tools/logger.h"

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
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	if (emptyValues_.Unsorted().Find(id)) {
		emptyValues_.Unsorted().Erase(id);	// ignore result
		memStat_.uniqKeysCount = emptyValues_.Unsorted().IsEmpty() ? 0 : 1;
	} else {
		Delete(keys[0], id, stringsHolder, clearCache);
	}
}

SelectKeyResults FloatVectorIndex::SelectKey(const VariantArray&, CondType condition, SortType sortId, SelectOpts opts,
											 const BaseFunctionCtx::Ptr&, const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	switch (condition) {
		case CondEmpty: {
			if (opts.forceComparator) {
				throw Error(errLogic, "FloatVectorIndex({}): Comparator for 'IS NULL' vector index condition is not implemented", Name());
			}
			SelectKeyResult res;
			res.emplace_back(emptyValues_, sortId);
			return SelectKeyResults(std::move(res));
		}
		case CondAny:
			return ComparatorIndexed<FloatVector>{Name(), condition, {}, nullptr, IsArray_False, false, payloadType_, Fields()};
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondGe:
		case CondLe:
		case CondRange:
		case CondGt:
		case CondLt:
		case CondLike:
		case CondDWithin:
		case CondKnn:
			throw Error(errQueryExec, "{} query on index '{}'", CondTypeToStrShort(condition), name_);
	}
	return {};
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
		emptyValues_.Unsorted().Add(id, IdSet::Auto, sortedIdxCount_);	// ignore result
		memStat_.uniqKeysCount = 1;
		return Variant{ConstFloatVectorView{}};
	}
	if (vect.Dimension() != Dimension()) {
		throw Error{errNotValid, "Attempt to upsert vector of dimension {} in a float vector index of dimension {}",
					vect.Dimension().Value(), Dimension().Value()};
	}
	return upsert(vect, id, clearCache);
}

SelectKeyResult FloatVectorIndex::Select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx, const RdxContext& rdxCtx) const {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (key.IsEmpty()) {
		throw Error{errNotValid, "Attempt to search knn by empty float vector"};
	}
	if (key.Dimension() != Dimension()) {
		throw Error{errNotValid, "Attempt to search knn by float vector of dimension {} in float vector index of dimension {}",
					key.Dimension().Value(), Dimension().Value()};
	}
	return select(key, p, ctx);
}

void FloatVectorIndex::Commit() {
	emptyValues_.Unsorted().Commit();
	logFmt(LogTrace, "FloatVectorIndex::Commit ({}) {} empty", name_, emptyValues_.Unsorted().size());
}

IndexMemStat FloatVectorIndex::GetMemStat(const RdxContext&) noexcept {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	memStat_.indexingStructSize = emptyValues_.Unsorted().Size() * sizeof(IdType);
	return memStat_;
}

FloatVector FloatVectorIndex::GetFloatVector(IdType id) const {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	if (emptyValues_.Unsorted().Find(id)) {
		return {};
	}

	return getFloatVector(id);
}

ConstFloatVectorView FloatVectorIndex::GetFloatVectorView(IdType id) const {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	if (emptyValues_.Unsorted().Find(id)) {
		return {};
	}

	return getFloatVectorView(id);
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
