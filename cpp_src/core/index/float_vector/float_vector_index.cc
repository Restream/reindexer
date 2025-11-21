#include "float_vector_index.h"
#include "core/embedding/embedder.h"
#include "core/keyvalue/float_vectors_keeper.h"
#include "core/rdxcontext.h"
#include "knn_raw_result.h"
#include "tools/assertrx.h"
#include "tools/logger.h"

namespace reindexer {

FloatVectorIndex::FloatVectorIndex(const FloatVectorIndex& other)
	: Index(other),
	  memStat_(other.memStat_),
	  emptyValues_(other.emptyValues_),
	  keeper_(FloatVectorsKeeper::Create(*this)),
	  metric_(other.metric_) {}

FloatVectorIndex::FloatVectorIndex(const IndexDef& idef, PayloadType&& pt, FieldsSet&& fields)
	: Index{idef, std::move(pt), std::move(fields)}, keeper_(FloatVectorsKeeper::Create(*this)) {
	assertrx_dbg(idef.Opts().IsFloatVector());
	assertrx_throw(!idef.Opts().IsArray());
	keyType_ = selectKeyType_ = KeyValueType::FloatVector{};
	memStat_.name = name_;
	if (opts_.FloatVector().Embedding().has_value()) {
		auto embedding = opts_.FloatVector().Embedding().value();
		if (embedding.upsertEmbedder.has_value()) {
			memStat_.upsertEmbedderStatus.emplace();
		}
		if (embedding.queryEmbedder.has_value()) {
			memStat_.queryEmbedderStatus.emplace();
		}
	}
	metric_ = idef.Opts().FloatVector().Metric();
}

void FloatVectorIndex::Delete(const VariantArray& keys, IdType id, MustExist mustExist, StringsHolder& stringsHolder, bool& clearCache) {
	assertrx_dbg(keys.size() == 1);
	keeper_->Remove(id);
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	if (emptyValues_.Unsorted().Find(id)) {
		std::ignore = emptyValues_.Unsorted().Erase(id);
		memStat_.uniqKeysCount = emptyValues_.Unsorted().IsEmpty() ? 0 : 1;
	} else {
		Delete(keys[0], id, mustExist, stringsHolder, clearCache);
	}
}

SelectKeyResults FloatVectorIndex::SelectKey(const VariantArray&, CondType condition, SortType sortId, const SelectContext& selectCtx,
											 const RdxContext& rdxCtx) {
	const auto indexWard(rdxCtx.BeforeIndexWork());
	if (selectCtx.opts.distinct) [[unlikely]] {
		throw Error(errParams, "FloatVectorIndex({}): DISTINCT is not supported for vector indexes", Index::Name());
	}
	switch (condition) {
		case CondEmpty: {
			if (selectCtx.opts.forceComparator) [[unlikely]] {
				throw Error(errLogic, "FloatVectorIndex({}): Comparator for 'IS NULL' vector index condition is not implemented", Name());
			}
			SelectKeyResult res;
			res.emplace_back(emptyValues_, sortId);
			return SelectKeyResults(std::move(res));
		}
		case CondAny:
			return ComparatorIndexed<FloatVector>{Name(), condition, {}, nullptr, IsArray_False, IsDistinct_False, payloadType_, Fields()};
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
	using namespace std::string_view_literals;
	keeper_->Remove(id);
	const ConstFloatVectorView vect{key};
	if (vect.IsEmpty()) {
		// Do not lock empty values mutex here
		return upsertEmptyVectImpl(id);
	}
	checkVectorDims(vect, "upsert"sv);
	return upsert(vect, id, clearCache);
}

Variant FloatVectorIndex::UpsertConcurrent(const Variant& key, IdType id, bool& clearCache) {
	using namespace std::string_view_literals;
	if (!IsSupportMultithreadTransactions()) {
		throw Error(errLogic, "Index {} does not support concurrent upsertions"sv, Name());
	}
	keeper_->Remove(id);
	const ConstFloatVectorView vect{key};
	if (vect.IsEmpty()) {
		unique_lock lck(emptyValuesInsertionMtx_);
		return upsertEmptyVectImpl(id);
	}
	checkVectorDims(vect, "upsert"sv);
	return upsertConcurrent(vect, id, clearCache);
}

SelectKeyResult FloatVectorIndex::Select(ConstFloatVectorView key, const KnnSearchParams& p, KnnCtx& ctx, const RdxContext& rdxCtx) const {
	checkForSelect(key);
	const auto indexWard(rdxCtx.BeforeIndexWork());
	return select(key, p, ctx);
}

KnnRawResult FloatVectorIndex::SelectRaw(ConstFloatVectorView key, const KnnSearchParams& p, const RdxContext& rdxCtx) const {
	checkForSelect(key);
	const auto indexWard(rdxCtx.BeforeIndexWork());
	return selectRaw(key, p);
}

void FloatVectorIndex::Commit() {
	emptyValues_.Unsorted().Commit();
	logFmt(LogTrace, "FloatVectorIndex::Commit ({}) {} empty", name_, emptyValues_.Unsorted().size());
}

IndexMemStat FloatVectorIndex::GetMemStat(const RdxContext&) noexcept {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	memStat_.indexingStructSize = emptyValues_.Unsorted().Size() * sizeof(IdType);
	memStat_.vectorsKeeperSize = keeper_->GetMemStat();
	if (opts_.FloatVector().Embedding().has_value()) {
		int fieldNo = 0;
		if (payloadType_.FieldByName(name_, fieldNo)) {
			const auto& type = payloadType_.Field(fieldNo);

			auto fillEmbedMemStat = [](auto&& embedder, auto& memStatEmbedField) {
				if (embedder && memStatEmbedField) {
					memStatEmbedField->lastError = embedder->GetLastError();
					memStatEmbedField->lastRequestResult = embedder->GetLastStatus();
				}
			};

			fillEmbedMemStat(type.UpsertEmbedder(), memStat_.upsertEmbedderStatus);
			fillEmbedMemStat(type.QueryEmbedder(), memStat_.queryEmbedderStatus);
		}
	}

	return memStat_;
}

namespace {
EmbedderPerfStat GetEmbedderPerfStat(const std::shared_ptr<const EmbedderBase>& embedder, std::string_view tag) {
	if (!embedder) {
		return {};
	}
	return embedder->GetPerfStat(tag);
}
}  // namespace

template <typename T>
void FloatVectorIndex::checkAndExecFuncOnEmbeders(T fun) {
	if (opts_.FloatVector().Embedding().has_value()) {
		int fieldNo = 0;
		if (payloadType_.FieldByName(name_, fieldNo)) {
			const auto& type = payloadType_.Field(fieldNo);
			auto upsertEmbeder = type.UpsertEmbedder();
			if (upsertEmbeder) {
				fun(upsertEmbeder);
			}
			auto queryEmbeder = type.QueryEmbedder();
			if (queryEmbeder) {
				fun(queryEmbeder);
			}
		}
	}
}

void FloatVectorIndex::ResetIndexPerfStat() {
	Index::ResetIndexPerfStat();
	checkAndExecFuncOnEmbeders([](const auto& v) { v->ResetPerfStat(); });
}

void FloatVectorIndex::EnablePerfStat(bool enable) {
	checkAndExecFuncOnEmbeders([enable](const auto& v) { v->EnablePerfStat(enable); });
}

void FloatVectorIndex::SetOpts(const IndexOpts& opts) {
	Index::SetOpts(opts);
	auto embedding = opts.FloatVector().Embedding();
	if (embedding.has_value()) {
		auto check = [](const std::optional<FloatVectorIndexOpts::EmbedderOpts>& embedder, std::optional<EmbedderStatus>& status) {
			if (embedder.has_value()) {
				if (!status.has_value()) {
					status.emplace();
				}
			} else {
				if (status.has_value()) {
					status.reset();
				}
			}
		};
		check(embedding->upsertEmbedder, memStat_.upsertEmbedderStatus);
		check(embedding->queryEmbedder, memStat_.queryEmbedderStatus);
	}
}

IndexPerfStat FloatVectorIndex::GetIndexPerfStat() {
	IndexPerfStat stat(name_, selectPerfCounter_.Get<PerfStat>(), commitPerfCounter_.Get<PerfStat>());
	if (!opts_.FloatVector().Embedding().has_value()) {
		return stat;
	}
	auto embedding = opts_.FloatVector().Embedding().value();  // NOLINT(bugprone-unchecked-optional-access)
	if (embedding.upsertEmbedder.has_value() || embedding.queryEmbedder.has_value()) {
		int fieldNo = 0;
		if (payloadType_.FieldByName(name_, fieldNo)) {
			const auto& type = payloadType_.Field(fieldNo);
			if (embedding.upsertEmbedder.has_value()) {
				stat.upsertEmbedder = GetEmbedderPerfStat(type.UpsertEmbedder(), embedding.upsertEmbedder.value().cacheTag);
			}
			if (embedding.queryEmbedder.has_value()) {
				stat.queryEmbedder = GetEmbedderPerfStat(type.QueryEmbedder(), embedding.queryEmbedder.value().cacheTag);
			}
		}
	}
	return stat;
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

void FloatVectorIndex::checkVectorDims(ConstFloatVectorView vect, std::string_view operation) const {
	using namespace std::string_view_literals;
	if (vect.Dimension() != Dimension()) {
		throw Error{errNotValid, "Attempt to {} vector of dimension {} in a float vector index of dimension {}"sv, operation,
					vect.Dimension().Value(), Dimension().Value()};
	}
}

void FloatVectorIndex::checkForSelect(ConstFloatVectorView key) const {
	using namespace std::string_view_literals;
	if (key.IsEmpty()) {
		throw Error{errNotValid, "Attempt to search knn by empty float vector"sv};
	}
	checkVectorDims(key, "search"sv);
}

Variant FloatVectorIndex::upsertEmptyVectImpl(IdType id) {
	std::ignore = emptyValues_.Unsorted().Add(id, IdSet::Auto, sortedIdxCount_);
	memStat_.uniqKeysCount = 1;
	return Variant{ConstFloatVectorView{}};
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
