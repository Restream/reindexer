#include "float_vector_index.h"
#include "core/embedding/embedder.h"
#include "core/idset/idset.h"
#include "core/index/float_vector/float_vector_id.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/float_vectors_keeper.h"
#include "core/rdxcontext.h"
#include "knn_raw_result.h"
#include "tools/assertrx.h"
#include "tools/logger.h"

namespace reindexer {

FloatVectorIndex::FloatVectorIndex(const FloatVectorIndex& other)
	: Index(other),
	  memStat_(other.memStat_),
	  emptyKeys_(other.emptyKeys_),
	  emptyVectors_(other.emptyVectors_),
	  emptyVectorsCounters_(other.emptyVectorsCounters_),
	  emptyValues_(other.emptyValues_),
	  keeper_(FloatVectorsKeeper::Create(*this)),
	  metric_(other.metric_) {}

FloatVectorIndex::FloatVectorIndex(const IndexDef& idef, PayloadType&& pt, FieldsSet&& fields)
	: Index{idef, std::move(pt), std::move(fields)}, keeper_(FloatVectorsKeeper::Create(*this)) {
	assertrx_dbg(idef.Opts().IsFloatVector());
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

void FloatVectorIndex::Delete(const Variant&, IdType, reindexer::MustExist, StringsHolder&, bool&) { assertrx_dbg(0 && "not implemented"); }

void FloatVectorIndex::Delete(const VariantArray& keys, IdType rowId, MustExist mustExist, StringsHolder&, bool&) {
	const auto count = keys.size();
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	if (const auto itEmptyValues = emptyValues_.find(rowId); itEmptyValues != emptyValues_.cend()) {
		assertrx_dbg(keys.empty());
		assertrx_dbg(emptyVectorsCounters_.find(rowId) == emptyVectorsCounters_.cend());
		emptyValues_.erase(itEmptyValues);
		std::ignore = emptyKeys_.Unsorted().Erase(rowId);
	} else if (const auto itEmptiesCount = emptyVectorsCounters_.find(rowId); itEmptiesCount == emptyVectorsCounters_.cend()) {
		for (unsigned i = 0; i < count; ++i) {
			const FloatVectorId id{rowId, i};
			keeper_->Remove(id);
			del(id, mustExist, IsLast(i == count - 1));
		}
	} else {
		auto& emptiesCount = itEmptiesCount->second;
		assertrx_dbg(emptiesCount > 0);
		for (unsigned i = 0; i < count; ++i) {
			const FloatVectorId id{rowId, i};
			keeper_->Remove(id);
			if (const auto itEmpty = emptyVectors_.find(id); itEmpty == emptyVectors_.cend()) {
				del(id, mustExist, IsLast(i == count - 1));
			} else if (emptiesCount == 1) {
				emptyVectors_.erase(itEmpty);
				emptyVectorsCounters_.erase(itEmptiesCount);
				std::ignore = emptyKeys_.Unsorted().Erase(rowId);
				for (++i; i < count; ++i) {
					keeper_->Remove({rowId, i});
					del({rowId, i}, mustExist, IsLast(i == count - 1));
				}
				return;
			} else {
				emptyVectors_.erase(itEmpty);
				--emptiesCount;
			}
		}
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
			res.emplace_back(emptyKeys_, sortId);
			return SelectKeyResults(std::move(res));
		}
		case CondAny:
			return ComparatorIndexed<FloatVector>{Name(),			condition,		  {},			nullptr,
												  Opts().IsArray(), IsDistinct_False, payloadType_, Fields()};
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
	if (keys.empty()) {
		const auto [it, inserted] = emptyValues_.insert(id);
		assertrx_dbg(inserted);
		try {
			std::ignore = emptyKeys_.Unsorted().Add(id, IdSetEditMode::Auto, sortedIdxCount_);
		} catch (...) {
			emptyValues_.erase(it);
			throw;
		}
	} else {
		for (unsigned i = 0, count = keys.size(); i < count; ++i) {
			result.emplace_back(Upsert(ConstFloatVectorView{keys[i]}, {id, i}, clearCache));
		}
	}
}

Variant FloatVectorIndex::Upsert(const Variant& key, IdType id, bool& clearCache) {
	return Upsert(ConstFloatVectorView{key}, {id, 0}, clearCache);
}

Variant FloatVectorIndex::Upsert(ConstFloatVectorView vect, FloatVectorId id, bool& clearCache) {
	using namespace std::string_view_literals;
	keeper_->Remove(id);
	if (vect.IsEmpty()) {
		// Do not lock empty values mutex here
		return upsertEmptyVectImpl(id);
	}
	checkVectorDims(vect, "upsert"sv);
	return upsert(vect, id, clearCache);
}

Variant FloatVectorIndex::UpsertConcurrent(const Variant& key, FloatVectorId id, bool& clearCache) {
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

bool FloatVectorIndex::RefreshCompositeKey(const Variant& /*key*/) noexcept {
	assertrx_dbg(0);
	return false;
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
	emptyKeys_.Unsorted().Commit();
	logFmt(LogTrace, "FloatVectorIndex::Commit ({}) {} empty", name_, emptyKeys_.Unsorted().Size());
}

IndexMemStat FloatVectorIndex::GetMemStat(const RdxContext&) const noexcept {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	auto res = memStat_;
	res.indexingStructSize =
		emptyVectors_.size() * sizeof(FloatVectorId) + emptyKeys_.Unsorted().Size() * sizeof(IdType);  // TODO calculate mem stat for empty
	res.vectorsKeeperSize = keeper_->GetMemStat();
	res.uniqKeysCount = emptyKeys_.Unsorted().IsEmpty() ? 0 : 1;
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

			fillEmbedMemStat(type.UpsertEmbedder(), res.upsertEmbedderStatus);
			fillEmbedMemStat(type.QueryEmbedder(), res.queryEmbedderStatus);
		}
	}

	return res;
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

ConstFloatVectorView FloatVectorIndex::getFloatVectorView(FloatVectorId id) const {
	// Intentionally don't lock emptyValuesInsertionMtx_ here - only upserts may be multithreaded
	assertrx_dbg(emptyValues_.find(id.RowId()) == emptyValues_.cend());
	if (const auto it = emptyVectors_.find(id); it != emptyVectors_.cend()) {
		return {};
	}
	return getFloatVectorViewImpl(id);
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

Variant FloatVectorIndex::upsertEmptyVectImpl(FloatVectorId id) {
	const auto [countersIt, newRowId] = emptyVectorsCounters_.try_emplace(id.RowId(), 0);
	if (newRowId) {
		std::ignore = emptyKeys_.Unsorted().Add(id.RowId(), IdSetEditMode::Auto, sortedIdxCount_);
	}
	try {
		emptyVectors_.emplace(id);
	} catch (...) {
		if (newRowId) {
			emptyVectorsCounters_.erase(countersIt);
			std::ignore = emptyKeys_.Unsorted().Erase(id.RowId());
		}
		throw;
	}
	++(countersIt->second);
	return Variant{ConstFloatVectorView{}};
}

void FloatVectorIndex::WriterBase::writePK(FloatVectorId fvId) {
	VariantArray pks = getPK_(fvId.RowId());
	if (!isCompositePK_) {
		ser_.PutVariant(pks[0]);
	} else {
		ser_.PutVarUint(pks.size());
		for (auto& v : pks) {
			ser_.PutVariant(v);
		}
	}
	if (isArray_) {
		ser_.PutVarUint(fvId.ArrayIndex());
	}
}

FloatVectorId FloatVectorIndex::LoaderBase::readPKEncodedData(void* destBuf, Serializer& ser, std::string_view name,
															  std::string_view idxType) {
	Variant key;
	if (!isCompositePK_) {
		key = ser.GetVariant();
	} else {
		VariantArray keyParts;
		const auto len = ser.GetVarUInt();
		if (!len) [[unlikely]] {
			throw Error(errLogic, "{}::LoadIndexCache:{}: serialized PK array is empty", idxType, name);
		}
		keyParts.reserve(len);
		for (size_t i = 0; i < len; ++i) {
			keyParts.emplace_back(ser.GetVariant());
		}
		key = Variant(keyParts);
	}
	const uint32_t arrIdx = isArray_ ? ser.GetVarUInt() : 0;
	const IdType itemID = getVectorData_(std::move(key), arrIdx, destBuf);
	if (!itemID.IsValid()) [[unlikely]] {
		throw Error(errLogic, "{}::LoadIndexCache:{}: unable to find indexed item with requested PK", idxType, name);
	}
	return {itemID, arrIdx};
}

}  // namespace reindexer
