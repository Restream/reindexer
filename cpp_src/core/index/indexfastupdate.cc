#include "core/index/indexfastupdate.h"
#include "core/formatters/namespacesname_fmt.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "tools/logger.h"

namespace reindexer {
bool IndexFastUpdate::Try(NamespaceImpl& ns, const IndexDef& from, const IndexDef& to) {
	if (!RelaxedEqual(from, to)) {
		return false;
	}

	auto indexDiff = from.Compare(to);
	logFmt(LogInfo, "[{}]:{} Start fast update index '{}'", ns.name_, ns.wal_.GetServer(), from.Name());
	if (needRecreateIndex(indexDiff)) {
		logFmt(LogTrace, "[{}]:{} Index '{}' will be created anew without changing the payloads of the items.", ns.name_,
			   ns.wal_.GetServer(), from.Name());

		ns.verifyUpdateIndex(to);

		const auto idxNo = ns.indexesNames_.find(from.Name())->second;
		auto& index = ns.indexes_[idxNo];
		const auto& fields = index->Fields();
		const auto isSparse = index->Opts().IsSparse();
		if (isSparse && fields.getJsonPathsLength() != 1) {
			assertrx_dbg(false);  // Currently we do not support sparse indexes with multiple jsonpaths
			logFmt(LogWarning,
				   "[{}]:{} Index '{}' was not updated using a fast strategy: got {} jsonpaths in sparse index, but exactly 1 jsonpath was "
				   "expected",
				   ns.name_, ns.wal_.GetServer(), from.Name(), fields.getJsonPathsLength());
			return false;
		}

		VariantArray keys, resKeys;
		auto newIndex = Index::New(to, PayloadType(index->GetPayloadType()), FieldsSet{fields}, ns.config_.cacheConfig, ns.itemsCount());
		const auto isComposite = IsComposite(index->Type());
		for (size_t rowId = 0; rowId < ns.items_.size(); ++rowId) {
			const auto& item = ns.items_[rowId];
			if (item.IsFree()) {
				continue;
			}

			if (isComposite) {
				keys.Clear();
				keys.emplace_back(item);
			} else if (isSparse) {
				try {
					ConstPayload(ns.payloadType_, item).GetByJsonPath(fields.getJsonPath(0), ns.tagsMatcher_, keys, index->KeyType());
				} catch (const std::exception& e) {
					logFmt(LogInfo, "[{}]:{} Unable to index sparse value during index fast update (index name: '{}'): '{}'", ns.name_,
						   ns.wal_.GetServer(), index->Name(), e.what());
					keys.resize(0);
				}
			} else {
				ConstPayload(ns.payloadType_, item).Get(idxNo, keys);
			}

			bool needClearCacheUnused = false;
			newIndex->Upsert(resKeys, keys, rowId, needClearCacheUnused);
		}

		auto indexesCacheCleaner{ns.GetIndexesCacheCleaner()};
		indexesCacheCleaner.Add(*index);

		index = std::move(newIndex);

		ns.indexOptimizer_.UpdateSortedIdxCount(ns.indexes_);
		ns.markUpdated(IndexOptimization::Full);
	} else if (indexDiff.AnyOfIsDifferent(FloatVectorIndexOpts::Diff::Embedding, FloatVectorIndexOpts::Diff::Radius,
										  IndexOpts::ParamsDiff::Config)) {
		logFmt(LogTrace, "[{}]:{} Only the options will be updated for the index '{}'.", ns.name_, ns.wal_.GetServer(), from.Name());
		const auto idx = ns.getIndexByName(to.Name());
		auto& index = *ns.indexes_[idx].get();

		if (indexDiff.AnyOfIsDifferent(FloatVectorIndexOpts::Diff::Embedding)) {
			ns.verifyUpsertEmbedder("update", to);
			PayloadFieldType f(ns.name_.ToLower(), index, to, ns.embeddersCache_, ns.enablePerfCounters_);
			f.SetOffset(ns.payloadType_.Field(idx).Offset());
			ns.payloadType_.Replace(idx, std::move(f));
		}

		index.SetOpts(to.Opts());
		index.ClearCache();
		ns.clearNamespaceCaches();
	} else {
		logFmt(LogWarning,
			   "[{}]:{} Index '{}' was not updated using a fast strategy for an unknown reason. Index will be updated completely.",
			   ns.name_, ns.wal_.GetServer(), from.Name());
		return false;
	}

	logFmt(LogInfo, "[{}]:{} Index '{}' successfully updated using a fast strategy", ns.name_, ns.wal_.GetServer(), from.Name());
	return true;
}

bool IndexFastUpdate::needRecreateIndex(auto indexDiff) noexcept {
	return indexDiff.AnyOfIsDifferent(IndexDef::Diff::IndexType, IndexOpts::OptsDiff::kIndexOptDense,
									  IndexOpts::OptsDiff::kIndexOptNoColumn, IndexOpts::ParamsDiff::CollateOpts);
}

bool IndexFastUpdate::RelaxedEqual(const IndexDef& from, const IndexDef& to) {
	if (!isLegalTypeTransform(from.IndexType(), to.IndexType())) {
		return false;
	}
	return from.Compare(to)
		.Skip(IndexDef::Diff::IndexType)
		.Skip(IndexOpts::OptsDiff::kIndexOptDense)
		.Skip(IndexOpts::OptsDiff::kIndexOptNoColumn)
		.Skip(IndexOpts::ParamsDiff::CollateOpts)
		.Skip(IndexOpts::ParamsDiff::Config)
		.Skip(FloatVectorIndexOpts::Diff::Embedding)
		.Skip(FloatVectorIndexOpts::Diff::Radius)
		.Equal();
}

bool IndexFastUpdate::isLegalTypeTransform(IndexType from, IndexType to) noexcept {
	return std::find_if(kTransforms.begin(), kTransforms.end(), [from, to](const auto& set) {
			   return set.find(from) != set.end() && set.find(to) != set.end();
		   }) != kTransforms.end();
}
const std::vector<fast_hash_set<IndexType>> IndexFastUpdate::kTransforms = {
	{IndexType::IndexIntBTree, IndexType::IndexIntHash, IndexType::IndexIntStore},
	{IndexType::IndexInt64BTree, IndexType::IndexInt64Hash, IndexType::IndexInt64Store},
	{IndexType::IndexStrBTree, IndexType::IndexStrHash, IndexType::IndexStrStore},
	{IndexType::IndexDoubleStore, IndexType::IndexDoubleBTree},
	{IndexType::IndexUuidStore, IndexType::IndexUuidHash},
	{IndexType::IndexFastFT},
	{IndexType::IndexCompositeFastFT},
	{IndexType::IndexHnsw},
	{IndexType::IndexVectorBruteforce},
	{IndexType::IndexIvf},
	{IndexType::IndexTtl},
};
}  // namespace reindexer
