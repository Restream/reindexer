#include "core/index/indexfastupdate.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "tools/logger.h"

namespace reindexer {
bool IndexFastUpdate::Try(NamespaceImpl& ns, const IndexDef& from, const IndexDef& to) {
	if (RelaxedEqual(from, to)) {
		logFmt(LogInfo, "[{}]:{} Start fast update index '{}'", ns.name_, ns.serverId_, from.name_);

		const auto idxNo = ns.indexesNames_.find(from.name_)->second;
		auto& index = ns.indexes_[idxNo];
		auto newIndex = Index::New(to, PayloadType(index->GetPayloadType()), FieldsSet{index->Fields()}, ns.config_.cacheConfig);
		VariantArray keys, resKeys;
		for (size_t rowId = 0; rowId < ns.items_.size(); ++rowId) {
			if (ns.items_[rowId].IsFree()) {
				continue;
			}

			bool needClearCache = false;
			ConstPayload(ns.payloadType_, ns.items_[rowId]).Get(idxNo, keys);
			newIndex->Upsert(resKeys, keys, rowId, needClearCache);
		}
		if (index->IsOrdered()) {
			auto indexesCacheCleaner{ns.GetIndexesCacheCleaner()};
			indexesCacheCleaner.Add(index->SortId());
		}

		index = std::move(newIndex);

		ns.updateSortedIdxCount();
		ns.markUpdated(IndexOptimization::Full);

		logFmt(LogInfo, "[{}]:{} Index '{}' successfully updated using a fast strategy", ns.name_, ns.serverId_, from.name_);

		return true;
	}
	return false;
}

bool IndexFastUpdate::RelaxedEqual(const IndexDef& from, const IndexDef& to) noexcept {
	if (!isLegalTypeTransform(from.Type(), to.Type())) {
		return false;
	}
	auto comparisonIndex = from;
	comparisonIndex.indexType_ = to.indexType_;
	comparisonIndex.opts_.Dense(to.opts_.IsDense());
	comparisonIndex.opts_.SetCollateMode(to.opts_.GetCollateMode());
	comparisonIndex.opts_.SetCollateSortOrder(to.opts_.GetCollateSortOrder());
	return comparisonIndex.IsEqual(to, IndexComparison::WithConfig);
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
};
}  // namespace reindexer