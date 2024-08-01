#include "ttlindex.h"

namespace reindexer {

void UpdateExpireAfter(Index* i, int64_t v) {
	auto ttlIndexEntryPlain = dynamic_cast<TtlIndex<number_map<int64_t, Index::KeyEntryPlain>>*>(i);
	if (ttlIndexEntryPlain == nullptr) {
		auto ttlIndex = dynamic_cast<TtlIndex<number_map<int64_t, Index::KeyEntry>>*>(i);
		if (ttlIndex == nullptr) {
			throw Error(errLogic, "Incorrect ttl index type");
		}
		ttlIndex->UpdateExpireAfter(v);
	} else {
		ttlIndexEntryPlain->UpdateExpireAfter(v);
	}
}

std::unique_ptr<Index> TtlIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									const NamespaceCacheConfigData& cacheCfg) {
	if (idef.opts_.IsPK() || idef.opts_.IsDense()) {
		return std::make_unique<TtlIndex<number_map<int64_t, Index::KeyEntryPlain>>>(idef, std::move(payloadType), std::move(fields),
																					 cacheCfg);
	}
	return std::make_unique<TtlIndex<number_map<int64_t, Index::KeyEntry>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}

}  // namespace reindexer
