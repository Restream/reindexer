#include "uuid_index.h"

namespace reindexer {

template <typename MapType>
void UuidIndex<MapType>::Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) {
	if (keys.empty() && !Index::Opts().IsArray()) {
		result = {Upsert(Variant{Uuid{}}, id, clearCache)};
	} else {
		return IndexStore<Uuid>::Upsert(result, keys, id, clearCache);
	}
}

std::unique_ptr<Index> IndexUuid_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									 const NamespaceCacheConfigData& cacheCfg) {
	if (idef.Opts().IsPK()) {
		return std::make_unique<UuidIndex<unordered_uuid_map<Index::KeyEntryPK>>>(idef, std::move(payloadType), std::move(fields),
																				  cacheCfg);
	}

	return std::make_unique<UuidIndex<unordered_uuid_map<Index::KeyEntryPlain>>>(idef, std::move(payloadType), std::move(fields), cacheCfg);
}

}  // namespace reindexer
