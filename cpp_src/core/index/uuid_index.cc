#include "uuid_index.h"

namespace reindexer {

void UuidIndex::Upsert(VariantArray &result, const VariantArray &keys, IdType id, bool &clearCache) {
	if (keys.empty() && !Opts().IsArray()) {
		result = {Upsert(Variant{Uuid{}}, id, clearCache)};
	} else {
		return IndexStore<Uuid>::Upsert(result, keys, id, clearCache);
	}
}

std::unique_ptr<Index> IndexUuid_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields) {
	return std::unique_ptr<Index>{new UuidIndex{idef, std::move(payloadType), fields}};
}

}  // namespace reindexer
