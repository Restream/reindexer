#pragma once

#include "core/index/indexunordered.h"

namespace reindexer {

class [[nodiscard]] UuidIndex : public IndexUnordered<unordered_uuid_map<Index::KeyEntryPlain>> {
	using Base = IndexUnordered<unordered_uuid_map<Index::KeyEntryPlain>>;

public:
	UuidIndex(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: Base{idef, std::move(payloadType), std::move(fields), cacheCfg} {}
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new UuidIndex(*this)); }
	using Base::Upsert;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) override;	// TODO delete this after #1353

private:
	UuidIndex(const UuidIndex&) = default;
};

std::unique_ptr<Index> IndexUuid_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									 const NamespaceCacheConfigData& cacheCfg);

}  // namespace reindexer
