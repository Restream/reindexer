#pragma once

#include "core/index/indexunordered.h"
#include "core/keyvalue/uuid.h"

namespace reindexer {

class UuidIndex : public IndexUnordered<unordered_uuid_map<Index::KeyEntryPlain>> {
	using Base = IndexUnordered<unordered_uuid_map<Index::KeyEntryPlain>>;

public:
	UuidIndex(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields) : Base{idef, std::move(payloadType), fields} {}
	std::unique_ptr<Index> Clone() const override { return std::unique_ptr<Index>{new UuidIndex{*this}}; }
	using Base::Upsert;
	void Upsert(VariantArray& result, const VariantArray& keys, IdType id, bool& clearCache) override;	// TODO delete this after #1353
	bool IsUuid() const noexcept override { return true; }
};

std::unique_ptr<Index> IndexUuid_New(const IndexDef& idef, PayloadType payloadType, const FieldsSet& fields);

}  // namespace reindexer
