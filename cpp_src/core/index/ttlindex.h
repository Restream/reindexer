#pragma once

#include "indexordered.h"

namespace reindexer {

/// TTL index based on ordered int64 index.
/// Stores UNIX timestamps, items expire after:
/// IndexDef::expireAfter_ (seconds) + item value itself
template <typename T>
class TtlIndex : public IndexOrdered<T> {
public:
	TtlIndex(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);
	TtlIndex(const TtlIndex<T> &other);
	int64_t GetTTLValue() const override;
	Index *Clone() override;

private:
	/// Expiration value in seconds.
	int64_t expireAfter_ = 0;
};

Index *TtlIndex_New(const IndexDef &idef, const PayloadType payloadType, const FieldsSet &fields);

}  // namespace reindexer
