#pragma once

#include "indexordered.h"

namespace reindexer {

/// TTL index based on ordered int64 index.
/// Stores UNIX timestamps, items expire after:
/// IndexDef::expireAfter_ (seconds) + item value itself
template <typename T>
class TtlIndex : public IndexOrdered<T> {
public:
	TtlIndex(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);
	TtlIndex(const TtlIndex<T> &other);
	int64_t GetTTLValue() const override;
	std::unique_ptr<Index> Clone() override;
	void UpdateExpireAfter(int64_t v);

private:
	/// Expiration value in seconds.
	int64_t expireAfter_ = 0;
};

std::unique_ptr<Index> TtlIndex_New(const IndexDef &idef, PayloadType payloadType, const FieldsSet &fields);
void UpdateExpireAfter(Index *i, int64_t v);

}  // namespace reindexer
