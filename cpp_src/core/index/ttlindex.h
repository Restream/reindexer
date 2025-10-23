#pragma once

#include "indexordered.h"

namespace reindexer {

/// TTL index based on ordered int64 index.
/// Stores UNIX timestamps, items expire after:
/// IndexDef::expireAfter_ (seconds) + item value itself
template <typename T>
class [[nodiscard]] TtlIndex : public IndexOrdered<T> {
public:
	TtlIndex(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields, const NamespaceCacheConfigData& cacheCfg)
		: IndexOrdered<T>(idef, std::move(payloadType), std::move(fields), cacheCfg), expireAfter_(idef.ExpireAfter()) {}
	int64_t GetTTLValue() const noexcept override { return expireAfter_; }
	std::unique_ptr<Index> Clone(size_t /*newCapacity*/) const override { return std::unique_ptr<Index>(new TtlIndex<T>(*this)); }
	void UpdateExpireAfter(int64_t v) noexcept { expireAfter_ = v; }

private:
	TtlIndex(const TtlIndex<T>& other) = default;

	/// Expiration value in seconds.
	int64_t expireAfter_ = 0;
};

std::unique_ptr<Index> TtlIndex_New(const IndexDef& idef, PayloadType&& payloadType, FieldsSet&& fields,
									const NamespaceCacheConfigData& cacheCfg);
void UpdateExpireAfter(Index* i, int64_t v);

}  // namespace reindexer
