#pragma once

#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadtype.h"

namespace reindexer::ns_indexes {

class Registry;
class TargetState;
class TransactionDDL;

/**
 * @brief The namespace payload metadata: the payload type and the tags matcher. Both describe exactly the same
 * fields layout as the indexes registry, so they may only change together with the registry content.
 *
 * Owned by the registry; the only way to change the fields layout is an indexes transaction. Outer code gets a
 * constant payload type and may register new tags in the tags matcher, but cannot change its fields layout - those
 * TagsMatcher methods are only available inside this module.
 */
class [[nodiscard]] Metadata {
public:
	Metadata(const std::string& nsName, int32_t tmStateToken) : plType_{nsName}, tagsMatcher_{plType_, {}, tmStateToken} {}
	explicit Metadata(const Metadata& src) : plType_{src.plType_}, tagsMatcher_{src.tagsMatcher_} {}
	Metadata(Metadata&&) = delete;
	Metadata& operator=(const Metadata&) = delete;
	Metadata& operator=(Metadata&&) = delete;
	~Metadata() = default;

	const PayloadType& GetPayloadType() const& noexcept { return plType_; }
	const TagsMatcher& GetTagsMatcher() const& noexcept { return tagsMatcher_; }

	auto GetPayloadType() const&& = delete;
	auto GetTagsMatcher() const&& = delete;

private:
	friend class Registry;
	friend class TargetState;
	friend class TransactionDDL;

	PayloadType plType_;
	TagsMatcher tagsMatcher_;
};

}  // namespace reindexer::ns_indexes
