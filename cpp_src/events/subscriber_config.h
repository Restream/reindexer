#pragma once

#include <span>
#include "core/type_consts.h"
#include "estl/fast_hash_set.h"
#include "events/iexternal_listener.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class [[nodiscard]] EventSubscriberConfig {
public:
	using EventsSetT = fast_hash_set<updates::URType>;

	struct [[nodiscard]] StreamConfig {
		using SetT = fast_hash_set<std::string, nocase_hash_str, nocase_equal_str, nocase_less_str>;

		explicit StreamConfig(uint16_t _id) : id(_id) {}
		bool Check(const EventRecord& rec) const noexcept {
			if (!eventTypes.empty() && !eventTypes.count(rec.Type())) {
				return false;
			}
			const auto& ns = rec.NsName();
			if (!isSystemNamespaceNameFast(ns)) {
				return nss.empty() || nss.count(ns);
			}
			return withConfigNamespace;
		}

		SetT nss;
		EventsSetT eventTypes;
		const uint16_t id = 0;
		bool withConfigNamespace = false;
	};
	using StreamsContainerT = std::vector<StreamConfig>;

	Error FromJSON(std::span<char> json) noexcept;
	void FromJSON(const gason::JsonNode& root);
	void GetJSON(WrSerializer& ser) const;

	const StreamsContainerT& Streams() const& noexcept { return streams_; }
	const StreamsContainerT& Streams() const&& = delete;
	size_t ActiveStreams() const noexcept { return streams_.size(); }
	bool WithDBName() const noexcept { return withDBName_; }
	bool WithServerID() const noexcept { return withServerID_; }
	bool WithShardID() const noexcept { return withShardID_; }
	bool WithLSN() const noexcept { return withLSN_; }
	bool WithTimestamp() const noexcept { return withTimestamp_; }
	SubscriptionDataType DataType() const noexcept { return dataType_; }
	const EventsSetT& Events() const noexcept { return eventTypes_; }

private:
	int formatVersion_ = kSubscribersConfigFormatVersion;
	SubscriptionDataType dataType_ = kSubscriptionDataTypeNone;
	bool withDBName_ = false;
	bool withServerID_ = false;
	bool withShardID_ = false;
	bool withLSN_ = false;
	bool withTimestamp_ = false;
	EventsSetT eventTypes_;
	StreamsContainerT streams_;
};

}  // namespace reindexer
