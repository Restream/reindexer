#pragma once

#include "core/namespace/namespacename.h"
#include "updates/updaterecord.h"

namespace reindexer {

class [[nodiscard]] EventRecord : public updates::UpdateRecord {
	using ClockT = system_clock_w;

public:
	EventRecord() = default;
	explicit EventRecord(const updates::UpdateRecord& rec)
		: updates::UpdateRecord(rec.Clone<ClonePolicy::WithEmitter>()), ts_(ClockT::now()) {}
	explicit EventRecord(updates::UpdateRecord&& rec) noexcept : updates::UpdateRecord(std::move(rec)), ts_(ClockT::now()) {}

	ClockT::time_point Timestamp() const noexcept { return ts_; }
	void UpdateTimestamp() noexcept { ts_ = ClockT::now(); }

private:
	ClockT::time_point ts_;
};

using EventsContainer = h_vector<EventRecord, 2>;

class [[nodiscard]] IExternalEventsListener {
public:
	virtual Error SendEvents(EventsContainer&& recs) = 0;
	virtual bool HasListenersFor(const NamespaceName& ns) const noexcept = 0;
	virtual ~IExternalEventsListener() {}
};

enum [[nodiscard]] SubscriptionDataType { kSubscriptionDataTypeNone = 0 };
constexpr static uint32_t kEventDataFormatMask = 0x7;
enum [[nodiscard]] EventsSerializationOpt {
	// 3 bits for data format
	kEventsSerializationOptWithDbName = 1 << 3,
	kEventsSerializationOptWithShardID = 1 << 4,
	kEventsSerializationOptWithLSN = 1 << 5,
	kEventsSerializationOptWithServerID = 1 << 6,
	kEventsSerializationOptWithTimestamp = 1 << 7,
};

class [[nodiscard]] EventsSerializationOpts {
public:
	explicit EventsSerializationOpts(int32_t version) noexcept : version_(version) {}

	bool IsWithDBName() const noexcept { return options_ & kEventsSerializationOptWithDbName; }
	std::string_view DBName() const noexcept { return dbName_; }

	EventsSerializationOpts& WithDBName(std::string_view name, bool value) noexcept {
		dbName_ = name;
		options_ = value ? options_ | kEventsSerializationOptWithDbName : options_ & ~(kEventsSerializationOptWithDbName);
		return *this;
	}

	SubscriptionDataType DataType() const noexcept { return SubscriptionDataType(options_ & kEventDataFormatMask); }
	EventsSerializationOpts& WithData(SubscriptionDataType t) {
		assertrx_throw(uint32_t(t) <= kEventDataFormatMask);
		options_ = (options_ & ~kEventDataFormatMask) | uint32_t(t);
		return *this;
	}

	bool IsWithShardID() const noexcept { return options_ & kEventsSerializationOptWithShardID; }
	int32_t ShardID() const noexcept { return shardID_; }
	EventsSerializationOpts& WithShardID(int32_t shardID, bool value) noexcept {
		shardID_ = shardID;
		options_ = value ? options_ | kEventsSerializationOptWithShardID : options_ & ~(kEventsSerializationOptWithShardID);
		return *this;
	}

	bool IsWithLSN() const noexcept { return options_ & kEventsSerializationOptWithLSN; }
	EventsSerializationOpts& WithLSN(bool value) noexcept {
		options_ = value ? options_ | kEventsSerializationOptWithLSN : options_ & ~(kEventsSerializationOptWithLSN);
		return *this;
	}

	bool IsWithServerID() const noexcept { return options_ & kEventsSerializationOptWithServerID; }
	int32_t ServerID() const noexcept { return serverID_; }
	EventsSerializationOpts& WithServerID(int32_t serverID, bool value) noexcept {
		serverID_ = serverID;
		options_ = value ? options_ | kEventsSerializationOptWithServerID : options_ & ~(kEventsSerializationOptWithServerID);
		return *this;
	}

	bool IsWithTimestamp() const noexcept { return options_ & kEventsSerializationOptWithTimestamp; }
	EventsSerializationOpts& WithTimestamp(bool value) noexcept {
		options_ = value ? options_ | kEventsSerializationOptWithTimestamp : options_ & ~(kEventsSerializationOptWithTimestamp);
		return *this;
	}

	uint32_t Version() const noexcept { return version_; }
	uint32_t Options() const noexcept { return options_; }

private:
	std::string_view dbName_;
	const int32_t version_;
	uint32_t options_ = 0;
	int32_t shardID_ = ShardingKeyType::NotSetShard;
	int32_t serverID_ = -1;
};

class [[nodiscard]] IEventsObserver {
public:
	virtual ~IEventsObserver() = default;
	virtual size_t AvailableEventsSpace() noexcept = 0;
	virtual void SendEvent(uint32_t streamsMask, const EventsSerializationOpts& opts, const EventRecord& rec) = 0;
};

}  // namespace reindexer
