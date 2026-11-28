#pragma once

#include <cstdint>
#include "core/replicationsender.h"

namespace reindexer::client {

enum [[nodiscard]] ConnectOpt { kConnectOptCreateIfMissing = 1 << 0, kConnectOptCheckClusterID = 1 << 1 };

class [[nodiscard]] ConnectOpts {
public:
	bool IsCreateDBIfMissing() const noexcept { return options_ & kConnectOptCreateIfMissing; }
	int ExpectedClusterID() const noexcept { return expectedClusterID_; }
	bool HasExpectedClusterID() const noexcept { return options_ & kConnectOptCheckClusterID; }
	const ReplicationSender& GetReplicationSender() const noexcept { return replicationSender_; }

	ConnectOpts& CreateDBIfMissing(bool value = true) noexcept {
		options_ = value ? options_ | kConnectOptCreateIfMissing : options_ & ~(kConnectOptCreateIfMissing);
		return *this;
	}
	ConnectOpts& WithExpectedClusterID(int clusterID) noexcept {
		expectedClusterID_ = clusterID;
		options_ |= kConnectOptCheckClusterID;
		return *this;
	}
	ConnectOpts& WithReplicationSender(ReplicationSender sender) noexcept {
		replicationSender_ = sender;
		return *this;
	}

private:
	uint16_t options_ = 0;
	int expectedClusterID_ = -1;
	ReplicationSender replicationSender_;
};

}  // namespace reindexer::client
