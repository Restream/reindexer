#pragma once

#include <cstdint>

namespace reindexer::client {

enum ConnectOpt { kConnectOptCreateIfMissing = 1 << 0, kConnectOptCheckClusterID = 1 << 1 };

class [[nodiscard]] ConnectOpts {
public:
	bool IsCreateDBIfMissing() const noexcept { return options_ & kConnectOptCreateIfMissing; }
	int ExpectedClusterID() const noexcept { return expectedClusterID_; }
	bool HasExpectedClusterID() const noexcept { return options_ & kConnectOptCheckClusterID; }

	ConnectOpts& CreateDBIfMissing(bool value = true) noexcept {
		options_ = value ? options_ | kConnectOptCreateIfMissing : options_ & ~(kConnectOptCreateIfMissing);
		return *this;
	}
	ConnectOpts& WithExpectedClusterID(int clusterID) noexcept {
		expectedClusterID_ = clusterID;
		options_ |= kConnectOptCheckClusterID;
		return *this;
	}

private:
	uint16_t options_ = 0;
	int expectedClusterID_ = -1;
};

}  // namespace reindexer::client
