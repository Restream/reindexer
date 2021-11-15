#pragma once

#include <cstdint>

namespace reindexer {
namespace client {

enum ConnectOpt {
	kConnectOptCreateIfMissing = 1 << 0,
	kConnectOptCheckClusterID = 1 << 1,
};

struct ConnectOpts {
	bool IsCreateDBIfMissing() const { return options & kConnectOptCreateIfMissing; }
	int ExpectedClusterID() const { return expectedClusterID; }
	bool HasExpectedClusterID() const { return options & kConnectOptCheckClusterID; }

	ConnectOpts& CreateDBIfMissing(bool value = true) {
		options = value ? options | kConnectOptCreateIfMissing : options & ~(kConnectOptCreateIfMissing);
		return *this;
	}
	ConnectOpts& WithExpectedClusterID(int clusterID) {
		expectedClusterID = clusterID;
		options |= kConnectOptCheckClusterID;
		return *this;
	}

	uint16_t options = 0;
	int expectedClusterID = -1;
};

}
}
