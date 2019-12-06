#pragma once

#include <chrono>

namespace reindexer {
namespace client {

using std::chrono::seconds;

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000, seconds _ConnectTimeout = seconds(0),
					seconds _RequestTimeout = seconds(0))
		: ConnPoolSize(_ConnPoolSize),
		  WorkerThreads(_WorkerThreads),
		  FetchAmount(_FetchAmount),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout) {}

	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
	seconds ConnectTimeout;
	seconds RequestTimeout;
};

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
	ConnectOpts& CheckClusterID(int value) {
		expectedClusterID = value;
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

}  // namespace client
}  // namespace reindexer
