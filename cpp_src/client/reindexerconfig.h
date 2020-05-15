#pragma once

#include <chrono>
#include <string>

namespace reindexer {
namespace client {

using std::chrono::seconds;

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000, int _ReconnectAttempts = 0,
					seconds _ConnectTimeout = seconds(0), seconds _RequestTimeout = seconds(0), bool _EnableCompression = false,
					std::string _appName = "CPP-client")
		: ConnPoolSize(_ConnPoolSize),
		  WorkerThreads(_WorkerThreads),
		  FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout),
		  EnableCompression(_EnableCompression),
		  AppName(std::move(_appName)) {}

	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
	int ReconnectAttempts;
	seconds ConnectTimeout;
	seconds RequestTimeout;
	bool EnableCompression;
	std::string AppName;
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
