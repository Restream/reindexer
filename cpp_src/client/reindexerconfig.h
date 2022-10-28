#pragma once

#include <chrono>
#include <string>

namespace reindexer {
namespace client {

using std::chrono::seconds;

struct ReindexerConfig {
	ReindexerConfig(int _ConnPoolSize = 4, int _WorkerThreads = 1, int _FetchAmount = 10000, int _ReconnectAttempts = 0,
					seconds _ConnectTimeout = seconds(0), seconds _RequestTimeout = seconds(0), bool _EnableCompression = false,
					bool _RequestDedicatedThread = false, std::string _appName = "CPP-client", unsigned _syncRxCoroCount = 10)
		: ConnPoolSize(_ConnPoolSize),
		  WorkerThreads(_WorkerThreads),
		  FetchAmount(_FetchAmount),
		  ReconnectAttempts(_ReconnectAttempts),
		  ConnectTimeout(_ConnectTimeout),
		  RequestTimeout(_RequestTimeout),
		  EnableCompression(_EnableCompression),
		  RequestDedicatedThread(_RequestDedicatedThread),
		  AppName(std::move(_appName)),
		  rxClientCoroCount(_syncRxCoroCount) {}

	int ConnPoolSize;
	int WorkerThreads;
	int FetchAmount;
	int ReconnectAttempts;
	seconds ConnectTimeout;
	seconds RequestTimeout;
	bool EnableCompression;
	bool RequestDedicatedThread;
	std::string AppName;
	unsigned rxClientCoroCount;
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
