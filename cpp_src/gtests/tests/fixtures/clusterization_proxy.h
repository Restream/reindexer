#pragma once

#include "clusterization_api.h"

class ClusterizationProxyApi : public ClusterizationApi {
public:
	class ItemTracker {
	public:
		~ItemTracker() { assert(validated_); }
		void AddCommited(std::string&& json, std::string threadName) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(commitedItems_.count(json), 0) << json;
			commitedItems_.emplace(std::make_pair(std::move(json), std::move(threadName)));
		}
		void AddError(std::string&& json, std::string threadName) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(errorItems_.count(json), 0) << json;
			errorItems_.emplace(std::make_pair(std::move(json), std::move(threadName)));
		}
		void AddUnknown(std::string&& json, std::string threadName) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(unknownItems_.count(json), 0) << json;
			unknownItems_.emplace(std::make_pair(std::move(json), std::move(threadName)));
		}
		void Validate(reindexer::client::SyncCoroQueryResults& qr) {
			WrSerializer ser;
			for (auto& it : qr) {
				ser.Reset();
				it.GetJSON(ser, false);
				std::string json(ser.Slice());
				if (commitedItems_.count(json)) {
					commitedItems_.erase(json);
				} else if (unknownItems_.count(json)) {
					unknownItems_.erase(json);
				} else {
					EXPECT_TRUE(false) << "Unexpected item: " << json << std::endl;
				}
			}
			for (auto& it : commitedItems_) {
				EXPECT_TRUE(false) << "Missing item: " << it.first << "; Added by thread: " << it.second << std::endl;
			}
			EXPECT_EQ(commitedItems_.size(), 0);
			validated_ = true;
		}

	private:
		std::mutex mtx_;
		std::unordered_map<std::string, std::string> commitedItems_;
		std::unordered_map<std::string, std::string> errorItems_;
		std::unordered_map<std::string, std::string> unknownItems_;
		bool validated_ = false;
	};

	virtual Defaults GetDefaultPorts() override { return {14100, 16100, fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterizationProxyApi")}; }
	int GetRandFollower(int clusterSize, int leaderId) {
		if (leaderId >= clusterSize) {
			assert(false);
		}
		while (true) {
			int k = rand() % clusterSize;
			if (k != leaderId) return k;
		}
	}
};
