#pragma once

#include <chrono>
#include <fstream>
#include <unordered_map>
#include "clusterization_api.h"
#include "tools/timetools.h"

class ClusterizationProxyApi : public ClusterizationApi {
public:
	class ItemTracker {
	public:
		using ClockT = system_clock_w;

		~ItemTracker() { assert(validated_); }
		struct ItemInfo {
			ItemInfo(int _itemCounter, int _serverId, std::string _threadName)
				: itemCounter(_itemCounter), serverId(_serverId), threadName(std::move(_threadName)) {
				ItemAdd = ClockT::now();
			}
			ItemInfo(int _itemCounter, int _txCounter, int _txId, int _serverId, ClockT::time_point _txStart, std::string _threadName)
				: itemCounter(_itemCounter),
				  txCounter(_txCounter),
				  txId(_txId),
				  serverId(_serverId),
				  txStart(_txStart),
				  threadName(std::move(_threadName)) {
				ItemAdd = ClockT::now();
			}
			ItemInfo(int _txCounter, int _txId, int _serverId, ClockT::time_point _txStart, ClockT::time_point _txBeforeCommit,
					 std::string _threadName)
				: txCounter(_txCounter),
				  txId(_txId),
				  serverId(_serverId),
				  txStart(_txStart),
				  txBeforeCommit(_txBeforeCommit),
				  threadName(std::move(_threadName)),
				  isChecked(true) {
				ItemAdd = ClockT::now();
			}
			ItemInfo(int _txCounter, int _txId, int _serverId, ClockT::time_point _txStart, ClockT::time_point _txBeforeCommit,
					 ClockT::time_point _txAfterCommit, std::string _threadName)
				: txCounter(_txCounter),
				  txId(_txId),
				  serverId(_serverId),
				  txStart(_txStart),
				  txBeforeCommit(_txBeforeCommit),
				  txAfterCommit(_txAfterCommit),
				  threadName(std::move(_threadName)),
				  isChecked(true) {
				ItemAdd = ClockT::now();
			}

			int itemCounter = -1;
			int txCounter = -1;
			int txId = -1;
			int serverId = -1;
			int serialCounter = -1;
			ClockT::time_point txStart;
			ClockT::time_point txBeforeCommit;
			ClockT::time_point txAfterCommit;
			ClockT::time_point ItemAdd;
			std::string threadName;
			bool isChecked = false;
		};

		void AddCommited(std::string&& json, ItemInfo&& info) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(commitedItems_.count(json), 0) << json;
			info.serialCounter = counter;
			counter++;
			commitedItems_.emplace(std::make_pair(std::move(json), std::move(info)));
		}
		void AddTxData(std::unordered_map<std::string, ItemInfo>& into, std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items,
					   int txNum, ItemInfo&& txInfo) {
			std::lock_guard lck(mtx_);
			txInfo.serialCounter = counter;
			counter++;
			into.emplace(std::make_pair("TxBefore_" + std::to_string(txNum), std::move(txInfo)));
			for (auto&& it : items) {
				ASSERT_EQ(commitedItems_.count(it.first), 0) << it.first;
				it.second.serialCounter = counter;
				counter++;
				into.emplace(it);
			}
		}
		void AddCommitedTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(commitedItems_, items, txNum, std::move(txInfo));
		}
		void AddErrorTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(errorItems_, items, txNum, std::move(txInfo));
		}
		void AddUnknownTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(unknownItems_, items, txNum, std::move(txInfo));
		}

		void AddError(std::string&& json, ItemInfo&& info) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(errorItems_.count(json), 0) << json;
			errorItems_.emplace(std::make_pair(std::move(json), std::move(info)));
			info.serialCounter = counter;
			counter++;
		}
		void AddUnknown(std::string&& json, ItemInfo&& info) {
			std::lock_guard lck(mtx_);
			ASSERT_EQ(unknownItems_.count(json), 0) << json;
			unknownItems_.emplace(std::make_pair(std::move(json), std::move(info)));
			info.serialCounter = counter;
			counter++;
		}
		void Validate(reindexer::client::QueryResults& qr) {
			bool validateOk = true;
			reindexer::WrSerializer ser;
			for (auto& it : qr) {
				ser.Reset();
				auto err = it.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				std::string json(ser.Slice());
				auto itf = commitedItems_.find(json);
				if (itf != commitedItems_.end()) {
					itf->second.isChecked = true;
				} else {
					auto itfu = unknownItems_.find(json);
					if (itfu != unknownItems_.end()) {
						itfu->second.isChecked = true;
					} else {
						EXPECT_TRUE(false) << "Unexpected item: " << json << std::endl;
						validateOk = false;
					}
				}
			}
			for (const auto& it : commitedItems_) {
				if (!it.second.isChecked) {
					EXPECT_TRUE(false) << "Missing item: " << it.first << "; Added by thread: " << it.second.threadName << std::endl;
					validateOk = false;
				}
			}

			if (!validateOk) {
				std::string log = ServerControl::getTestLogPath();
				log += "itemtrackervalidate.log";
				std::ofstream itemsLog(log);

				auto outArray = [&itemsLog](const std::unordered_map<std::string, ItemInfo>& itemMap) {
					typedef std::pair<std::string, ItemTracker::ItemInfo> TrackerRow;
					std::vector<TrackerRow> infoVector(itemMap.begin(), itemMap.end());
					sort(infoVector.begin(), infoVector.end(),
						 [](const TrackerRow& a, const TrackerRow& b) { return a.second.serialCounter < b.second.serialCounter; });

					auto timeToString = [](ClockT::time_point tp) {
						auto tm = ClockT::to_time_t(tp);
						std::tm tmTime = reindexer::localtime(tm);
						auto timeInUs = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch()).count();
						int us = timeInUs % 1000;
						int ms = (timeInUs / 1000) % 1000;
						return fmt::format("{:02}:{:02}:{:02}.{:03}'{:03}", tmTime.tm_hour, tmTime.tm_min, tmTime.tm_sec, ms, us);
					};

					auto it = infoVector.begin();
					while (it != infoVector.end()) {
						if (it->second.txCounter == -1)	 // single entry
						{
							itemsLog << it->first;
							itemsLog << " serverId = " << it->second.serverId;
							itemsLog << " ItemAddTime = " << timeToString(it->second.ItemAdd);

							itemsLog << " threadName = " << it->second.threadName;
							itemsLog << " isChecked = " << it->second.isChecked;
							itemsLog << std::endl;
							++it;
						} else {  // transaction
							itemsLog << "txCounter = " << it->second.txCounter;
							itemsLog << " txId = " << it->second.txId;
							itemsLog << " serverId = " << it->second.serverId;
							itemsLog << " threadName = " << it->second.threadName;
							itemsLog << " txStart = " << timeToString(it->second.txStart);
							itemsLog << " txBeforeCommit = " << timeToString(it->second.txBeforeCommit);
							itemsLog << " txAfterCommit = " << timeToString(it->second.txAfterCommit);
							itemsLog << std::endl;
							int curTx = it->second.txCounter;
							++it;
							while (it != infoVector.end() && it->second.txCounter == curTx) {
								itemsLog << "    " << it->first;
								itemsLog << " ItemAddTime = " << timeToString(it->second.ItemAdd) << " ";
								itemsLog << " isChecked = " << it->second.isChecked;
								itemsLog << std::endl;
								++it;
							}
						}
					}
				};

				itemsLog << "=============================================commitedItems_============================================="
						 << std::endl;
				outArray(commitedItems_);
				itemsLog << "=============================================unknownItems_============================================="
						 << std::endl;
				outArray(unknownItems_);
				itemsLog << "=============================================errorItems_============================================="
						 << std::endl;
				outArray(errorItems_);
			}
			validated_ = true;
		}

	private:
		std::mutex mtx_;
		std::unordered_map<std::string, ItemInfo> commitedItems_;
		std::unordered_map<std::string, ItemInfo> errorItems_;
		std::unordered_map<std::string, ItemInfo> unknownItems_;
		bool validated_ = false;
		int counter = 0;
	};

	const Defaults& GetDefaults() const override {
		static Defaults defs{14100, 16100, fs::JoinPath(fs::GetTempDir(), "rx_test/ClusterizationProxyApi")};
		return defs;
	}
	int GetRandFollower(int clusterSize, int leaderId) {
		if (leaderId >= clusterSize) {
			assert(false);
		}
		while (true) {
			int k = rand() % clusterSize;
			if (k != leaderId) {
				return k;
			}
		}
	}
};
