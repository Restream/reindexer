#pragma once

#include <unordered_map>
#include "cluster_operation_api.h"
#include "estl/mutex.h"

class [[nodiscard]] ClusterOperationProxyApi : public ClusterOperationApi {
public:
	class [[nodiscard]] ItemTracker {
	public:
		using ClockT = reindexer::system_clock_w;

		~ItemTracker() { assert(validated_); }
		struct [[nodiscard]] ItemInfo {
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

		void AddCommited(std::string&& json, ItemInfo&& info);
		void AddTxData(std::unordered_map<std::string, ItemInfo>& into, std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items,
					   int txNum, ItemInfo&& txInfo);
		void AddCommitedTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(commitedItems_, items, txNum, std::move(txInfo));
		}
		void AddErrorTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(errorItems_, items, txNum, std::move(txInfo));
		}
		void AddUnknownTx(std::vector<std::pair<std::string, ItemTracker::ItemInfo>>& items, int txNum, ItemInfo&& txInfo) {
			AddTxData(unknownItems_, items, txNum, std::move(txInfo));
		}

		void AddError(std::string&& json, ItemInfo&& info);
		void AddUnknown(std::string&& json, ItemInfo&& info);
		void Validate(reindexer::client::QueryResults& qr);

	private:
		reindexer::mutex mtx_;
		std::unordered_map<std::string, ItemInfo> commitedItems_;
		std::unordered_map<std::string, ItemInfo> errorItems_;
		std::unordered_map<std::string, ItemInfo> unknownItems_;
		bool validated_ = false;
		int counter = 0;
	};

	const Defaults& GetDefaults() const override;
	int GetRandFollower(int clusterSize, int leaderId);
};
