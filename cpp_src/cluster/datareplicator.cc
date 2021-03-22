#include "datareplicator.h"
#include "client/snapshot.h"
#include "core/namespace/snapshot/snapshot.h"
#include "core/reindexerimpl.h"

namespace reindexer {
namespace cluster {

constexpr int kDefaultSyncThreadCount = 4;
constexpr size_t kNsReplicationRoutineStackSize = 128 * 1024;
constexpr auto kLeaderNsResyncInterval = std::chrono::milliseconds(1000);

DataReplicator::DataReplicator(ReindexerImpl& thisNode, std::function<void()> requestElectionsRestartCb)
	: updatesQueue_(kDefaultSyncThreadCount), thisNode_(thisNode), requestElectionsRestartCb_(std::move(requestElectionsRestartCb)) {
	assert(requestElectionsRestartCb_);
	terminateAsync_.set([this](net::ev::async& watcher) { this->onTerminateAsync(watcher); });
	terminateAsync_.set(loop_);
	roleSwitchAsync_.set([this](net::ev::async& watcher) { this->onRoleSwitchAsync(watcher); });
	roleSwitchAsync_.set(loop_);
}

Error DataReplicator::Replicate(UpdateRecord&& rec, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	UpdatesContainer recs(1);
	recs[0] = std::move(rec);
	return Replicate(std::move(recs), std::move(beforeWaitF), ctx);
}

Error DataReplicator::Replicate(UpdatesContainer&& recs, std::function<void()> beforeWaitF, const RdxContext& ctx) {
	if (recs.empty()) return errOK;
	string_view name = recs[0].GetNsName();
	if (name.size() && name[0] == '#') return errOK;

	std::pair<Error, bool> res;
	if (ctx.originLsn_.isEmpty()) {
		res = updatesQueue_.Push(std::move(recs), std::move(beforeWaitF), ctx);
	} else {
		// Update can't be replicated to cluster from another node, so may only be replicated to async replicas
		res = updatesQueue_.PushAsync(std::move(recs));
	}
	if (res.second) {
		return res.first;
	}
	return Error();	 // This namespace is not taking part in any replication
}

void DataReplicator::Run(int serverId, ClusterConfigData config) {
	config_ = std::move(config);

	consensusCnt_ = config_.nodes.size() / 2 + 1;
	serverId_ = serverId;
	for (auto it = config_.nodes.begin(); it != config_.nodes.end();) {
		if (it->serverId == serverId) {
			it = config_.nodes.erase(it);
		} else {
			++it;
		}
	}

	syncThreads_.clear();
	syncThreads_.resize(config_.syncThreadsCount > 0 ? config_.syncThreadsCount : kDefaultSyncThreadCount);
	auto nsNames = config_.namespaces;
	if (nsNames.empty()) {
		std::vector<NamespaceDef> nsDefs;
		thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary().WithClosed());
		for (auto& ns : nsDefs) {
			nsNames.emplace(std::move(ns.name));
		}
	}
	std::unique_ptr<NsNamesHashSetT> clusterNsList(new NsNamesHashSetT);
	*clusterNsList = config_.namespaces;
	// TODO: Add async namespaces from replication config
	// TODO: updatesQueue_ must contain cluster + replication namespaces and syncState_ must contain cluster namespaces only
	updatesQueue_.RebuildShards(syncThreads_.size(), nsNames, std::move(clusterNsList), std::unique_ptr<NsNamesHashSetT>());
	sharedSyncState_.Reset(std::move(nsNames), config_.nodes.size());

	for (size_t i = 0; i < syncThreads_.size(); ++i) {
		syncThreads_[i].roleSwitchAsync.set(syncThreads_[i].loop);
		syncThreads_[i].roleSwitchAsync.set([this, i](net::ev::async& watcher) {
			watcher.loop.spawn([this, i] {
				auto& nextRoleCh = syncThreads_[i].nextRoleCh;
				if (terminate_) {
					nextRoleCh.close();
				} else {
					auto newState = sharedSyncState_.CurrentRole();
					if (!nextRoleCh.empty()) {
						nextRoleCh.pop();
					}
					// TODO: Here we also may cancel current leader's sync to speed up role transition
					nextRoleCh.push(newState.role);
				}
			});
		});

		syncThreads_[i].updatesAsync.set(syncThreads_[i].loop);
		syncThreads_[i].updatesAsync.set(
			[this, i](net::ev::async& watcher) { watcher.loop.spawn([this, i] { updatesReadingRoutine(i); }); });

		syncThreads_[i].th = std::thread([this, i] { dataReplicationThread(i, config_); });
	}

	terminateAsync_.start();
	roleSwitchAsync_.start();

	loop_.run();

	terminateAsync_.stop();
	roleSwitchAsync_.stop();

	for (auto& t : syncThreads_) {
		t.th.join();
	}
}

void DataReplicator::OnRoleChanged(RaftInfo::Role to, int leaderId) {
	RaftInfo info;
	info.role = to;
	info.leaderId = leaderId;
	sharedSyncState_.SetRole(info);
	roleSwitchAsync_.send();
}

RaftInfo DataReplicator::GetRaftInfo(bool allowTransitState, const RdxContext& ctx) const {
	return sharedSyncState_.AwaitRole(allowTransitState, ctx);
}

void DataReplicator::dataReplicationThread(size_t shardId, const ClusterConfigData& config) {
	auto& loop = syncThreads_[shardId].loop;
	auto& threadLocal = syncThreads_[shardId];
	loop.spawn([this, shardId, &config, &threadLocal] {
		auto& loop = threadLocal.loop;

		std::cerr << serverId_ << ":starting dataReplicationThread" << std::endl;
		threadLocal.nodes.clear();
		threadLocal.nsUpdatesData.clear();
		client::CoroReindexerConfig rpcCfg;
		rpcCfg.AppName = config.appName;
		rpcCfg.ConnectTimeout = std::chrono::seconds(config.updatesTimeoutSec);
		rpcCfg.RequestTimeout = std::chrono::seconds(config.updatesTimeoutSec);
		rpcCfg.EnableCompression = config.enableCompression;
		for (auto& node : config.nodes) {
			// TODO: add async replication nodes
			threadLocal.nodes.emplace_back(ReplicatorNode::Type::Cluster, node.serverId, rpcCfg);
			threadLocal.nodes.back().dsn = node.GetRPCDsn();
		}

		auto shard = updatesQueue_.GetShard(shardId);
		auto nsList = shard->GetTokens();
		shard->SetDataNotifier([&threadLocal] { threadLocal.updatesAsync.send(); });

		for (auto& ns : nsList) {
			auto& listernes = threadLocal.nsUpdatesData[std::string(ns)].listeners;

			for (auto& node : threadLocal.nodes) {
				// TODO: check if this node has to replicate specific namespace
				node.updateNotifiers.emplace_back(ns);
				listernes.Add(&node.updateNotifiers.back().ch);
			}
		}

		threadLocal.wg.add(1);
		loop.spawn([this, shardId, &threadLocal] {
			coroutine::wait_group_guard wgg(threadLocal.wg);
			workerRoleMonitoringRoutine(shardId);
		});
		// TODO: Try to invert this loop to reuse QueryResults and eliminate local selects
		// (for each ns -> local select -> for each node -> send results)
		for (size_t i = 0; i < threadLocal.nodes.size(); ++i) {
			threadLocal.wg.add(1);
			loop.spawn([this, &threadLocal, i] {
				coroutine::wait_group_guard wgg(threadLocal.wg);
				nodeReplicationRoutine(i, threadLocal);
			});
		}
		threadLocal.wg.wait();
		shard->SetDataNotifier(std::function<void()>());

		for (auto& node : threadLocal.nodes) {
			threadLocal.wg.add(1);
			loop.spawn([&node, &threadLocal] {
				coroutine::wait_group_guard wgg(threadLocal.wg);
				node.client.Stop();
			});
		}

		threadLocal.wg.wait();
	});

	auto& roleSwitchAsync = syncThreads_[shardId].roleSwitchAsync;
	auto& updatesAsync = syncThreads_[shardId].updatesAsync;
	roleSwitchAsync.start();
	updatesAsync.start();
	roleSwitchAsync.send();	 // Initiate role switch right after loop start
	updatesAsync.send();

	loop.run();

	roleSwitchAsync.stop();
	updatesAsync.stop();
}

void DataReplicator::nodeReplicationRoutine(size_t nodeId, DataReplicator::SyncThread& threadLocal) {
	// 3) Perform wal-sync/force-sync for each follower
	for (auto& nsListener : threadLocal.nsUpdatesData) {
		auto nsName = nsListener.first;
		// TODO: If we're going to allow the same node to be replica and cluster memember, we've to
		// add cluster-flag for each namespace, not just for full node
		threadLocal.nodes[nodeId].wg.add(1);
		threadLocal.loop.spawn(
			[nsName, this, nodeId, &threadLocal] {
				coroutine::wait_group_guard wgg(threadLocal.nodes[nodeId].wg);
				nsReplicationRoutine(nodeId, nsName, threadLocal, false);
			},
			kNsReplicationRoutineStackSize);
	}
	threadLocal.nodes[nodeId].wg.wait();
}

void DataReplicator::nsReplicationRoutine(size_t nodeId, string_view nsName, DataReplicator::SyncThread& threadLocal, bool newNs) {
	auto& node = threadLocal.nodes[nodeId];
	while (!terminate_) {
		assert(node.namespaceData[nsName].tx.IsFree());

		if (node.type == ReplicatorNode::Type::Cluster) {
			threadLocal.leadershipAwaitCh.pop();
			if (terminate_) {
				break;
			}
		}

		std::cerr << serverId_ << ":" << nodeId << ":Start NS replication as leader: " << nodeId << ":" << nsName << ":" << newNs
				  << std::endl;

		// 3.1) Perform wal-sync/force-sync for specified namespace in separated routine
		ReplicationStateV2 replState;
		auto err = node.client.GetReplState(nsName, replState);
		if (err.ok()) {
			if (replState.clusterStatus.role == NsClusterizationStatus::Role::None || replState.clusterStatus.leaderId != serverId_) {
				// Await transition
				std::cerr << serverId_ << ":" << nodeId << ":Awaiting role switch" << std::endl;
				threadLocal.loop.sleep(std::chrono::milliseconds(100));
				continue;
			}
		} else {
			std::cerr << serverId_ << ":" << nodeId << ":GetReplState: " << err.what() << std::endl;
		}

		if (err.ok() || err.code() == errNotFound) {
			if (err.code() == errNotFound) {
				replState = ReplicationStateV2();
			}
			err = syncNamespace(nodeId, nsName, replState, threadLocal);
			if (!err.ok()) {
				std::cerr << serverId_ << ":" << nodeId << ":Namespace sync error: " << err.what() << std::endl;
				if (err.code() == errDataHashMismatch) {
					replState = ReplicationStateV2();
					err = syncNamespace(nodeId, nsName, replState, threadLocal);
					if (!err.ok()) {
						std::cerr << serverId_ << ":" << nodeId << ":Namespace sync error (resyn due to datahash missmatch): " << err.what()
								  << std::endl;
					}
				}
			}
		}

		if (err.ok()) {
			// 4) Sending updates for this namespace
			err = nsUpdatesHandlingLoop(nodeId, nsName, threadLocal);
			newNs = false;
		}
		// Wait before next sync retry
		auto& tx = node.namespaceData[nsName].tx;
		if (!tx.IsFree()) {
			node.client.RollBackTransaction(tx);
		}
		if (!terminate_) {
			std::cerr << serverId_ << ":" << nsName << ":" << nodeId << "; resync was scheduled due to error: " << err.what() << std::endl;
		}
		constexpr auto kGranularSleepInterval = std::chrono::milliseconds(100);
		auto awaitTime = std::chrono::milliseconds(config_.retrySyncIntervalMSec);
		while (!terminate_ && awaitTime.count() > 0) {
			auto diff = std::min(awaitTime, kGranularSleepInterval);
			awaitTime -= diff;
			threadLocal.loop.sleep(diff);

			if (isNetworkError(err) || isLeaderChangedError(err)) {
				handleUpdatesWithError(nodeId, nsName, threadLocal, err);
			}
		}
	}
	std::cerr << serverId_ << ":" << nodeId << ":nsReplicationRoutine was terminated" << std::endl;
}

void DataReplicator::updatesReadingRoutine(size_t shardId) {
	auto shard = updatesQueue_.GetShard(shardId);
	auto& threadLocal = syncThreads_[shardId];
	std::cerr << serverId_ << ": Reading for " << shardId << std::endl;
	if (!terminate_) {
		auto updates = shard->Read();
		// 5) Check updates and send them to other routines;
		auto count = updates.second;
		auto it = updates.first;
		if (count) {
			std::cerr << serverId_ << ": Recv new updates(" << count << ") for " << it->data.nsName << std::endl;
		}
		for (size_t i = 0; i < count; ++i) {
			std::string nameStr(it->data.nsName);
			auto& updatesData = threadLocal.nsUpdatesData[nameStr];
			if (startUpdateRoutineForNewNs(nameStr, shard, threadLocal)) {
				sharedSyncState_.MarkSynchronized(std::move(nameStr),
												  true);  // If we've got first update for NS, it has to be synchronized (?)
			}

			if (it->RequireResult() && !isLeader(threadLocal)) {
				auto roles = sharedSyncState_.GetRolesPair();
				if (roles.second.role != RaftInfo::Role::Leader) {
					auto tmpIt = it;  // OnResult call will erase actual 'it'
					++it;
					tmpIt->OnResult(
						Error(errUpdateReplication, "%d node is not a leader. It's not allowed to replicate sync updates", serverId_));
					continue;
				}
			}
			updatesData.updates.emplace_back(it);
			updatesData.listeners.Notify(--updatesData.updates.end());
			++it;
		}
	} else {
		auto updates = shard->ReadAndInvalidate();
		auto count = updates.second;
		auto it = updates.first;
		for (size_t i = 0; i < count; ++i, ++it) {
			it->OnResult(Error(errUpdateReplication, "Updates queue was invalidated (cluster reconfiguration?)"));
			// Remove invalidated updates (those updates were recieved after cluster was rebuilt)
		}
		for (auto& node : threadLocal.nodes) {
			for (auto& notifier : node.updateNotifiers) {
				notifier.ch.close();
			}
		}
	}
}

void DataReplicator::workerRoleMonitoringRoutine(size_t shardId) {
	auto& threadLocal = syncThreads_[shardId];
	auto& leadershipAwaitCh = threadLocal.leadershipAwaitCh;
	auto& nextRoleCh = threadLocal.nextRoleCh;
	RaftInfo::Role workerRole = RaftInfo::Role::None;
	net::ev::timer leaderResyncTimer;
	coroutine::wait_group leaderResyncWg;
	leaderResyncTimer.set(syncThreads_[shardId].loop);
	auto leaderSyncFn = [this, shardId, &leaderResyncWg](net::ev::timer& timer, int) {
		leaderResyncWg.add(1);
		assert(leaderResyncWg.wait_count() == 1);
		timer.loop.spawn([this, shardId, &timer, &leaderResyncWg] {
			coroutine::wait_group_guard wgg(leaderResyncWg);
			try {
				syncThreads_[shardId].ConnectClusterNodes();

				initialLeadersSync(shardId);
			} catch (Error& err) {
				syncThreads_[shardId].DisconnectClusterNodes();
				double interval = std::chrono::duration_cast<std::chrono::milliseconds>(kLeaderNsResyncInterval).count();
				timer.start(interval / 1000);
				std::cerr << serverId_ << ": Leader's sync failed. Last error is: " << err.what() << ". New sync was scheduled in "
						  << std::chrono::duration_cast<std::chrono::milliseconds>(kLeaderNsResyncInterval).count() << " ms" << std::endl;
				return;
			}

			std::cerr << serverId_ << ": Leader's resync done" << std::endl;
			syncThreads_[shardId].leadershipAwaitCh.close();
		});
	};
	leaderResyncTimer.set(leaderSyncFn);

	while (!terminate_) {
		auto roleP = nextRoleCh.pop();
		if (!roleP.second) {
			break;
		}
		auto newRole = roleP.first;
		std::cerr << serverId_ << ": workerRoleMonitoringRoutine got new role: " << RaftInfo::RoleToStr(newRole) << "; current role is "
				  << RaftInfo::RoleToStr(workerRole) << std::endl;
		if (workerRole != newRole) {
			if (newRole == RaftInfo::Role::Leader) {
				std::cerr << serverId_ << ": Leader resync has begun" << std::endl;
				leaderSyncFn(leaderResyncTimer, 0);
			} else if (workerRole == RaftInfo::Role::Leader) {
				std::cerr << serverId_ << ": Leader resync timer stop" << std::endl;
				leaderResyncTimer.stop();
				leaderResyncWg.wait();
				if (!leadershipAwaitCh.opened()) {
					leadershipAwaitCh.reopen();
				}
				threadLocal.DisconnectClusterNodes();
			}
			workerRole = newRole;
		}
	}
	leaderResyncTimer.stop();
	leaderResyncTimer.reset();
	leaderResyncWg.wait();
	leadershipAwaitCh.close();
	threadLocal.DisconnectClusterNodes();
}

void DataReplicator::initialLeadersSync(size_t shardId) {
	auto& threadLocal = syncThreads_[shardId];
	Error lastErr;
	coroutine::wait_group leaderSyncWg;

	NsNamesHashSetT followersNsList;
	if (config_.namespaces.empty()) {
		followersNsList = collectNsNames(threadLocal);
	}
	for (auto& nsName : followersNsList) {
		auto shardPair = updatesQueue_.GetShard(nsName);
		if (shardPair.second == UpdatesQueueT::TokenType::Sync && shardPair.first == updatesQueue_.GetShard(shardId)) {
			startUpdateRoutineForNewNs(nsName, shardPair.first, threadLocal);
		}
	}

	for (auto& nsListener : threadLocal.nsUpdatesData) {
		std::cerr << serverId_ << ": Leader's resync timer: create coroutine for " << nsListener.first << std::endl;
		leaderSyncWg.add(1);
		threadLocal.loop.spawn([this, &lastErr, &leaderSyncWg, &nsListener, &threadLocal] {
			coroutine::wait_group_guard wgg(leaderSyncWg);
			auto err = syncLatestNamespaceToLeader(nsListener.first, threadLocal);
			if (err.ok()) {
				sharedSyncState_.MarkSynchronized(nsListener.first, true);
			} else {
				lastErr = std::move(err);
			}
		});
	}
	leaderSyncWg.wait();
	if (!lastErr.ok()) {
		throw lastErr;
	}
}

Error DataReplicator::syncLatestNamespaceToLeader(string_view nsName, DataReplicator::SyncThread& threadLocal) {
	// This stage will not be required if each ns will have it's own leader
	// 1) Find most recent data among all the followers
	lsn_t localLsn, latestLsn;
	uint64_t dataHash = 0;
	int latestLsnOwner = -1;
	{
		ReplicationStateV2 state;
		auto err = thisNode_.GetReplState(nsName, state);
		if (err.ok()) {
			localLsn = latestLsn = state.lastLsn;
			dataHash = state.dataHash;
		}
		std::cerr << serverId_ << ": Begin leader's sync for namespace " << nsName << " (lsn: " << localLsn << ")" << std::endl;
	}
	size_t responses = 1;
	coroutine::wait_group wg;
	for (int id = 0; size_t(id) < threadLocal.nodes.size(); ++id) {
		wg.add(1);
		threadLocal.loop.spawn([this, id, &threadLocal, &wg, &nsName, &latestLsn, &dataHash, &latestLsnOwner, &responses] {
			coroutine::wait_group_guard wgg(wg);
			ReplicationStateV2 state;
			auto err = threadLocal.nodes[id].client.GetReplState(nsName, state);
			if (err.ok()) {
				++responses;
				std::cerr << serverId_ << ": Got lsn " << state.lastLsn << " from " << id << " for " << nsName << std::endl;
				if (latestLsn.isEmpty() || latestLsn.Counter() < state.lastLsn.Counter()) {
					latestLsn = state.lastLsn;
					dataHash = state.dataHash;
					latestLsnOwner = id;
				}
			} else if (err.code() == errNotFound) {
				++responses;
			}
		});
	}
	wg.wait();
	if (responses < consensusCnt_) {
		return Error(errClusterConsensus, "Unable to get enough responses. Got %d out of %d", responses, consensusCnt_);
	}

	// 2) Recv most recent data
	std::string tmpNsName;
	std::cerr << serverId_ << ":Local lsn counter: " << localLsn.Counter() << "; latestLsn counter: " << latestLsn.Counter() << std::endl;
	try {
		if (localLsn != latestLsn) {
			auto sync = [this, &tmpNsName, &nsName, &latestLsnOwner, &threadLocal, &localLsn](bool forced) {
				assert(latestLsnOwner >= 0);
				client::Snapshot snapshot;
				auto err = threadLocal.nodes[latestLsnOwner].client.GetSnapshot(nsName, forced ? lsn_t() : localLsn, snapshot);
				if (!err.ok()) {
					throw err;
				}

				RdxContext ctx;
				ctx.WithNoWaitSync();
				auto ns = thisNode_.getNamespaceNoThrow(nsName, ctx);
				if (!ns || snapshot.HasRawData()) {
					// TODO: Allow tmp ns without storage via config
					err = thisNode_.CreateTemporaryNamespace(nsName, tmpNsName, StorageOpts().Enabled());
					if (!err.ok()) {
						throw err;
					}
					ns = thisNode_.getNamespaceNoThrow(tmpNsName, ctx);
					assert(ns);
				}

				for (auto& ch : snapshot) {
					if (terminate_) {
						return;
					}
					ns->ApplySnapshotChunk(ch.Chunk(), ctx);
				}

				if (!tmpNsName.empty()) {
					err = thisNode_.renameNamespace(tmpNsName, std::string(nsName), true);
					if (!err.ok()) {
						throw err;
					}
				}
			};

			for (int retry = 0; retry < 2; ++retry) {
				const bool fullResync = retry > 0;
				sync(fullResync);
				ReplicationStateV2 state;
				auto err = thisNode_.GetReplState(nsName, state);
				if (!err.ok()) {
					throw err;
				}
				localLsn = latestLsn = state.lastLsn;
				if (state.dataHash == dataHash) {
					std::cerr << serverId_ << ":Local namespace " << nsName << " was updated from " << latestLsnOwner << " (lsn is "
							  << localLsn << ")" << std::endl;
					break;
				}

				if (fullResync) {
					throw Error(errDataHashMismatch,
								"%d: Datahash missmatch after full resync for local namespce '%s'. Expected: %d; actual: %d", serverId_,
								nsName, dataHash, state.dataHash);
				}
				std::cerr << serverId_ << ": Datahash missmatch after local namespace " << nsName << " sync. Expected: " << dataHash
						  << "; actual: " << state.dataHash << ". Forcing full resync..." << std::endl;
			}
		} else {
			std::cerr << serverId_ << ":Local namespace " << nsName << " is up-to-date (lsn is " << localLsn << ")" << std::endl;
		}
	} catch (Error& err) {
		std::cerr << serverId_ << ": Unable to sync local namespace " << nsName << ": " << err.what() << std::endl;
		if (!tmpNsName.empty()) {
			thisNode_.DropNamespace(tmpNsName);
		}
		return err;
	}
	return errOK;
}

DataReplicator::NsNamesHashSetT DataReplicator::collectNsNames(SyncThread& threadLocal) {
	NsNamesHashSetT result;

	coroutine::wait_group wg;
	size_t responses = 1;
	for (auto& node : threadLocal.nodes) {
		wg.add(1);
		threadLocal.loop.spawn([this, &node, &result, &wg, &responses] {
			coroutine::wait_group_guard wgg(wg);
			std::vector<NamespaceDef> nsDefs;
			auto err = node.client.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem());
			if (err.ok()) {
				++responses;
				for (auto&& def : nsDefs) {
					result.emplace(std::move(def.name));
				}
			} else {
				std::cerr << serverId_ << ": EnumNamespaces error: " << err.what() << std::endl;
			}
		});
	}
	wg.wait();
	if (responses < consensusCnt_) {
		throw Error(errClusterConsensus, "Unable to get enough responses. Got %d out of %d", responses, consensusCnt_);
	}

	return result;
}

bool DataReplicator::startUpdateRoutineForNewNs(const std::string& name, const std::shared_ptr<UpdatesQueueShardT>& shard,
												DataReplicator::SyncThread& threadLocal) {
	auto& listeners = threadLocal.nsUpdatesData[name].listeners;
	// If new namespace has appeared, start replication routines for it
	if (listeners.Empty()) {
		string_view nsName = shard->GetTokens().find(name).key();  // Get string view to actual storage
		std::cerr << serverId_ << ":Starting new updates routines: " << nsName << std::endl;
		for (size_t j = 0; j < threadLocal.nodes.size(); ++j) {
			auto& nodeNotifiers = threadLocal.nodes[j].updateNotifiers;
			nodeNotifiers.emplace_back(nsName);
			listeners.Add(&nodeNotifiers.back().ch);
			threadLocal.nodes[j].wg.add(1);
			threadLocal.loop.spawn(
				[this, j, nsName, &threadLocal] {
					coroutine::wait_group_guard wgg(threadLocal.nodes[j].wg);
					nsReplicationRoutine(j, nsName, threadLocal, true);
				},
				kNsReplicationRoutineStackSize);
		}
		return true;
	}
	return false;
}

Error DataReplicator::syncNamespace(size_t nodeId, string_view nsName, const ReplicationStateV2& followerState, SyncThread& threadLocal) {
	try {
		struct TmpNsGuard {
			std::string tmpNsName;
			client::CoroReindexer& client;
			int serverId;

			~TmpNsGuard() {
				if (tmpNsName.size()) {
					std::cerr << "Removing tmp ns on error: " << tmpNsName << std::endl;
					client.WithLSN(lsn_t(0, serverId)).DropNamespace(tmpNsName);
				}
			}
		};

		ReplicationStateV2 localState;
		Snapshot snapshot;
		lsn_t requiredLsn = followerState.lastLsn;
		bool createTmpNamespace = false;
		auto& client = threadLocal.nodes[nodeId].client;
		TmpNsGuard tmpNsGuard = {std::string(), client, serverId_};

		auto err = thisNode_.GetReplState(nsName, localState);
		if (!err.ok()) {
			if (err.code() == errNotFound && followerState.lastLsn.isEmpty()) {
				std::cerr << serverId_ << ":" << nodeId << ": Namespace " << nsName << " doesn't exist on both follower and leader"
						  << std::endl;
				return errOK;
			}
			return err;
		}

		std::cerr << serverId_ << ": ReplState for " << nodeId << ":" << nsName << ": { local: { lsn: " << localState.lastLsn
				  << ", dataHash: " << localState.dataHash << " }, remote: { lsn: " << followerState.lastLsn
				  << ", dataHash: " << followerState.dataHash << " } }" << std::endl;

		if (!requiredLsn.isEmpty()) {
			if (followerState.lastLsn.Counter() > localState.lastLsn.Counter()) {
				std::cerr << serverId_ << ":" << nodeId << ": Unexpected follower's lsn: " << followerState.lastLsn
						  << "; local lsn: " << localState.lastLsn << std::endl;
				requiredLsn = lsn_t();
			} else if (followerState.lastLsn == localState.lastLsn && followerState.dataHash != localState.dataHash) {
				std::cerr << serverId_ << ":" << nodeId << ": Datahash missmatch. Expected: " << localState.dataHash
						  << "; Actual: " << followerState.dataHash << std::endl;
				requiredLsn = lsn_t();
			} else if (followerState.lastLsn.Counter() == localState.lastLsn.Counter() &&
					   followerState.lastLsn.Server() != localState.lastLsn.Server()) {
				std::cerr << serverId_ << ":" << nodeId << ": Unexpected follower lsn: " << followerState.lastLsn
						  << "; local lsn: " << localState.lastLsn << "(lsns have different server ids)" << std::endl;
				requiredLsn = lsn_t();
			}
		}

		err = thisNode_.GetSnapshot(nsName, requiredLsn, snapshot);
		if (!err.ok()) return err;
		if (snapshot.HasRawData()) {
			std::cerr << serverId_ << ":" << nodeId << ": Snapshot has raw data, creating tmp namespace" << std::endl;
			createTmpNamespace = true;
		}

		string_view replNsName;
		if (createTmpNamespace) {
			// TODO: Allow tmp ns without storage via config
			err = client.WithLSN(lsn_t(0, serverId_)).CreateTemporaryNamespace(nsName, tmpNsGuard.tmpNsName);
			if (!err.ok()) return err;
			replNsName = tmpNsGuard.tmpNsName;
		} else {
			replNsName = nsName;
		}
		for (auto& it : snapshot) {
			if (!isLeader(threadLocal) && threadLocal.nodes[nodeId].type == ReplicatorNode::Type::Cluster) {
				return Error(errLogic, "Leader was changed");
			}
			if (terminate_) {
				std::cerr << serverId_ << ":: Terminated, while syncing namespace" << std::endl;
				return errOK;
			}
			err = client.WithLSN(lsn_t(0, serverId_)).ApplySnapshotChunk(replNsName, it.Chunk());
			if (!err.ok()) {
				return err;
			}
		}
		if (createTmpNamespace) {
			err = client.WithLSN(lsn_t(0, serverId_)).RenameNamespace(replNsName, std::string(nsName));
			if (!err.ok()) return err;
			tmpNsGuard.tmpNsName.clear();
		}

		{
			ReplicationStateV2 replState;
			err = client.GetReplState(nsName, replState);
			if (!err.ok() && err.code() != errNotFound) return err;
			std::cerr << serverId_ << ":" << nodeId << ": Sync done for " << nodeId << ":" << nsName
					  << ": { snapshot: { lsn: " << snapshot.LastLSN() << ", dataHash: " << snapshot.ExpectedDatahash()
					  << " }, remote: { lsn: " << replState.lastLsn << ", dataHash: " << replState.dataHash << " } }" << std::endl;

			threadLocal.nodes[nodeId].namespaceData[nsName].latestLsn = replState.lastLsn;

			bool dataMissmatch = !snapshot.LastLSN().isEmpty() && snapshot.LastLSN() != replState.lastLsn;
			if (dataMissmatch || snapshot.ExpectedDatahash() != replState.dataHash) {
				std::cerr << "Snapshot dump on data missmatch:\n" << snapshot.Dump() << std::endl;
				return Error(
					errDataHashMismatch,
					"%d:%d:%s: Datahash missmatcher after sync. Actual: { datahash: %d, lsn: %d }; expected: { datahash: %d, lsn: %d }",
					serverId_, nodeId, nsName, replState.dataHash, replState.lastLsn, snapshot.ExpectedDatahash(), snapshot.LastLSN());
			}
		}
	} catch (Error& err) {
		return err;
	}
	return errOK;
}

Error DataReplicator::nsUpdatesHandlingLoop(size_t nodeId, string_view nsName, DataReplicator::SyncThread& threadLocal) {
	UpdatesChT* updatesNotifier = nullptr;
	for (auto& updCh : threadLocal.nodes[nodeId].updateNotifiers) {
		if (updCh.name == nsName) {
			updatesNotifier = &updCh.ch;
			break;
		}
	}
	assert(updatesNotifier);
	auto& node = threadLocal.nodes[nodeId];
	auto& namespaceData = threadLocal.nodes[nodeId].namespaceData[nsName];
	auto found = threadLocal.nsUpdatesData.find(std::string(nsName));
	assert(found != threadLocal.nsUpdatesData.end());
	auto& updatesList = found->second.updates;
	std::cerr << serverId_ << ": Start updates handling loop: " << nodeId << ":" << nsName << "; lates known lsn is "
			  << threadLocal.nodes[nodeId].namespaceData[nsName].latestLsn << std::endl;
	bool requestElectionsOnNextError = true;

	while (!terminate_) {
		std::cerr << serverId_ << ":" << nodeId << ": Awaiting updates" << std::endl;
		auto updP = updatesNotifier->pop();
		if (updP.second) {
			std::cerr << serverId_ << ":" << nodeId << ": Got new update from tmp queue" << std::endl;
			Error err;
			auto it = updP.first;
			while (it != updatesList.end() && !terminate_) {
				bool isEmmiter = it->data.emmiterServerId == node.serverId;
				assert(it->data.emmiterServerId != serverId_);

				if (err.ok()) {
					if (namespaceData.latestLsn.isEmpty() || it->data.lsn.Counter() > namespaceData.latestLsn.Counter()) {
						std::cerr << serverId_ << ":" << nodeId << ":" << nsName << ": Applying update with type: " << int(it->data.type)
								  << "; lsn: " << it->data.lsn << "; last synced lsn: " << namespaceData.latestLsn << std::endl;
						err = applyUpdate(it->data, node, namespaceData);
						if (err.ok() && it->data.type != UpdateRecord::Type::AddNamespace &&
							it->data.type != UpdateRecord::Type::RenameNamespace && it->data.type != UpdateRecord::Type::DropNamespace) {
							// Updates with *Namespace types have fake lsn. Those updates should not be count in latestLsn
							namespaceData.latestLsn = it->data.lsn;
						}
					} else {
						std::cerr << serverId_ << ":" << nodeId << ":" << nsName << ": Skipping update with type: " << int(it->data.type)
								  << "; lsn: " << it->data.lsn << "; last synced lsn: " << namespaceData.latestLsn << std::endl;
					}
				}
				if (isEmmiter) {
					it->data.emmiterServerId = -1;
				}
				if (err.ok()) {
					requestElectionsOnNextError = true;
					if (node.type == ReplicatorNode::Type::Cluster) {
						++it->aproves;
						if ((isEmmiter && it->aproves >= consensusCnt_) ||
							(!isEmmiter && it->data.emmiterServerId == -1 && it->aproves == consensusCnt_)) {
							it->OnResult(errOK);
						}
					}
				} else {
					if (node.type == ReplicatorNode::Type::Cluster && ++it->errors == consensusCnt_) {
						it->OnResult(
							Error(errUpdateReplication, "Unable to send update to enough amount of replicas. Last error: %s", err.what()));
						if (requestElectionsOnNextError) {
							std::cerr << serverId_ << ":" << nodeId << ":" << nsName << ":Requesting for electon:" << err.what()
									  << ";errors: " << it->errors << ";consensus:" << consensusCnt_ << ";lsn" << it->data.lsn << std::endl;

							requestElectionsOnNextError = false;
							requestElectionsRestartCb_();
						}
					}
				}
				std::cerr << serverId_ << ":" << nodeId << ":" << nsName << ":apply:" << (err.ok() ? "OK" : "ERROR:" + err.what())
						  << "; Required: " << consensusCnt_ << "; succeed: " << it->aproves << "; failed: " << it->errors
						  << "; replicas: " << it->replicas + 1 << std::endl;
				if (++it->replicas == threadLocal.nodes.size()) {
					it = updatesList.erase(it);
					std::cerr << serverId_ << ":" << nodeId << ":" << nsName
							  << ": Removing update from tmp queue; updatesList size: " << updatesList.size() << std::endl;
				} else {
					++it;
				}
			}
			if (!updatesNotifier->empty()) {
				updatesNotifier->pop();
			}
			if (!err.ok()) {
				return err;
			}
		} else {
			break;
		}
	}
	return errOK;
}

void DataReplicator::handleUpdatesWithError(size_t nodeId, string_view nsName, DataReplicator::SyncThread& threadLocal, const Error& err) {
	UpdatesChT* updatesNotifier = nullptr;
	for (auto& updCh : threadLocal.nodes[nodeId].updateNotifiers) {
		if (updCh.name == nsName) {
			updatesNotifier = &updCh.ch;
			break;
		}
	}
	assert(updatesNotifier);
	auto& node = threadLocal.nodes[nodeId];

	if (updatesNotifier->empty() || !updatesNotifier->opened()) {
		return;
	}
	auto found = threadLocal.nsUpdatesData.find(std::string(nsName));
	assert(found != threadLocal.nsUpdatesData.end());
	auto& updatesList = found->second.updates;
	auto updP = updatesNotifier->pop();
	assert(updP.second);
	std::cerr << serverId_ << ":" << nodeId << ": Dropping updates from tmp queue" << std::endl;
	auto it = updP.first;
	bool requestElectionsOnNextError = true;
	while (it != updatesList.end()) {
		if (node.type == ReplicatorNode::Type::Cluster && ++it->errors == consensusCnt_) {
			it->OnResult(Error(errUpdateReplication, "Unable to send update to enough amount of replicas. Last error: %s", err.what()));
			if (requestElectionsOnNextError) {
				requestElectionsOnNextError = false;
				requestElectionsRestartCb_();
			}
		}
		std::cerr << serverId_ << ":" << nodeId << ":" << nsName << ":drop:" << err.what() << "; lsn: " << it->data.lsn
				  << "; Required: " << consensusCnt_ << "; succeed: " << it->aproves << "; failed: " << it->errors
				  << "; replicas: " << it->replicas + 1 << std::endl;
		if (++it->replicas == threadLocal.nodes.size()) {
			it = updatesList.erase(it);
			std::cerr << serverId_ << ":" << nodeId << ":" << nsName
					  << ": Removing update from tmp queue; updatesList size: " << updatesList.size() << std::endl;
		} else {
			++it;
		}
	}
}

void DataReplicator::onTerminateAsync(net::ev::async& watcher) noexcept {
	for (auto& th : syncThreads_) {
		th.roleSwitchAsync.send();	// This will close channels
		th.updatesAsync.send();		// This will close channels
	}
	sharedSyncState_.SetTerminated();
	watcher.loop.break_loop();
}

void DataReplicator::onRoleSwitchAsync(net::ev::async&) {
	auto rolesPair = sharedSyncState_.GetRolesPair();
	auto& newState = rolesPair.second;
	auto& curState = rolesPair.first;
	bool notify = newState != curState;
	while (newState != curState) {
		std::cerr << serverId_ << ": onRoleSwitchAsync: " << RaftInfo::RoleToStr(curState.role) << " -> "
				  << RaftInfo::RoleToStr(newState.role) << std::endl;
		auto nsNames = config_.namespaces;
		if (nsNames.empty()) {
			std::vector<NamespaceDef> nsDefs;
			thisNode_.EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().HideTemporary().HideSystem());
			nsNames.reserve(nsDefs.size());
			for (auto& ns : nsDefs) {
				nsNames.emplace(std::move(ns.name));
			}
		}
		RdxContext ctx;
		ctx.WithNoWaitSync(true);
		for (auto& nsName : nsNames) {
			// 0) Mark local namespaces with new leader data
			auto nsPtr = thisNode_.getNamespaceNoThrow(nsName, ctx);
			NsClusterizationStatus status;
			switch (newState.role) {
				case RaftInfo::Role::None:
				case RaftInfo::Role::Leader:
					status.role = NsClusterizationStatus::Role::None;  // -V1048
					break;
				case RaftInfo::Role::Follower:
					status.role = NsClusterizationStatus::Role::ClusterReplica;
					break;
				case RaftInfo::Role::Candidate:
					status.role = (curState.role == RaftInfo::Role::Leader) ? NsClusterizationStatus::Role::None
																			: NsClusterizationStatus::Role::ClusterReplica;
					break;
			}
			status.leaderId = newState.leaderId;
			std::cerr << serverId_ << ": Setting new leader id " << RaftInfo::RoleToStr(newState.role) << ":" << status.leaderId << " for "
					  << nsName << std::endl;
			nsPtr->SetClusterizationStatus(std::move(status), ctx);
		}
		curState = newState;
		newState = sharedSyncState_.TryTransitRole(newState);
	}

	if (notify) {
		for (auto& worker : syncThreads_) {
			worker.roleSwitchAsync.send();
		}
	}
}

Error DataReplicator::applyUpdate(const UpdateRecord& rec, DataReplicator::ReplicatorNode& node,
								  DataReplicator::NamespaceData& nsData) noexcept {
	auto lsn = rec.lsn;
	string_view nsName = rec.GetNsName();
	auto& client = node.client;
	try {
		switch (rec.type) {
			case UpdateRecord::Type::ItemUpdate:
			case UpdateRecord::Type::ItemUpsert:
			case UpdateRecord::Type::ItemDelete:
			case UpdateRecord::Type::ItemInsert: {
				auto& data = mpark::get<std::unique_ptr<ItemReplicationRecord>>(rec.data);
				client::Item item = client.NewItem(nsName);
				auto err = item.FromJSON(string_view(data->cjson));
				if (err.ok()) {
					switch (rec.type) {
						case UpdateRecord::Type::ItemUpdate:
							return client.WithLSN(lsn).Update(nsName, item);
						case UpdateRecord::Type::ItemUpsert:
							return client.WithLSN(lsn).Upsert(nsName, item);
						case UpdateRecord::Type::ItemDelete:
							return client.WithLSN(lsn).Delete(nsName, item);
						case UpdateRecord::Type::ItemInsert:
							return client.WithLSN(lsn).Insert(nsName, item);
						default:
							assert(false);
					}
				}
				return err;
			}
			case UpdateRecord::Type::IndexAdd: {
				auto& data = mpark::get<std::unique_ptr<IndexReplicationRecord>>(rec.data);
				return client.WithLSN(lsn).AddIndex(nsName, data->idef);
			}
			case UpdateRecord::Type::IndexDrop: {
				auto& data = mpark::get<std::unique_ptr<IndexReplicationRecord>>(rec.data);
				return client.WithLSN(lsn).DropIndex(nsName, data->idef);
			}
			case UpdateRecord::Type::IndexUpdate: {
				auto& data = mpark::get<std::unique_ptr<IndexReplicationRecord>>(rec.data);
				return client.WithLSN(lsn).UpdateIndex(nsName, data->idef);
			}
			case UpdateRecord::Type::PutMeta: {
				auto& data = mpark::get<std::unique_ptr<MetaReplicationRecord>>(rec.data);
				return client.WithLSN(lsn).PutMeta(nsName, data->key, data->value);
			}
			case UpdateRecord::Type::UpdateQuery: {
				auto& data = mpark::get<std::unique_ptr<QueryReplicationRecord>>(rec.data);
				client::CoroQueryResults qr;
				return client.WithLSN(lsn).Update(data->q, qr);
			}
			case UpdateRecord::Type::DeleteQuery: {
				auto& data = mpark::get<std::unique_ptr<QueryReplicationRecord>>(rec.data);
				client::CoroQueryResults qr;
				return client.WithLSN(lsn).Delete(data->q, qr);
			}
			case UpdateRecord::Type::SetSchema: {
				auto& data = mpark::get<std::unique_ptr<SchemaReplicationRecord>>(rec.data);
				return client.WithLSN(lsn).SetSchema(nsName, data->schema);
			}
			case UpdateRecord::Type::Truncate: {
				return client.WithLSN(lsn).TruncateNamespace(nsName);
			}
			case UpdateRecord::Type::BeginTx: {
				assert(nsData.tx.IsFree());
				nsData.tx = node.client.WithLSN(lsn).NewTransaction(nsName);
				return nsData.tx.Status();
			}
			case UpdateRecord::Type::CommitTx: {
				assert(!nsData.tx.IsFree());
				return node.client.WithLSN(lsn).CommitTransaction(nsData.tx);
			}
			case UpdateRecord::Type::ItemUpdateTx:
			case UpdateRecord::Type::ItemUpsertTx:
			case UpdateRecord::Type::ItemDeleteTx:
			case UpdateRecord::Type::ItemInsertTx: {
				assert(!nsData.tx.IsFree());  // TODO: Maybe we should remove this assert and handle this error somehow
				auto& data = mpark::get<std::unique_ptr<ItemReplicationRecord>>(rec.data);
				client::Item item = nsData.tx.NewItem();
				auto err = item.FromJSON(string_view(data->cjson));
				if (err.ok()) {
					switch (rec.type) {
						case UpdateRecord::Type::ItemUpdateTx:
							return nsData.tx.Update(std::move(item), lsn);
						case UpdateRecord::Type::ItemUpsertTx:
							return nsData.tx.Upsert(std::move(item), lsn);
						case UpdateRecord::Type::ItemDeleteTx:
							return nsData.tx.Delete(std::move(item), lsn);
						case UpdateRecord::Type::ItemInsertTx:
							return nsData.tx.Insert(std::move(item), lsn);
						default:
							assert(false);
					}
				}
				return err;
			}
			case UpdateRecord::Type::UpdateQueryTx:
			case UpdateRecord::Type::DeleteQueryTx: {
				assert(!nsData.tx.IsFree());
				auto& data = mpark::get<std::unique_ptr<QueryReplicationRecord>>(rec.data);
				return nsData.tx.Modify(Query(data->q), lsn);
			}
			case UpdateRecord::Type::AddNamespace: {
				auto& data = mpark::get<std::unique_ptr<AddNamespaceReplicationRecord>>(rec.data);
				lsn.SetServer(serverId_);
				return client.WithLSN(lsn).AddNamespace(data->def);
			}
			case UpdateRecord::Type::DropNamespace: {
				return client.WithLSN(lsn).DropNamespace(nsName);
			}
			case UpdateRecord::Type::RenameNamespace: {
				auto& data = mpark::get<std::unique_ptr<RenameNamespaceReplicationRecord>>(rec.data);
				lsn.SetServer(serverId_);
				return client.WithLSN(lsn).RenameNamespace(nsName, data->dstNsName);
			}
			default:
				assert(false);
		}
	} catch (mpark::bad_variant_access&) {
		assert(false);
	}
	return errOK;
}

bool DataReplicator::isNetworkError(const Error& err) noexcept {
	return err.code() == errNetwork || err.code() == errTimeout || err.code() == errCanceled;
}

bool DataReplicator::isLeaderChangedError(const Error& err) noexcept { return err.code() == errWrongReplicationData; }

}  // namespace cluster
}  // namespace reindexer
