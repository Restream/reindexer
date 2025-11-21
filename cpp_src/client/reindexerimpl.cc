#include "client/reindexerimpl.h"
#include "client/connectionspool.h"
#include "cluster/sharding/shardingcontrolrequest.h"
#include "estl/dummy_mutex.h"
#include "tools/catch_and_return.h"
#include "tools/dsn.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;
constexpr size_t kAsyncCoroStackSize = 32 * 1024;

ReindexerImpl::ReindexerImpl(const ReindexerConfig& conf, uint32_t connCount, uint32_t threadsCount) : conf_(conf) {
	const auto conns = std::max(connCount, 1u);
	if (threadsCount > 1) {
		const auto connsPerThread = conns / threadsCount;
		auto mod = conns % threadsCount;
		for (unsigned i = 0; i < threadsCount; ++i) {
			if (mod) {
				--mod;
				workers_.emplace_back(connsPerThread + 1);
			} else if (connsPerThread) {
				workers_.emplace_back(connsPerThread);
			}
		}
	} else {
		workers_.emplace_back(conns);
	}
	sharedNamespaces_ = (workers_.size() > 1) ? INamespaces::PtrT(new NamespacesImpl<shared_timed_mutex>())
											  : INamespaces::PtrT(new NamespacesImpl<DummyMutex>());
	commandsQueue_.Init(workers_);
}

ReindexerImpl::~ReindexerImpl() { stop(); }

Error ReindexerImpl::Connect(const DSN& dsn, const client::ConnectOpts& opts) {
	lock_guard lock(workersMtx_);
	if (conf_.SyncRxCoroCount < 1 || conf_.SyncRxCoroCount > 10'000) {
		return Error(errParams,
					 "The number of synchronization coroutines (SyncRxCoroCount in conf) must be in the range [1..10'000]. But was: {}",
					 conf_.SyncRxCoroCount);
	}
	if (workers_.size() && workers_[0].th.joinable()) {
		return Error(errLogic, "Client is already started. DSN: {}", dsn);
	}
	lastError_.Set(Error());
	runningWorkers_ = 0;
	commandsQueue_.ClearConnectionsMapping();

	std::promise<Error> isRunningPromise;
	auto isRunningFuture = isRunningPromise.get_future();
	for (uint32_t i = 0; i < workers_.size(); ++i) {
		workers_[i].th = std::thread([this, &isRunningPromise, dsn, opts, i] { this->threadLoopFun(i, isRunningPromise, dsn, opts); });
	}
	auto ret = isRunningFuture.get();
	if (ret.ok()) {
		ret = lastError_.Get();
	}
	if (!ret.ok()) {
		stop();
	}
	commandsQueue_.SetValid();
	return ret;
}

void ReindexerImpl::Stop() {
	lock_guard lock(workersMtx_);
	requiresStatusCheck_.store(true, std::memory_order_relaxed);
	stop();
}
Error ReindexerImpl::OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx, const StorageOpts& opts,
								   const NsReplicationOpts& replOpts) {
	return sendCommand<Error>(DbCmdOpenNamespace, ctx, std::move(nsName), opts, replOpts);
}
Error ReindexerImpl::AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx, const NsReplicationOpts& replOpts) {
	return sendCommand<Error>(DbCmdAddNamespace, ctx, nsDef, replOpts);
}
Error ReindexerImpl::CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdCloseNamespace, ctx, std::move(nsName));
}
Error ReindexerImpl::DropNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdDropNamespace, ctx, std::move(nsName));
}
Error ReindexerImpl::TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdTruncateNamespace, ctx, std::move(nsName));
}
Error ReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdRenameNamespace, ctx, std::move(srcNsName), dstNsName);
}
Error ReindexerImpl::AddIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdAddIndex, ctx, std::move(nsName), index);
}
Error ReindexerImpl::UpdateIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdUpdateIndex, ctx, std::move(nsName), index);
}
Error ReindexerImpl::DropIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdDropIndex, ctx, std::move(nsName), index);
}
Error ReindexerImpl::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdSetSchema, ctx, std::move(nsName), std::move(schema));
}
Error ReindexerImpl::GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdGetSchema, ctx, std::move(nsName), std::move(format), schema);
}
Error ReindexerImpl::EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdEnumNamespaces, ctx, defs, std::move(opts));
}
Error ReindexerImpl::EnumDatabases(std::vector<std::string>& dbList, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdEnumDatabases, ctx, dbList);
}
Error ReindexerImpl::Version(std::string& version, const InternalRdxContext& ctx) { return sendCommand<Error>(DbCmdVersion, ctx, version); }
Error ReindexerImpl::Insert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdInsert, ctx, std::move(nsName), item, std::move(format));
}
Error ReindexerImpl::Insert(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdInsertQR, ctx, std::move(nsName), item, result.results_);
}

Error ReindexerImpl::Update(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdUpdate, ctx, std::move(nsName), item, std::move(format));
}
Error ReindexerImpl::Update(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdUpdateQR, ctx, std::move(nsName), item, result.results_);
}

Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdUpsert, ctx, std::move(nsName), item, std::move(format));
}
Error ReindexerImpl::Upsert(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdUpsertQR, ctx, std::move(nsName), item, result.results_);
}

Error ReindexerImpl::Update(const Query& query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = result.setClient(shared_from_this());
	if (!err.ok()) {
		return err;
	}
	return sendCommand<Error>(DbCmdUpdateQ, ctx, query, result.results_);
}
Error ReindexerImpl::Delete(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdDelete, ctx, std::move(nsName), item, std::move(format));
}
Error ReindexerImpl::Delete(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdDeleteQR, ctx, std::move(nsName), item, result.results_);
}

Error ReindexerImpl::Delete(const Query& query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = result.setClient(shared_from_this());
	if (!err.ok()) {
		return err;
	}
	return sendCommand<Error>(DbCmdDeleteQ, ctx, query, result.results_);
}
Error ReindexerImpl::ExecSQL(std::string_view query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = result.setClient(shared_from_this());
	if (!err.ok()) {
		return err;
	}
	return sendCommand<Error>(DbCmdExecSQL, ctx, std::move(query), result.results_);
}
Error ReindexerImpl::Select(const Query& query, QueryResults& result, const InternalRdxContext& ctx) {
	Error err = result.setClient(shared_from_this());
	if (!err.ok()) {
		return err;
	}
	return sendCommand<Error>(DbCmdSelect, ctx, query, result.results_);
}

Item ReindexerImpl::NewItem(std::string_view nsName, const InternalRdxContext& ctx) noexcept {
	try {
		return sendCommand<Item>(DbCmdNewItem, ctx, std::move(nsName));
	} catch (std::exception& e) {
		return Item(e);
	} catch (...) {
		return Item(Error{errSystem, "Unexpected error in client::Reindexer"});
	}
}

Error ReindexerImpl::GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdGetMeta, ctx, std::move(nsName), key, data);
}
Error ReindexerImpl::GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data,
							 const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdGetShardedMeta, ctx, std::move(nsName), key, data);
}
Error ReindexerImpl::PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdPutMeta, ctx, std::move(nsName), key, std::move(data));
}
Error ReindexerImpl::EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdEnumMeta, ctx, std::move(nsName), keys);
}
Error ReindexerImpl::DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdDeleteMeta, ctx, std::move(nsName), key);
}
Error ReindexerImpl::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions) {
	return sendCommand<Error>(DbCmdGetSqlSuggestions, InternalRdxContext(), std::move(sqlQuery), std::move(pos), suggestions);
}

Error ReindexerImpl::Status(bool forceCheck, const InternalRdxContext& ctx) {
	// Skip actual status check if latest connection state was 'online'
	if (requiresStatusCheck_.load(std::memory_order_relaxed)) {
		return sendCommand<Error>(DbCmdStatus, ctx, std::move(forceCheck));
	}
	if (ctx.cmpl()) {
		ctx.cmpl()(Error());
	}
	return Error();
}

CoroTransaction ReindexerImpl::NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) noexcept {
	try {
		return sendCommand<CoroTransaction>(DbCmdNewTransaction, ctx, std::move(nsName));
	} catch (std::exception& e) {
		return CoroTransaction(e);
	} catch (...) {
		return CoroTransaction(Error{errSystem, "Unexpected error in client::Reindexer"});
	}
}

Error ReindexerImpl::CommitTransaction(Transaction& tr, QueryResults& results, const InternalRdxContext& ctx) {
	if (tr.IsFree()) {
		return Error(errBadTransaction, "Attempt to commit empty transaction");
	}
	if (tr.rx_.get() != this) {
		return Error(errTxInvalidLeader, "Attempt to commit transaction to the incorrect leader");
	}
	return sendCommand<true, Error, CoroTransaction&, CoroQueryResults&>(tr.coroConnection(), DbCmdCommitTransaction, ctx, tr.tr_,
																		 results.results_);
}

Error ReindexerImpl::RollBackTransaction(Transaction& tr, const InternalRdxContext& ctx) {
	if (tr.IsFree()) {
		return tr.Status();
	}
	if (tr.rx_.get() != this) {
		return Error(errLogic, "Attempt to rollback transaction on the incorrect leader");
	}
	return sendCommand<true, Error, CoroTransaction&>(tr.coroConnection(), DbCmdRollBackTransaction, ctx, tr.tr_);
}

Error ReindexerImpl::GetReplState(std::string_view nsName, ReplicationStateV2& state, const InternalRdxContext& ctx) {
	return sendCommand<Error>(DbCmdGetReplState, ctx, std::move(nsName), state);
}

Error ReindexerImpl::SaveNewShardingConfig(std::string_view config, int64_t sourceId, const InternalRdxContext& ctx) noexcept {
	RETURN_RESULT_NOEXCEPT(sendCommand<Error, std::string_view, int64_t>(DbCmdSaveNewShardingCfg, ctx, std::move(config),
																		 std::move(sourceId)))}

Error ReindexerImpl::ResetShardingConfigCandidate(int64_t sourceId, const InternalRdxContext& ctx) noexcept {
	RETURN_RESULT_NOEXCEPT(sendCommand<Error, int64_t>(DbCmdResetConfigCandidate, ctx, std::move(sourceId)))}

Error ReindexerImpl::ResetOldShardingConfig(int64_t sourceId, const InternalRdxContext& ctx) noexcept {
	RETURN_RESULT_NOEXCEPT(sendCommand<Error, int64_t>(DbCmdResetOldShardingCfg, ctx, std::move(sourceId)))}

Error ReindexerImpl::RollbackShardingConfigCandidate(int64_t sourceId, const InternalRdxContext& ctx) noexcept {
	RETURN_RESULT_NOEXCEPT(sendCommand<Error, int64_t>(DbCmdRollbackConfigCandidate, ctx, std::move(sourceId)))}

Error ReindexerImpl::ApplyNewShardingConfig(int64_t sourceId, const InternalRdxContext& ctx) noexcept {
	RETURN_RESULT_NOEXCEPT(sendCommand<Error, int64_t>(DbCmdApplyNewShardingCfg, ctx, std::move(sourceId)))}

Error ReindexerImpl::fetchResults(int flags, int offset, int limit, QueryResults& result) {
	return sendCommand<true, Error>(result.coroConnection(), DbCmdFetchResultsParametrized, InternalRdxContext(), std::move(flags),
									std::move(offset), std::move(limit), result.results_);
}

Error ReindexerImpl::fetchResults(int flags, QueryResults& result) {
	return sendCommand<true, Error>(result.coroConnection(), DbCmdFetchResults, InternalRdxContext(), std::move(flags), result.results_);
}

Error ReindexerImpl::closeResults(QueryResults& result) {
	return sendCommand<true, Error>(result.coroConnection(), DbCmdCloseResults, InternalRdxContext(), result.results_);
}

Error ReindexerImpl::addTxItem(Transaction& tr, Item&& item, ItemModifyMode mode, const InternalRdxContext& ctx) {
	return sendCommand<true, Error>(tr.coroConnection(), DbCmdAddTxItem, ctx, tr.tr_, std::move(item), std::move(mode));
}

Error ReindexerImpl::putTxMeta(Transaction& tr, std::string_view key, std::string_view value, const InternalRdxContext& ctx) {
	return sendCommand<true, Error>(tr.coroConnection(), DbCmdPutTxMeta, ctx, tr.tr_, std::move(key), std::move(value));
}

Error ReindexerImpl::setTxTm(Transaction& tr, TagsMatcher&& tm, const InternalRdxContext& ctx) {
	return sendCommand<true, Error>(tr.coroConnection(), DbCmdSetTxTagsMatcher, ctx, tr.tr_, std::move(tm));
}

Error ReindexerImpl::modifyTx(Transaction& tr, Query&& q, const InternalRdxContext& ctx) {
	return sendCommand<true, Error>(tr.coroConnection(), DbCmdModifyTx, ctx, tr.tr_, std::move(q));
}

Item ReindexerImpl::newItemTx(CoroTransaction& tr) {
	return sendCommand<true, Item>(tr.getConn(), DbCmdNewItemTx, InternalRdxContext(), tr);
}

void ReindexerImpl::threadLoopFun(uint32_t tid, std::promise<Error>& isRunning, const DSN& dsn, const client::ConnectOpts& opts) {
	auto& th = workers_[tid];
	assert(sharedNamespaces_);
	if (!th.connData) {
		th.connData = std::make_unique<ConnectionsPoolData>(th.connCount, conf_, sharedNamespaces_);
	}
	ConnectionsPool<DatabaseCommand> connPool(*th.connData);
	struct {
		uint32_t tid;
		ConnectionsPool<DatabaseCommand>& connPool;
	} thData = {tid, connPool};

	th.commandAsync.set(th.loop);
	th.commandAsync.set([this, &thData](net::ev::async&) {
		workers_[thData.tid].loop.spawn(
			[this, &thData]() {
				h_vector<DatabaseCommand, 16> q;
				for (bool readMore = true; readMore;) {
					commandsQueue_.Get(thData.tid, q);
					readMore = q.size();
					for (auto&& cmd : q) {
						auto connIdx = cmd.connIdx;
						assertrx_dbg(connIdx < 0 || uint32_t(connIdx) < thData.connPool.Size());
						auto& conn = (connIdx >= 0 && uint32_t(connIdx) < thData.connPool.Size()) ? thData.connPool.GetConn(connIdx)
																								  : thData.connPool.GetConn();
						assertrx(conn.IsChOpened());
						conn.PushCmd(std::move(cmd));
					}
					q.clear();
				}
			},
			kAsyncCoroStackSize);
	});

	uint32_t runningCount = 0;
	for (size_t connIdx = 0; connIdx < connPool.Size(); ++connIdx) {
		auto& conn = connPool.GetConn(connIdx);
		// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Function may throw exceptions in some rare cases. Is it possible to fix it?
		th.loop.spawn([this, dsn, opts, &isRunning, &conn, connIdx, &thData, &runningCount]() noexcept {
			auto& th = workers_[thData.tid];
			auto err = conn.rx.Connect(dsn, th.loop, opts);
			if (err.ok()) {
				commandsQueue_.RegisterConn(thData.tid, conn.rx.GetConnPtr(), connIdx);
			} else {
				lastError_.Set(err);
			}
			if (++runningCount == th.connCount && ++runningWorkers_ == workers_.size()) {
				isRunning.set_value(err);
			}
			coroutine::wait_group wg;
			for (unsigned n = 0; n < conf_.SyncRxCoroCount; ++n) {
				th.loop.spawn(wg, [this, &conn, &thData]() noexcept { coroInterpreter(conn, thData.connPool, thData.tid); });
			}
			const auto obsID = conn.rx.AddConnectionStateObserver(
				[this](const Error& e) noexcept { requiresStatusCheck_.store(!e.ok(), std::memory_order_relaxed); });
			wg.wait();
			err = conn.rx.RemoveConnectionStateObserver(obsID);
			(void)err;	// ignore

			th.commandAsync.stop();
			th.closeAsync.stop();
			assert(!conn.IsChOpened());
			conn.rx.Stop();
		});
	}
	th.commandAsync.start();

	th.closeAsync.set(th.loop);
	th.closeAsync.set([this, &thData](net::ev::async&) {
		workers_[thData.tid].commandAsync.stop();
		workers_[thData.tid].loop.spawn([this, &thData]() {
			coroutine::wait_group wg;
			for (auto& conn : thData.connPool) {
				if (conn.IsChOpened()) {
					workers_[thData.tid].loop.spawn(wg, [&conn] { conn.rx.Stop(); });
				}
			}
			wg.wait();
			h_vector<DatabaseCommand, 16> q;
			commandsQueue_.Invalidate(thData.tid, q);
			for (auto& conn : thData.connPool) {
				if (conn.IsChOpened()) {
					for (auto&& cmd : q) {
						conn.PushCmd(std::move(cmd));
					}
					q.clear();
					break;
				}
			}
			assert(q.empty());
			for (auto& conn : thData.connPool) {
				conn.CloseCh();
			}
		});
	});
	th.closeAsync.start();

	th.loop.run();
	th.commandAsync.stop();
	requiresStatusCheck_.store(true, std::memory_order_relaxed);
}

void ReindexerImpl::stop() {
	for (auto& w : workers_) {
		w.closeAsync.send();
	}
	for (auto& w : workers_) {
		if (w.th.joinable()) {
			w.th.join();
		}
	}
}

// NOLINTNEXTLINE(bugprone-exception-escape) TODO: Function may throw exceptions in some rare cases. Is it possible to fix it?
void ReindexerImpl::coroInterpreter(Connection<DatabaseCommand>& conn, ConnectionsPool<DatabaseCommand>& pool, uint32_t tid) noexcept {
	using namespace std::placeholders;
	for (std::pair<DatabaseCommand, bool> v = conn.PopCmd(); v.second == true; v = conn.PopCmd()) {
		const auto cmd = v.first.Data();
		switch (cmd->id) {
			case DbCmdOpenNamespace: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const StorageOpts& sopts, const NsReplicationOpts& replOpts) {
					return conn.rx.OpenNamespace(nsName, cmd->ctx, sopts, replOpts);
				});
				break;
			}
			case DbCmdAddNamespace: {
				execCommand(cmd, [&conn, &cmd](const NamespaceDef& nsDef, const NsReplicationOpts& replOpts) {
					return conn.rx.AddNamespace(nsDef, cmd->ctx, replOpts);
				});
				break;
			}
			case DbCmdCloseNamespace: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName) { return conn.rx.CloseNamespace(nsName, cmd->ctx); });
				break;
			}
			case DbCmdDropNamespace: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName) { return conn.rx.DropNamespace(nsName, cmd->ctx); });
				break;
			}
			case DbCmdTruncateNamespace: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName) { return conn.rx.TruncateNamespace(nsName, cmd->ctx); });
				break;
			}
			case DbCmdRenameNamespace: {
				execCommand(cmd, [&conn, &cmd](std::string_view srcNsName, const std::string& dstNsName) {
					return conn.rx.RenameNamespace(srcNsName, dstNsName, cmd->ctx);
				});
				break;
			}
			case DbCmdAddIndex: {
				execCommand(
					cmd, [&conn, &cmd](std::string_view nsName, const IndexDef& iDef) { return conn.rx.AddIndex(nsName, iDef, cmd->ctx); });
				break;
			}
			case DbCmdUpdateIndex: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const IndexDef& iDef) {
					return conn.rx.UpdateIndex(nsName, iDef, cmd->ctx);
				});
				break;
			}
			case DbCmdDropIndex: {
				execCommand(
					cmd, [&conn, &cmd](std::string_view nsName, const IndexDef& idx) { return conn.rx.DropIndex(nsName, idx, cmd->ctx); });
				break;
			}
			case DbCmdSetSchema: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, std::string_view schema) {
					return conn.rx.SetSchema(nsName, schema, cmd->ctx);
				});
				break;
			}
			case DbCmdGetSchema: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, int format, std::string& schema) {
					return conn.rx.GetSchema(nsName, format, schema, cmd->ctx);
				});
				break;
			}
			case DbCmdEnumNamespaces: {
				execCommand(cmd, [&conn, &cmd](std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts) {
					return conn.rx.EnumNamespaces(defs, opts, cmd->ctx);
				});
				break;
			}
			case DbCmdEnumDatabases: {
				execCommand(cmd, [&conn, &cmd](std::vector<std::string>& dbList) { return conn.rx.EnumDatabases(dbList, cmd->ctx); });
				break;
			}
			case DbCmdInsert: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, Item& item, RPCDataFormat format) {
					return conn.rx.Insert(nsName, item, format, cmd->ctx);
				});
				break;
			}
			case DbCmdInsertQR: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, CoroQueryResults& result) {
					return conn.rx.Insert(nsName, item, result, cmd->ctx);
				});
				break;
			}
			case DbCmdUpdate: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, RPCDataFormat format) {
					return conn.rx.Update(nsName, item, format, cmd->ctx);
				});
				break;
			}
			case DbCmdUpdateQR: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, CoroQueryResults& result) {
					return conn.rx.Update(nsName, item, result, cmd->ctx);
				});
				break;
			}
			case DbCmdUpsert: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, Item& item, RPCDataFormat format) {
					return conn.rx.Upsert(nsName, item, format, cmd->ctx);
				});
				break;
			}
			case DbCmdUpsertQR: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, CoroQueryResults& result) {
					return conn.rx.Upsert(nsName, item, result, cmd->ctx);
				});
				break;
			}
			case DbCmdUpdateQ: {
				execCommand(
					cmd, [&conn, &cmd](const Query& query, CoroQueryResults& result) { return conn.rx.Update(query, result, cmd->ctx); });
				break;
			}
			case DbCmdDelete: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, RPCDataFormat format) {
					return conn.rx.Delete(nsName, item, format, cmd->ctx);
				});
				break;
			}
			case DbCmdDeleteQR: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, client::Item& item, CoroQueryResults& result) {
					return conn.rx.Delete(nsName, item, result, cmd->ctx);
				});
				break;
			}
			case DbCmdDeleteQ: {
				execCommand(
					cmd, [&conn, &cmd](const Query& query, CoroQueryResults& result) { return conn.rx.Delete(query, result, cmd->ctx); });
				break;
			}
			case DbCmdNewItem: {
				auto cd = dynamic_cast<DatabaseCommandData<Item, std::string_view>*>(cmd);
				assert(cd);
				Item item = conn.rx.NewItem(std::get<0>(cd->arguments), *this, cd->ctx.execTimeout());
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdExecSQL: {
				execCommand(cmd, [&conn, &cmd](std::string_view ns, CoroQueryResults& qr) { return conn.rx.ExecSQL(ns, qr, cmd->ctx); });
				break;
			}
			case DbCmdSelect: {
				execCommand(
					cmd, [&conn, &cmd](const Query& query, CoroQueryResults& result) { return conn.rx.Select(query, result, cmd->ctx); });
				break;
			}
			case DbCmdGetMeta: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const std::string& key, std::string& data) {
					return conn.rx.GetMeta(nsName, key, data, cmd->ctx);
				});
				break;
			}
			case DbCmdGetShardedMeta: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data) {
					return conn.rx.GetMeta(nsName, key, data, cmd->ctx);
				});
				break;
			}
			case DbCmdPutMeta: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const std::string& key, std::string_view data) {
					return conn.rx.PutMeta(nsName, key, data, cmd->ctx);
				});
				break;
			}
			case DbCmdEnumMeta: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, std::vector<std::string>& keys) {
					return conn.rx.EnumMeta(nsName, keys, cmd->ctx);
				});
				break;
			}
			case DbCmdDeleteMeta: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, const std::string& key) {
					return conn.rx.DeleteMeta(nsName, key, cmd->ctx);
				});
				break;
			}
			case DbCmdGetSqlSuggestions: {
				execCommand(cmd, [&conn](std::string_view query, int pos, std::vector<std::string>& suggests) {
					return conn.rx.GetSqlSuggestions(query, pos, suggests);
				});
				break;
			}
			case DbCmdStatus: {
				auto* cd = dynamic_cast<DatabaseCommandData<Error, bool>*>(cmd);
				assert(cd);
				const bool force = std::get<0>(cd->arguments);
				if (force) {
					execCommand(cmd, [&conn, &cmd](bool forceCheck) { return conn.rx.Status(forceCheck, cmd->ctx); });
				} else {
					bool rspSent = false;
					for (auto& c : pool) {
						if (c.rx.RequiresStatusCheck()) {
							rspSent = true;
							// Execute status check on one of the broken connections
							execCommand(cmd, [&c, &cmd](bool forceCheck) { return c.rx.Status(forceCheck, cmd->ctx); });
							break;
						}
					}
					if (!rspSent) {
						cd->ret.set_value(Error());
					}
				}
				break;
			}
			case DbCmdNewTransaction: {
				auto* cd = dynamic_cast<DatabaseCommandData<CoroTransaction, std::string_view>*>(cmd);
				assertrx(cd);
				CoroTransaction coroTrans = conn.rx.NewTransaction(std::get<0>(cd->arguments), cd->ctx);
				cd->ret.set_value(std::move(coroTrans));
				break;
			}
			case DbCmdCommitTransaction: {
				execCommand(cmd, [&conn, &cmd](CoroTransaction& tr, CoroQueryResults& result) {
					assertrx_dbg(conn.rx.GetConnPtr() == tr.getConn());
					return conn.rx.CommitTransaction(tr, result, cmd->ctx);
				});
				break;
			}
			case DbCmdRollBackTransaction: {
				execCommand(cmd, [&conn, &cmd](CoroTransaction& tr) {
					assertrx_dbg(!tr.getConn() || conn.rx.GetConnPtr() == tr.getConn());
					return conn.rx.RollBackTransaction(tr, cmd->ctx);
				});
				break;
			}
			case DbCmdFetchResults: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, int, CoroQueryResults&>*>(cmd);
				assertrx(cd);
				CoroQueryResults& coroResults = std::get<1>(cd->arguments);
				assertrx_dbg(conn.rx.GetConnPtr() == coroResults.i_.conn_);
				try {
					fetchResultsImpl(std::get<0>(cd->arguments), coroResults.i_.queryParams_.count + coroResults.i_.fetchOffset_,
									 coroResults.i_.fetchAmount_, coroResults);
					cd->ret.set_value(Error());
				} catch (std::exception& e) {
					cd->ret.set_value(std::move(e));
				}
				break;
			}
			case DbCmdFetchResultsParametrized: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, int, int, int, CoroQueryResults&>*>(cmd);
				assertrx(cd);
				CoroQueryResults& coroResults = std::get<3>(cd->arguments);
				assertrx_dbg(conn.rx.GetConnPtr() == coroResults.i_.conn_);
				try {
					fetchResultsImpl(std::get<0>(cd->arguments), std::get<1>(cd->arguments), std::get<2>(cd->arguments), coroResults);
					cd->ret.set_value(Error());
				} catch (std::exception& e) {
					cd->ret.set_value(std::move(e));
				}
				break;
			}
			case DbCmdCloseResults: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroQueryResults&>*>(cmd);
				assert(cd);
				CoroQueryResults& coroResults = std::get<0>(cd->arguments);
				Error err;
				if (coroResults.holdsRemoteData()) {
					assertrx_dbg(conn.rx.GetConnPtr() == coroResults.i_.conn_);
					err = coroResults.i_.conn_
							  ->Call({reindexer::net::cproto::kCmdCloseResults, coroResults.i_.requestTimeout_, milliseconds(0), lsn_t(),
									  -1, ShardingKeyType::NotSetShard, nullptr, false, coroResults.i_.sessionTs_},
									 coroResults.i_.queryID_.main, coroResults.i_.queryID_.uid)
							  .Status();
					coroResults.setClosed();
				} else {
					err = Error(errLogic, "Client query results does not hold remote data");
				}
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdNewItemTx: {
				auto cd = dynamic_cast<DatabaseCommandData<Item, CoroTransaction&>*>(cmd);
				assertrx(cd);
				Item item = std::get<0>(cd->arguments).NewItem(this);
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdAddTxItem: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction&, Item, ItemModifyMode>*>(cmd);
				assertrx(cd);
				auto& tr = std::get<0>(cd->arguments);
				assertrx_dbg(conn.rx.GetConnPtr() == tr.getConn());
				Error err = tr.Modify(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), cmd->ctx.lsn());
				if (cd->ctx.cmpl()) {
					cd->ctx.cmpl()(err);
				} else {
					cd->ret.set_value(std::move(err));
				}
				break;
			}
			case DbCmdPutTxMeta: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction&, std::string_view, std::string_view>*>(cmd);
				assertrx(cd);
				auto& tr = std::get<0>(cd->arguments);
				assertrx_dbg(conn.rx.GetConnPtr() == tr.getConn());
				Error err = tr.PutMeta(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), cd->ctx.lsn());
				if (cd->ctx.cmpl()) {
					cd->ctx.cmpl()(err);
				} else {
					cd->ret.set_value(std::move(err));
				}
				break;
			}
			case DbCmdSetTxTagsMatcher: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction&, TagsMatcher>*>(cmd);
				assertrx(cd);
				auto& tr = std::get<0>(cd->arguments);
				assertrx_dbg(conn.rx.GetConnPtr() == tr.getConn());
				Error err = tr.SetTagsMatcher(std::move(std::get<1>(cd->arguments)), cd->ctx.lsn());
				if (cd->ctx.cmpl()) {
					cd->ctx.cmpl()(err);
				} else {
					cd->ret.set_value(std::move(err));
				}
				break;
			}
			case DbCmdModifyTx: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction&, Query>*>(cmd);
				assertrx(cd);
				CoroTransaction& tr = std::get<0>(cd->arguments);
				Error err(errLogic, "Connection pointer in transaction is nullptr.");
				auto txConn = tr.getConn();
				if (txConn) {
					assertrx_dbg(conn.rx.GetConnPtr() == txConn);
					WrSerializer ser;
					std::get<1>(cd->arguments).Serialize(ser);
					switch (std::get<1>(cd->arguments).type_) {
						case QueryUpdate:
							err = txConn
									  ->Call({cproto::kCmdUpdateQueryTx, tr.i_.requestTimeout_, tr.i_.execTimeout_, cmd->ctx.lsn(),
											  cmd->ctx.emitterServerId(), cmd->ctx.shardId(), nullptr, false, tr.i_.sessionTs_},
											 ser.Slice(), tr.i_.txId_)
									  .Status();
							break;
						case QueryDelete:
							err = txConn
									  ->Call({cproto::kCmdDeleteQueryTx, tr.i_.requestTimeout_, tr.i_.execTimeout_, cmd->ctx.lsn(),
											  cmd->ctx.emitterServerId(), cmd->ctx.shardId(), nullptr, false, tr.i_.sessionTs_},
											 ser.Slice(), tr.i_.txId_)
									  .Status();
							break;
						case QuerySelect:
						case QueryTruncate:
							err = Error(errParams, "Incorrect query type in transaction modify {}", int(std::get<1>(cd->arguments).type_));
					}
				}
				if (cd->ctx.cmpl()) {
					cd->ctx.cmpl()(err);
				} else {
					cd->ret.set_value(std::move(err));
				}
				break;
			}
			case DbCmdGetReplState: {
				execCommand(cmd, [&conn, &cmd](std::string_view nsName, ReplicationStateV2& state) {
					return conn.rx.GetReplState(nsName, state, cmd->ctx);
				});
				break;
			}
			case DbCmdSaveNewShardingCfg: {
				execCommand(cmd, [&conn, &cmd](std::string_view config, int64_t sourceId) {
					sharding::ShardingControlResponseData res;
					return conn.rx.ShardingControlRequest({sharding::ControlCmdType::SaveCandidate, config, sourceId}, res, cmd->ctx);
				});
				break;
			}
			case DbCmdResetOldShardingCfg: {
				execCommand(cmd, [&conn, &cmd](int64_t sourceId) {
					sharding::ShardingControlResponseData res;
					return conn.rx.ShardingControlRequest(
						sharding::ShardingControlRequestData{sharding::ControlCmdType::ResetOldSharding, sourceId}, res, cmd->ctx);
				});
				break;
			}
			case DbCmdRollbackConfigCandidate: {
				execCommand(cmd, [&conn, &cmd](int64_t sourceId) {
					sharding::ShardingControlResponseData res;
					return conn.rx.ShardingControlRequest(
						sharding::ShardingControlRequestData{sharding::ControlCmdType::RollbackCandidate, sourceId}, res, cmd->ctx);
				});
				break;
			}
			case DbCmdResetConfigCandidate: {
				execCommand(cmd, [&conn, &cmd](int64_t sourceId) {
					sharding::ShardingControlResponseData res;
					return conn.rx.ShardingControlRequest(
						sharding::ShardingControlRequestData{sharding::ControlCmdType::ResetCandidate, sourceId}, res, cmd->ctx);
				});
				break;
			}
			case DbCmdApplyNewShardingCfg: {
				execCommand(cmd, [&conn, &cmd](int64_t sourceId) {
					sharding::ShardingControlResponseData res;
					return conn.rx.ShardingControlRequest(
						sharding::ShardingControlRequestData{sharding::ControlCmdType::ApplyNew, sourceId}, res, cmd->ctx);
				});
				break;
			}
			case DbCmdVersion: {
				execCommand(cmd, [&conn, &cmd](std::string& version) { return conn.rx.Version(version, cmd->ctx); });
				break;
			}
			case DbCmdNone:
				assert(false);
				break;
		}
		conn.OnRequestDone();
		commandsQueue_.OnCmdDone(tid);
	}
}

void ReindexerImpl::fetchResultsImpl(int flags, int offset, int limit, CoroQueryResults& coroResults) {
	if (!coroResults.holdsRemoteData()) [[unlikely]] {
		throw Error(errLogic, "Client query results does not hold any remote data");
	}
	auto ret = coroResults.i_.conn_->Call({reindexer::net::cproto::kCmdFetchResults, coroResults.i_.requestTimeout_, milliseconds(0),
										   lsn_t(), -1, ShardingKeyType::NotSetShard, nullptr, false, coroResults.i_.sessionTs_},
										  coroResults.i_.queryID_.main, flags, offset, limit, coroResults.i_.queryID_.uid);
	if (!ret.Status().ok()) [[unlikely]] {
		throw ret.Status();
	}

	coroResults.handleFetchedBuf(ret);
}

ReindexerImpl::WorkerThread::WorkerThread(uint32_t _connCount) : connCount(_connCount) {}
ReindexerImpl::WorkerThread::~WorkerThread() = default;

void ReindexerImpl::CommandsQueue::Get(uint32_t tid, h_vector<DatabaseCommand, 16>& cmds) {
	auto& thD = RX_GET_WITHOUT_MUTEX_ANALYSIS {
		assertrx(tid < thData_.size());
		return thData_[tid];
	}();
	lock_guard lck(mtx_);
	if (thD.personalQueue.size()) {
		std::swap(thD.personalQueue, cmds);
		thD.reqCnt.fetch_add(cmds.size(), std::memory_order_relaxed);
		return;
	}
	if (sharedQueue_.size()) {
		cmds.emplace_back(std::move(*sharedQueue_.begin()));
		sharedQueue_.pop_front();
		thD.reqCnt.fetch_add(1, std::memory_order_relaxed);
		return;
	}
	thD.isReading = false;
}

void ReindexerImpl::CommandsQueue::OnCmdDone(uint32_t tid) {
	auto& thD = RX_GET_WITHOUT_MUTEX_ANALYSIS {
		assertrx(tid < thData_.size());
		return thData_[tid];
	}();
	thD.reqCnt.fetch_sub(1, std::memory_order_release);
}

void ReindexerImpl::CommandsQueue::Init(std::deque<WorkerThread>& threads) {
	lock_guard lock(mtx_);
	assertrx(thData_.empty());
	for (auto& th : threads) {
		thData_.emplace_back(th.commandAsync);
	}
}

void ReindexerImpl::CommandsQueue::Invalidate(uint32_t tid, h_vector<DatabaseCommand, 16>& cmds) {
	auto& thD = RX_GET_WITHOUT_MUTEX_ANALYSIS {
		assertrx(tid < thData_.size());
		return thData_[tid];
	}();
	lock_guard lock(mtx_);
	std::swap(thD.personalQueue, cmds);
	thD.reqCnt.store(0, std::memory_order_relaxed);
	while (sharedQueue_.size()) {
		cmds.emplace_back(std::move(*sharedQueue_.begin()));
		sharedQueue_.pop_front();
	}
	thByConns_.clear();
	isValid_ = false;
}

void ReindexerImpl::CommandsQueue::SetValid() {
	lock_guard lock(mtx_);
	isValid_ = true;
}

void ReindexerImpl::CommandsQueue::ClearConnectionsMapping() {
	lock_guard lock(mtx_);
	thByConns_.clear();
}

void ReindexerImpl::CommandsQueue::RegisterConn(uint32_t tid, const void* conn, uint32_t connIdx) {
	auto& thD = RX_GET_WITHOUT_MUTEX_ANALYSIS {
		assertrx(tid < thData_.size());
		return thData_[tid];
	}();
	lock_guard lock(mtx_);
	const auto res = thByConns_.emplace(conn, ConnMeta{.threadData = &thD, .connIdx = connIdx});
	assertrx(res.second);
	(void)res;
}

}  // namespace client
}  // namespace reindexer
