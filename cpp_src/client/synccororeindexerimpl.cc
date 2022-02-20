#include "synccororeindexerimpl.h"
#include "client/connectionspool.h"
#include "client/itemimpl.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

SyncCoroReindexerImpl::SyncCoroReindexerImpl(const CoroReindexerConfig &conf, size_t connCount)
	: conf_(conf), connCount_(connCount > 0 ? connCount : 1) {}

SyncCoroReindexerImpl::~SyncCoroReindexerImpl() {
	std::unique_lock<std::mutex> lock(loopThreadMtx_);
	if (loopThread_.joinable()) {
		closeAsync_.send();
		loopThread_.join();
	}
}

Error SyncCoroReindexerImpl::Connect(const string &dsn, const client::ConnectOpts &opts) {
	std::unique_lock<std::mutex> lock(loopThreadMtx_);
	if (loopThread_.joinable()) return Error(errLogic, "Client is already started");

	std::promise<Error> isRunningPromise;
	auto isRunningFuture = isRunningPromise.get_future();
	loopThread_ = std::thread([this, &isRunningPromise, dsn, opts]() { this->threadLoopFun(std::move(isRunningPromise), dsn, opts); });
	isRunningFuture.wait();
	auto isRunning = isRunningFuture.get();
	if (isRunning.ok()) {
		commandsQueue_.Init();
	}
	return isRunning;
}

Error SyncCoroReindexerImpl::Stop() {
	std::unique_lock<std::mutex> lock(loopThreadMtx_);
	if (loopThread_.joinable()) {
		closeAsync_.send();
		loopThread_.join();
	}
	return errOK;
}
Error SyncCoroReindexerImpl::OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx, const StorageOpts &opts,
										   const NsReplicationOpts &replOpts) {
	return sendCommand<Error>(DbCmdOpenNamespace, ctx, std::forward<std::string_view>(nsName), opts, replOpts);
}
Error SyncCoroReindexerImpl::AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx, const NsReplicationOpts &replOpts) {
	return sendCommand<Error>(DbCmdAddNamespace, ctx, nsDef, replOpts);
}
Error SyncCoroReindexerImpl::CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdCloseNamespace, ctx, std::forward<std::string_view>(nsName));
}
Error SyncCoroReindexerImpl::DropNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDropNamespace, ctx, std::forward<std::string_view>(nsName));
}
Error SyncCoroReindexerImpl::TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdTruncateNamespace, ctx, std::forward<std::string_view>(nsName));
}
Error SyncCoroReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdRenameNamespace, ctx, std::forward<std::string_view>(srcNsName), dstNsName);
}
Error SyncCoroReindexerImpl::AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdAddIndex, ctx, std::forward<std::string_view>(nsName), index);
}
Error SyncCoroReindexerImpl::UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdateIndex, ctx, std::forward<std::string_view>(nsName), index);
}
Error SyncCoroReindexerImpl::DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDropIndex, ctx, std::forward<std::string_view>(nsName), index);
}
Error SyncCoroReindexerImpl::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdSetSchema, ctx, std::forward<std::string_view>(nsName), std::forward<std::string_view>(schema));
}
Error SyncCoroReindexerImpl::GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetSchema, ctx, std::forward<std::string_view>(nsName), std::forward<int>(format), schema);
}
Error SyncCoroReindexerImpl::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumNamespaces, ctx, defs, std::forward<EnumNamespacesOpts>(opts));
}
Error SyncCoroReindexerImpl::EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumDatabases, ctx, dbList);
}
Error SyncCoroReindexerImpl::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdInsert, ctx, std::forward<std::string_view>(nsName), item);
}
Error SyncCoroReindexerImpl::Insert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdInsertQR, ctx, std::forward<std::string_view>(nsName), item, result.results_);
}

Error SyncCoroReindexerImpl::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdate, ctx, std::forward<std::string_view>(nsName), item);
}
Error SyncCoroReindexerImpl::Update(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdateQR, ctx, std::forward<std::string_view>(nsName), item, result.results_);
}

Error SyncCoroReindexerImpl::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpsert, ctx, std::forward<std::string_view>(nsName), item);
}
Error SyncCoroReindexerImpl::Upsert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpsertQR, ctx, std::forward<std::string_view>(nsName), item, result.results_);
}

Error SyncCoroReindexerImpl::Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdUpdateQ, ctx, query, result.results_);
}
Error SyncCoroReindexerImpl::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDelete, ctx, std::forward<std::string_view>(nsName), item);
}
Error SyncCoroReindexerImpl::Delete(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDeleteQR, ctx, std::forward<std::string_view>(nsName), item, result.results_);
}

Error SyncCoroReindexerImpl::Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdDeleteQ, ctx, query, result.results_);
}
Error SyncCoroReindexerImpl::Select(std::string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdSelectS, ctx, std::forward<std::string_view>(query), result.results_);
}
Error SyncCoroReindexerImpl::Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdSelectQ, ctx, query, result.results_);
}
Error SyncCoroReindexerImpl::Commit(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdCommit, ctx, std::forward<std::string_view>(nsName));
}

Item SyncCoroReindexerImpl::NewItem(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Item>(DbCmdNewItem, ctx, std::forward<std::string_view>(nsName));
}

Error SyncCoroReindexerImpl::GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetMeta, ctx, std::forward<std::string_view>(nsName), key, data);
}
Error SyncCoroReindexerImpl::GetMeta(std::string_view nsName, const std::string &key, std::vector<ShardedMeta> &data,
									 const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetShardedMeta, ctx, std::forward<std::string_view>(nsName), key, data);
}
Error SyncCoroReindexerImpl::PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdPutMeta, ctx, std::forward<std::string_view>(nsName), key, std::forward<std::string_view>(data));
}
Error SyncCoroReindexerImpl::EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumMeta, ctx, std::forward<std::string_view>(nsName), keys);
}
Error SyncCoroReindexerImpl::GetSqlSuggestions(std::string_view sqlQuery, int pos, vector<string> &suggestions) {
	return sendCommand<Error>(DbCmdGetSqlSuggestions, InternalRdxContext(), std::forward<std::string_view>(sqlQuery),
							  std::forward<int>(pos), suggestions);
}
Error SyncCoroReindexerImpl::Status(bool forceCheck, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdStatus, ctx, std::forward<bool>(forceCheck));
}

CoroTransaction SyncCoroReindexerImpl::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<CoroTransaction>(DbCmdNewTransaction, ctx, std::forward<std::string_view>(nsName));
}

Error SyncCoroReindexerImpl::CommitTransaction(SyncCoroTransaction &tr, SyncCoroQueryResults &results, const InternalRdxContext &ctx) {
	if (tr.IsFree()) {
		return Error(errBadTransaction, "commit free transaction");
	}
	if (tr.rx_.get() != this) {
		return Error(errTxInvalidLeader, "Commit transaction to incorrect leader");
	}
	return sendCommand<Error, CoroTransaction &, CoroQueryResults &>(DbCmdCommitTransaction, ctx, tr.tr_, results.results_);
}

Error SyncCoroReindexerImpl::RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	if (tr.IsFree()) return tr.Status();
	if (tr.rx_.get() != this) {
		return Error(errLogic, "RollBack transaction to incorrect leader");
	}
	return sendCommand<Error, CoroTransaction &>(DbCmdRollBackTransaction, ctx, tr.tr_);
}

Error SyncCoroReindexerImpl::GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetReplState, ctx, std::forward<std::string_view>(nsName), state);
}

Error SyncCoroReindexerImpl::fetchResults(int flags, SyncCoroQueryResults &result) {
	return sendCommand<Error>(DbCmdFetchResults, InternalRdxContext(), std::forward<int>(flags), result.results_);
}

Error SyncCoroReindexerImpl::closeResults(SyncCoroQueryResults &result) {
	return sendCommand<Error>(DbCmdCloseResults, InternalRdxContext(), result.results_);
}

Error SyncCoroReindexerImpl::addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode, lsn_t lsn) {
	return sendCommand<Error>(DbCmdAddTxItem, InternalRdxContext(), tr.tr_, std::move(item), std::forward<ItemModifyMode>(mode),
							  std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::putTxMeta(SyncCoroTransaction &tr, std::string_view key, std::string_view value, lsn_t lsn) {
	return sendCommand<Error>(DbCmdPutTxMeta, InternalRdxContext(), tr.tr_, std::forward<std::string_view>(key),
							  std::forward<std::string_view>(value), std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::setTxTm(SyncCoroTransaction &tr, TagsMatcher &&tm, lsn_t lsn) {
	return sendCommand<Error>(DbCmdSetTxTagsMatcher, InternalRdxContext(), tr.tr_, std::forward<TagsMatcher>(tm), std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::modifyTx(SyncCoroTransaction &tr, Query &&q, lsn_t lsn) {
	return sendCommand<Error>(DbCmdModifyTx, InternalRdxContext(), tr.tr_, std::move(q), std::forward<lsn_t>(lsn));
}

Item SyncCoroReindexerImpl::newItemTx(CoroTransaction &tr) { return sendCommand<Item>(DbCmdNewItemTx, InternalRdxContext(), tr); }

void SyncCoroReindexerImpl::threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts) {
	ConnectionsPool<DatabaseCommand> connPool(getConnData());

	commandAsync_.set(loop_);
	commandAsync_.set([this, &connPool](net::ev::async &) {
		loop_.spawn([this, &connPool]() {
			std::vector<DatabaseCommand> q;
			static constexpr size_t kReserveSize = 32;
			q.reserve(kReserveSize);
			commandsQueue_.Get(q);
			for (size_t i = 0; i < q.size(); ++i) {
				auto &conn = connPool.GetConn();
				assert(conn.IsChOpened());
				conn.PushCmd(std::move(q[i]));
			}
		});
	});

	size_t runningCount = 0;
	for (auto &conn : connPool) {
		loop_.spawn([this, dsn, opts, &isRunning, &conn, &connPool, &runningCount]() noexcept {
			auto err = conn.rx.Connect(dsn, loop_, opts);
			if (++runningCount == connCount_) {
				isRunning.set_value(err);
			}
			coroutine::wait_group wg;
			for (unsigned n = 0; n < conf_.SyncRxCoroCount; ++n) {
				loop_.spawn(wg, [this, &conn, &connPool]() noexcept { coroInterpreter(conn, connPool); });
			}
			wg.wait();

			commandAsync_.stop();
			closeAsync_.stop();
			assert(!conn.IsChOpened());
			conn.rx.Stop();
		});
	}
	commandAsync_.start();

	closeAsync_.set(loop_);
	closeAsync_.set([this, &connPool](net::ev::async &) {
		commandAsync_.stop();
		loop_.spawn([this, &connPool]() {
			coroutine::wait_group wg;
			for (auto &conn : connPool) {
				if (conn.IsChOpened()) {
					loop_.spawn(wg, [&conn] { conn.rx.Stop(); });
				}
			}
			wg.wait();
			std::vector<DatabaseCommand> q;
			commandsQueue_.Invalidate(q);
			for (auto &conn : connPool) {
				if (conn.IsChOpened()) {
					for (size_t i = 0; i < q.size(); ++i) {
						conn.PushCmd(std::move(q[i]));
					}
					q.clear();
					break;
				}
			}
			assert(q.empty());
			for (auto &conn : connPool) {
				conn.CloseCh();
			}
		});
	});
	closeAsync_.start();

	loop_.run();
	commandAsync_.stop();
}

ConnectionsPoolData &SyncCoroReindexerImpl::getConnData() {
	if (!connData_) {
		connData_ = std::make_unique<ConnectionsPoolData>(connCount_, conf_);
	}
	return *connData_;
}

void SyncCoroReindexerImpl::coroInterpreter(Connection<DatabaseCommand> &conn, ConnectionsPool<DatabaseCommand> &pool) noexcept {
	using namespace std::placeholders;
	for (std::pair<DatabaseCommand, bool> v = conn.PopCmd(); v.second == true; v = conn.PopCmd()) {
		const auto cmd = v.first.Data();
		switch (cmd->id) {
			case DbCmdOpenNamespace: {
				std::function<Error(std::string_view, const StorageOpts &, const NsReplicationOpts &)> f =
					std::bind(&client::CoroRPCClient::OpenNamespace, &conn.rx, _1, std::ref(cmd->ctx), _2, _3);
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdAddNamespace: {
				std::function<Error(const NamespaceDef &, const NsReplicationOpts &)> f =
					std::bind(&client::CoroRPCClient::AddNamespace, &conn.rx, _1, std::ref(cmd->ctx), _2);
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdCloseNamespace: {
				std::function<Error(std::string_view)> f =
					std::bind(&client::CoroRPCClient::CloseNamespace, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdDropNamespace: {
				std::function<Error(std::string_view)> f =
					std::bind(&client::CoroRPCClient::DropNamespace, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdTruncateNamespace: {
				std::function<Error(std::string_view)> f =
					std::bind(&client::CoroRPCClient::TruncateNamespace, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdRenameNamespace: {
				std::function<Error(std::string_view, const std::string &)> f =
					std::bind(&client::CoroRPCClient::RenameNamespace, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}

			case DbCmdAddIndex: {
				std::function<Error(std::string_view, const IndexDef &)> f =
					std::bind(&client::CoroRPCClient::AddIndex, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdUpdateIndex: {
				std::function<Error(std::string_view, const IndexDef &)> f =
					std::bind(&client::CoroRPCClient::UpdateIndex, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdDropIndex: {
				std::function<Error(std::string_view, const IndexDef &)> f =
					std::bind(&client::CoroRPCClient::DropIndex, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdSetSchema: {
				std::function<Error(std::string_view, std::string_view)> f =
					std::bind(&client::CoroRPCClient::SetSchema, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdGetSchema: {
				std::function<Error(std::string_view, int, std::string &)> f =
					std::bind(&client::CoroRPCClient::GetSchema, &conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdEnumNamespaces: {
				std::function<Error(vector<NamespaceDef> &, EnumNamespacesOpts)> f =
					std::bind(&client::CoroRPCClient::EnumNamespaces, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdEnumDatabases: {
				std::function<Error(vector<string> &)> f =
					std::bind(&client::CoroRPCClient::EnumDatabases, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdInsert: {
				std::function<Error(std::string_view, Item &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Insert),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdInsertQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Insert),
					&conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}

			case DbCmdUpdate: {
				std::function<Error(std::string_view, Item &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdUpdateQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Update),
					&conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}

			case DbCmdUpsert: {
				std::function<Error(std::string_view, Item &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Upsert),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdUpsertQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Upsert),
					&conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}

			case DbCmdUpdateQ: {
				std::function<Error(const Query &, CoroQueryResults &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdDelete: {
				std::function<Error(std::string_view, Item &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdDeleteQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Delete),
					&conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdDeleteQ: {
				std::function<Error(const Query &, CoroQueryResults &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}

			case DbCmdNewItem: {
				auto cd = dynamic_cast<DatabaseCommandData<Item, std::string_view> *>(cmd);
				assert(cd);
				Item item = conn.rx.NewItem(std::get<0>(cd->arguments), *this, cd->ctx.execTimeout());
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdSelectS: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, std::string_view, CoroQueryResults &> *>(cmd);
				assert(cd);
				Error err = conn.rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments), cd->ctx);
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdSelectQ: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, const Query &, CoroQueryResults &> *>(cmd);
				assert(cd);
				Error err = conn.rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments), cd->ctx);
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdCommit: {
				std::function<Error(std::string_view)> f = std::bind(&client::CoroRPCClient::Commit, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdGetMeta: {
				std::function<Error(std::string_view, const std::string &, std::string &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, const std::string &, std::string &,
																		   const InternalRdxContext &)>(&client::CoroRPCClient::GetMeta),
							  &conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdGetShardedMeta: {
				std::function<Error(std::string_view, const std::string &, std::vector<ShardedMeta> &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, const std::string &, std::vector<ShardedMeta> &,
																 const InternalRdxContext &)>(&client::CoroRPCClient::GetMeta),
					&conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdPutMeta: {
				std::function<Error(std::string_view, const string &, std::string_view)> f =
					std::bind(&client::CoroRPCClient::PutMeta, &conn.rx, _1, _2, _3, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdEnumMeta: {
				std::function<Error(std::string_view, vector<string> &)> f =
					std::bind(&client::CoroRPCClient::EnumMeta, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdGetSqlSuggestions: {
				std::function<Error(std::string_view, int, vector<string> &)> f =
					std::bind(&client::CoroRPCClient::GetSqlSuggestions, &conn.rx, _1, _2, _3);
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdStatus: {
				auto *cd = dynamic_cast<DatabaseCommandData<Error, bool> *>(cmd);
				assert(cd);
				bool force = std::get<0>(cd->arguments);
				for (auto &c : pool) {
					if (force || c.rx.RequiresStatusCheck()) {
						force = true;
						break;
					}
				}
				std::function<Error(bool)> f = std::bind(&client::CoroRPCClient::Status, &conn.rx, force, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdNewTransaction: {
				auto *cd = dynamic_cast<DatabaseCommandData<CoroTransaction, std::string_view> *>(cmd);
				assert(cd);
				CoroTransaction coroTrans = conn.rx.NewTransaction(std::get<0>(cd->arguments), cd->ctx);
				cd->ret.set_value(std::move(coroTrans));
				break;
			}
			case DbCmdCommitTransaction: {
				std::function<Error(CoroTransaction &, CoroQueryResults &)> f =
					std::bind(&client::CoroRPCClient::CommitTransaction, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdRollBackTransaction: {
				std::function<Error(CoroTransaction &)> f =
					std::bind(&client::CoroRPCClient::RollBackTransaction, &conn.rx, _1, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			case DbCmdFetchResults: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, int, CoroQueryResults &> *>(cmd);
				assert(cd);
				CoroQueryResults &coroResults = std::get<1>(cd->arguments);
				if (!coroResults.holdsRemoteData()) {
					cd->ret.set_value(Error(errLogic, "Client query results does not hold any remote data"));
					break;
				}
				auto ret = coroResults.i_.conn_->Call(
					{reindexer::net::cproto::kCmdFetchResults, coroResults.i_.requestTimeout_, milliseconds(0), lsn_t(), -1,
					 ShardingKeyType::NotSetShard, nullptr, false, coroResults.i_.sessionTs_},
					coroResults.i_.queryID_, std::get<0>(cd->arguments), coroResults.i_.queryParams_.count + coroResults.i_.fetchOffset_,
					coroResults.i_.fetchAmount_);
				if (!ret.Status().ok()) {
					cd->ret.set_value(ret.Status());
					break;
				}
				Error err;
				try {
					auto args = ret.GetArgs(2);

					coroResults.i_.fetchOffset_ += coroResults.i_.queryParams_.count;

					std::string_view rawResult = p_string(args[0]);
					ResultSerializer ser(rawResult);

					ser.GetRawQueryParams(coroResults.i_.queryParams_, nullptr);

					coroResults.i_.rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());
				} catch (Error &e) {
					err = e;
				}

				cd->ret.set_value(err);

				break;
			}
			case DbCmdCloseResults: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroQueryResults &> *>(cmd);
				assert(cd);
				CoroQueryResults &coroResults = std::get<0>(cd->arguments);
				Error err;
				if (coroResults.holdsRemoteData()) {
					err = coroResults.i_.conn_
							  ->Call({reindexer::net::cproto::kCmdCloseResults, coroResults.i_.requestTimeout_, milliseconds(0), lsn_t(),
									  -1, ShardingKeyType::NotSetShard, nullptr, false, coroResults.i_.sessionTs_},
									 coroResults.i_.queryID_)
							  .Status();
					coroResults.setClosed();
				} else {
					err = Error(errLogic, "Client query results does not hold remote data");
				}
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdNewItemTx: {
				auto cd = dynamic_cast<DatabaseCommandData<Item, CoroTransaction &> *>(cmd);
				assert(cd);
				Item item = std::get<0>(cd->arguments).NewItem(this);
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdAddTxItem: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction &, Item, ItemModifyMode, lsn_t> *>(cmd);
				assert(cd);
				Error err = std::get<0>(cd->arguments)
								.Modify(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), std::get<3>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdPutTxMeta: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction &, std::string_view, std::string_view, lsn_t> *>(cmd);
				assert(cd);
				Error err = std::get<0>(cd->arguments)
								.PutMeta(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), std::get<3>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdSetTxTagsMatcher: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction &, TagsMatcher, lsn_t> *>(cmd);
				assert(cd);
				Error err = std::get<0>(cd->arguments).SetTagsMatcher(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdModifyTx: {
				auto cd = dynamic_cast<DatabaseCommandData<Error, CoroTransaction &, Query, lsn_t> *>(cmd);
				assert(cd);
				CoroTransaction &tr = std::get<0>(cd->arguments);
				Error err(errLogic, "Connection pointer in transaction is nullptr.");
				auto txConn = tr.getConn();
				if (txConn) {
					WrSerializer ser;
					std::get<1>(cd->arguments).Serialize(ser);
					switch (std::get<1>(cd->arguments).type_) {
						case QueryUpdate: {
							err =
								txConn
									->Call({cproto::kCmdUpdateQueryTx, tr.i_.requestTimeout_, tr.i_.execTimeout_,
											std::get<2>(cd->arguments), -1, ShardingKeyType::NotSetShard, nullptr, false, tr.i_.sessionTs_},
										   ser.Slice(), tr.i_.txId_)
									.Status();
							break;
						}
						case QueryDelete: {
							err =
								txConn
									->Call({cproto::kCmdDeleteQueryTx, tr.i_.requestTimeout_, tr.i_.execTimeout_,
											std::get<2>(cd->arguments), -1, ShardingKeyType::NotSetShard, nullptr, false, tr.i_.sessionTs_},
										   ser.Slice(), tr.i_.txId_)
									.Status();
							break;
						}
						default:
							err = Error(errParams, "Incorrect query type in transaction modify %d", std::get<1>(cd->arguments).type_);
					}
				}
				cd->ret.set_value(err);
				break;
			}
			case DbCmdGetReplState: {
				std::function<Error(std::string_view, ReplicationStateV2 &)> f =
					std::bind(&client::CoroRPCClient::GetReplState, &conn.rx, _1, _2, std::ref(cmd->ctx));
				execCommand(cmd, std::move(f));
				break;
			}
			default:
				assert(false);
				break;
		}
		conn.OnRequestDone();
	}
}

Error SyncCoroReindexerImpl::CommandsQueue::Push(net::ev::async &ev, DatabaseCommand &&cmd) {
	bool notify = false;
	std::unique_lock<std::mutex> lck(mtx_);
	if (!isValid_) {
		return Error(errNotValid, "SyncCoroReindexer command queue is invalidated");
	}
	notify = (queue_.empty());
	queue_.emplace_back(std::move(cmd));
	lck.unlock();

	if (notify) {
		ev.send();
	}
	return Error();
}
void SyncCoroReindexerImpl::CommandsQueue::Get(std::vector<DatabaseCommand> &cmds) {
	std::lock_guard<std::mutex> lock(mtx_);
	cmds.swap(queue_);
}

void SyncCoroReindexerImpl::CommandsQueue::Invalidate(std::vector<DatabaseCommand> &cmds) {
	std::lock_guard<std::mutex> lock(mtx_);
	cmds.swap(queue_);
	isValid_ = false;
}

void SyncCoroReindexerImpl::CommandsQueue::Init() {
	std::lock_guard<std::mutex> lock(mtx_);
	isValid_ = true;
}

}  // namespace client
}  // namespace reindexer
