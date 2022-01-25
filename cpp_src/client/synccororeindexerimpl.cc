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
	return sendCommand<Error>(DbCmdOpenNamespace, std::forward<std::string_view>(nsName), ctx, opts, replOpts);
}
Error SyncCoroReindexerImpl::AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx, const NsReplicationOpts &replOpts) {
	return sendCommand<Error>(DbCmdAddNamespace, nsDef, ctx, replOpts);
}
Error SyncCoroReindexerImpl::CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdCloseNamespace, std::forward<std::string_view>(nsName), ctx);
}
Error SyncCoroReindexerImpl::DropNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDropNamespace, std::forward<std::string_view>(nsName), ctx);
}
Error SyncCoroReindexerImpl::TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdTruncateNamespace, std::forward<std::string_view>(nsName), ctx);
}
Error SyncCoroReindexerImpl::RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdRenameNamespace, std::forward<std::string_view>(srcNsName), dstNsName, ctx);
}
Error SyncCoroReindexerImpl::AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdAddIndex, std::forward<std::string_view>(nsName), index, ctx);
}
Error SyncCoroReindexerImpl::UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdateIndex, std::forward<std::string_view>(nsName), index, ctx);
}
Error SyncCoroReindexerImpl::DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDropIndex, std::forward<std::string_view>(nsName), index, ctx);
}
Error SyncCoroReindexerImpl::SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdSetSchema, std::forward<std::string_view>(nsName), std::forward<std::string_view>(schema), ctx);
}
Error SyncCoroReindexerImpl::GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetSchema, std::forward<std::string_view>(nsName), std::forward<int>(format), schema, ctx);
}
Error SyncCoroReindexerImpl::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumNamespaces, defs, std::forward<EnumNamespacesOpts>(opts), ctx);
}
Error SyncCoroReindexerImpl::EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumDatabases, dbList, ctx);
}
Error SyncCoroReindexerImpl::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdInsert, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Insert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdInsertQR, std::forward<std::string_view>(nsName), item, result.results_, ctx);
}

Error SyncCoroReindexerImpl::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdate, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Update(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdateQR, std::forward<std::string_view>(nsName), item, result.results_, ctx);
}

Error SyncCoroReindexerImpl::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpsert, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Upsert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpsertQR, std::forward<std::string_view>(nsName), item, result.results_, ctx);
}

Error SyncCoroReindexerImpl::Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdUpdateQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDelete, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Delete(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDeleteQR, std::forward<std::string_view>(nsName), item, result.results_, ctx);
}

Error SyncCoroReindexerImpl::Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdDeleteQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Select(std::string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdSelectS, std::forward<std::string_view>(query), result.results_, ctx);
}
Error SyncCoroReindexerImpl::Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	Error err = result.setClient(this);
	if (!err.ok()) return err;
	return sendCommand<Error>(DbCmdSelectQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Commit(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdCommit, std::forward<std::string_view>(nsName), ctx);
}

Item SyncCoroReindexerImpl::NewItem(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<Item>(DbCmdNewItem, std::forward<std::string_view>(nsName), ctx);
}

Error SyncCoroReindexerImpl::GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetMeta, std::forward<std::string_view>(nsName), key, data, ctx);
}
Error SyncCoroReindexerImpl::PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdPutMeta, std::forward<std::string_view>(nsName), key, std::forward<std::string_view>(data), ctx);
}
Error SyncCoroReindexerImpl::EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumMeta, std::forward<std::string_view>(nsName), keys, ctx);
}
Error SyncCoroReindexerImpl::GetSqlSuggestions(std::string_view sqlQuery, int pos, vector<string> &suggestions) {
	return sendCommand<Error>(DbCmdGetSqlSuggestions, std::forward<std::string_view>(sqlQuery), std::forward<int>(pos), suggestions);
}
Error SyncCoroReindexerImpl::Status(const InternalRdxContext &ctx) { return sendCommand<Error>(DbCmdStatus, ctx); }

CoroTransaction SyncCoroReindexerImpl::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	return sendCommand<CoroTransaction>(DbCmdNewTransaction, std::forward<std::string_view>(nsName), ctx);
}

Error SyncCoroReindexerImpl::CommitTransaction(SyncCoroTransaction &tr, SyncCoroQueryResults &results, const InternalRdxContext &ctx) {
	if (tr.IsFree()) return Error(errLogic, "commit free transaction");
	if (tr.rx_.get() != this) {
		return Error(errTxInvalidLeader, "Commit transaction to incorrect leader");
	}
	return sendCommand<Error, CoroTransaction &, CoroQueryResults &, const InternalRdxContext &>(DbCmdCommitTransaction, tr.tr_,
																								 results.results_, ctx);
}

Error SyncCoroReindexerImpl::RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	if (tr.IsFree()) return tr.Status();
	if (tr.rx_.get() != this) {
		return Error(errLogic, "RollBack transaction to incorrect leader");
	}
	return sendCommand<Error, CoroTransaction &, const InternalRdxContext &>(DbCmdRollBackTransaction, tr.tr_, ctx);
}

Error SyncCoroReindexerImpl::GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetReplState, std::forward<std::string_view>(nsName), state, ctx);
}

Error SyncCoroReindexerImpl::fetchResults(int flags, SyncCoroQueryResults &result) {
	return sendCommand<Error>(DbCmdFetchResults, std::forward<int>(flags), result.results_);
}

Error SyncCoroReindexerImpl::addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode, lsn_t lsn) {
	return sendCommand<Error>(DbCmdAddTxItem, tr.tr_, std::move(item), std::forward<ItemModifyMode>(mode), std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::putTxMeta(SyncCoroTransaction &tr, std::string_view key, std::string_view value, lsn_t lsn) {
	return sendCommand<Error>(DbCmdPutTxMeta, tr.tr_, std::forward<std::string_view>(key), std::forward<std::string_view>(value),
							  std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::setTxTm(SyncCoroTransaction &tr, TagsMatcher &&tm, lsn_t lsn) {
	return sendCommand<Error>(DbCmdSetTxTagsMatcher, tr.tr_, std::forward<TagsMatcher>(tm), std::forward<lsn_t>(lsn));
}

Error SyncCoroReindexerImpl::modifyTx(SyncCoroTransaction &tr, Query &&q, lsn_t lsn) {
	return sendCommand<Error>(DbCmdModifyTx, tr.tr_, std::move(q), std::forward<lsn_t>(lsn));
}

Item SyncCoroReindexerImpl::newItemTx(CoroTransaction &tr) { return sendCommand<Item>(DbCmdNewItemTx, tr); }

void SyncCoroReindexerImpl::threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts) {
	ConnectionsPool<DatabaseCommandBase> connPool(connCount_, conf_);

	commandAsync_.set(loop_);
	commandAsync_.set([this, &connPool](net::ev::async &) {
		loop_.spawn([this, &connPool]() {
			std::vector<DatabaseCommandBase *> q;
			commandsQueue_.Get(q);
			for (size_t i = 0; i < q.size(); ++i) {
				auto &conn = connPool.GetConn();
				assert(conn.IsChOpened());
				conn.PushCmd(q[i]);
			}
		});
	});

	size_t runningCount = 0;
	for (auto &conn : connPool) {
		loop_.spawn([this, dsn, opts, &isRunning, &conn, &runningCount]() noexcept {
			auto err = conn.rx.Connect(dsn, loop_, opts);
			if (++runningCount == connCount_) {
				isRunning.set_value(err);
			}
			coroutine::wait_group wg;
			for (unsigned n = 0; n < conf_.SyncRxCoroCount; ++n) {
				loop_.spawn(wg, [this, &conn]() noexcept { coroInterpreter(conn); });
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
			std::vector<DatabaseCommandBase *> q;
			commandsQueue_.Invalidate(q);
			for (auto &conn : connPool) {
				if (conn.IsChOpened()) {
					for (size_t i = 0; i < q.size(); ++i) {
						conn.PushCmd(q[i]);
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

void SyncCoroReindexerImpl::coroInterpreter(Connection<DatabaseCommandBase> &conn) noexcept {
	using namespace std::placeholders;
	for (std::pair<DatabaseCommandBase *, bool> v = conn.PopCmd(); v.second == true; v = conn.PopCmd()) {
		switch (v.first->id_) {
			case DbCmdOpenNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &, const StorageOpts &, const NsReplicationOpts &)> f =
					std::bind(&client::CoroRPCClient::OpenNamespace, &conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdAddNamespace: {
				std::function<Error(const NamespaceDef &, const InternalRdxContext &, const NsReplicationOpts &)> f =
					std::bind(&client::CoroRPCClient::AddNamespace, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdCloseNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::CloseNamespace, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDropNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::DropNamespace, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdTruncateNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::TruncateNamespace, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdRenameNamespace: {
				std::function<Error(std::string_view, const std::string &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::RenameNamespace, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}

			case DbCmdAddIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::AddIndex, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpdateIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::UpdateIndex, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDropIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::DropIndex, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdSetSchema: {
				std::function<Error(std::string_view, std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::SetSchema, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdGetSchema: {
				std::function<Error(std::string_view, int, std::string &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::GetSchema, &conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumNamespaces: {
				std::function<Error(vector<NamespaceDef> &, EnumNamespacesOpts, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumNamespaces, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumDatabases: {
				std::function<Error(vector<string> &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumDatabases, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdInsert: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Insert),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdInsertQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Insert),
					&conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}

			case DbCmdUpdate: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpdateQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Update),
					&conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);

				break;
			}

			case DbCmdUpsert: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Upsert),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpsertQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Upsert),
					&conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}

			case DbCmdUpdateQ: {
				std::function<Error(const Query &, CoroQueryResults &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDelete: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDeleteQR: {
				std::function<Error(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)> f = std::bind(
					static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, CoroQueryResults &, const InternalRdxContext &)>(
						&client::CoroRPCClient::Delete),
					&conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);

				break;
			}
			case DbCmdDeleteQ: {
				std::function<Error(const Query &, CoroQueryResults &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}

			case DbCmdNewItem: {
				auto cd = dynamic_cast<DatabaseCommand<Item, std::string_view, const InternalRdxContext &> *>(v.first);
				assert(cd);
				Item item = conn.rx.NewItem(std::get<0>(cd->arguments), *this, std::get<1>(cd->arguments).execTimeout());
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdSelectS: {
				auto cd = dynamic_cast<DatabaseCommand<Error, std::string_view, CoroQueryResults &, const InternalRdxContext &> *>(v.first);
				assert(cd);
				Error err = conn.rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments), std::get<2>(cd->arguments));
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdSelectQ: {
				auto cd = dynamic_cast<DatabaseCommand<Error, const Query &, CoroQueryResults &, const InternalRdxContext &> *>(v.first);
				assert(cd);
				Error err = conn.rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments), std::get<2>(cd->arguments));
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdCommit: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::Commit, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdGetMeta: {
				std::function<Error(std::string_view, const string &, string &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::GetMeta, &conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdPutMeta: {
				std::function<Error(std::string_view, const string &, std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::PutMeta, &conn.rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumMeta: {
				std::function<Error(std::string_view, vector<string> &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumMeta, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdGetSqlSuggestions: {
				std::function<Error(std::string_view, int, vector<string> &)> f =
					std::bind(&client::CoroRPCClient::GetSqlSuggestions, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdStatus: {
				std::function<Error(const InternalRdxContext &)> f = std::bind(&client::CoroRPCClient::Status, &conn.rx, false, _1);
				execCommand(v.first, f);
				break;
			}
			case DbCmdNewTransaction: {
				auto *cd = dynamic_cast<DatabaseCommand<CoroTransaction, std::string_view, const InternalRdxContext &> *>(v.first);
				assert(cd);
				CoroTransaction coroTrans = conn.rx.NewTransaction(std::get<0>(cd->arguments), std::get<1>(cd->arguments));
				cd->ret.set_value(std::move(coroTrans));
				break;
			}
			case DbCmdCommitTransaction: {
				std::function<Error(CoroTransaction &, CoroQueryResults &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::CommitTransaction, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdRollBackTransaction: {
				std::function<Error(CoroTransaction &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::RollBackTransaction, &conn.rx, _1, _2);
				execCommand(v.first, f);
				break;
			}

			case DbCmdFetchResults: {
				auto cd = dynamic_cast<DatabaseCommand<Error, int, CoroQueryResults &> *>(v.first);
				assert(cd);
				CoroQueryResults &coroResults = std::get<1>(cd->arguments);
				auto ret = coroResults.conn_->Call({reindexer::net::cproto::kCmdFetchResults, coroResults.requestTimeout_, milliseconds(0),
													lsn_t(), -1, IndexValueType::NotSet, nullptr, false},
												   coroResults.queryID_, std::get<0>(cd->arguments),
												   coroResults.queryParams_.count + coroResults.fetchOffset_, coroResults.fetchAmount_);
				if (!ret.Status().ok()) {
					cd->ret.set_value(ret.Status());
					break;
				}

				auto args = ret.GetArgs(2);

				coroResults.fetchOffset_ += coroResults.queryParams_.count;

				std::string_view rawResult = p_string(args[0]);
				ResultSerializer ser(rawResult);

				ser.GetRawQueryParams(coroResults.queryParams_, nullptr);

				coroResults.rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());

				Error err;
				cd->ret.set_value(err);

				break;
			}
			case DbCmdNewItemTx: {
				auto cd = dynamic_cast<DatabaseCommand<Item, CoroTransaction &> *>(v.first);
				assert(cd);
				Item item = std::get<0>(cd->arguments).NewItem(this);
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdAddTxItem: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, Item, ItemModifyMode, lsn_t> *>(v.first);
				assert(cd);
				Error err = std::get<0>(cd->arguments)
								.Modify(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), std::get<3>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdPutTxMeta: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, std::string_view, std::string_view, lsn_t> *>(v.first);
				assert(cd);
				Error err = std::get<0>(cd->arguments)
								.PutMeta(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments), std::get<3>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdSetTxTagsMatcher: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, TagsMatcher, lsn_t> *>(v.first);
				assert(cd);
				Error err = std::get<0>(cd->arguments).SetTagsMatcher(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdModifyTx: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, Query, lsn_t> *>(v.first);
				assert(cd);
				CoroTransaction tr = std::move(std::get<0>(cd->arguments));
				Error err(errLogic, "Connection pointer in transaction is nullptr.");
				if (tr.rpcClient_) {
					WrSerializer ser;
					std::get<1>(cd->arguments).Serialize(ser);
					switch (std::get<1>(cd->arguments).type_) {
						case QueryUpdate: {
							err = tr.getConn()
									  ->Call({cproto::kCmdUpdateQueryTx, tr.requestTimeout_, tr.execTimeout_, std::get<2>(cd->arguments),
											  -1, IndexValueType::NotSet, nullptr, false},
											 ser.Slice(), tr.txId_)
									  .Status();
							break;
						}
						case QueryDelete: {
							err = tr.getConn()
									  ->Call({cproto::kCmdDeleteQueryTx, tr.requestTimeout_, tr.execTimeout_, std::get<2>(cd->arguments),
											  -1, IndexValueType::NotSet, nullptr, false},
											 ser.Slice(), tr.txId_)
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
				std::function<Error(std::string_view, ReplicationStateV2 &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::GetReplState, &conn.rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			default:
				assert(false);
				break;
		}
		conn.OnRequestDone();
	}
}

Error SyncCoroReindexerImpl::CommandsQueue::Push(net::ev::async &ev, DatabaseCommandBase *cmd) {
	std::unique_lock<std::mutex> lock(mtx_);
	if (!isValid_) {
		return Error(errNotValid, "SyncCoroReindexer command queue is invalidated");
	}
	queue_.push_back(cmd);
	lock.unlock();

	ev.send();
	return Error();
}
void SyncCoroReindexerImpl::CommandsQueue::Get(std::vector<DatabaseCommandBase *> &cmds) {
	std::lock_guard<std::mutex> lock(mtx_);
	cmds.swap(queue_);
}

void SyncCoroReindexerImpl::CommandsQueue::Invalidate(std::vector<DatabaseCommandBase *> &cmds) {
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
