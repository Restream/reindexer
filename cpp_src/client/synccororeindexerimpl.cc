#include "synccororeindexerimpl.h"
#include "client/cororpcclient.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

SyncCoroReindexerImpl::SyncCoroReindexerImpl(const ReindexerConfig &conf) : conf_(conf) {}

SyncCoroReindexerImpl::~SyncCoroReindexerImpl() {
	if (loopThread_->joinable()) {
		closeAsync_.send();
		loopThread_->join();
	}
}

Error SyncCoroReindexerImpl::Connect(const std::string &dsn, const client::ConnectOpts &opts) {
	std::unique_lock<std::mutex> lock(loopThreadMtx_);
	if (loopThread_) return Error(errLogic, "Client is already started");

	std::promise<Error> isRunningPromise;
	auto isRunningFuture = isRunningPromise.get_future();
	loopThread_.reset(
		new std::thread([this, &isRunningPromise, dsn, opts]() { this->threadLoopFun(std::move(isRunningPromise), dsn, opts); }));
	isRunningFuture.wait();
	auto isRunning = isRunningFuture.get();
	return isRunning;
}

Error SyncCoroReindexerImpl::Stop() {
	std::unique_lock<std::mutex> lock(loopThreadMtx_);
	if (loopThread_->joinable()) {
		closeAsync_.send();
		loopThread_->join();
	}
	return errOK;
}
Error SyncCoroReindexerImpl::OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx, const StorageOpts &opts) {
	return sendCommand<Error>(DbCmdOpenNamespace, std::forward<std::string_view>(nsName), ctx, opts);
}
Error SyncCoroReindexerImpl::AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdAddNamespace, nsDef, ctx);
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
	return sendCommand<Error>(DbCmdSetSchema, std::forward<std::string_view>(nsName), schema, ctx);
}
Error SyncCoroReindexerImpl::EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumNamespaces, defs, std::forward<EnumNamespacesOpts>(opts), ctx);
}
Error SyncCoroReindexerImpl::EnumDatabases(std::vector<std::string> &dbList, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumDatabases, dbList, ctx);
}
Error SyncCoroReindexerImpl::Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdInsert, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdate, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpsert, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdUpdateQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDelete, std::forward<std::string_view>(nsName), item, ctx);
}
Error SyncCoroReindexerImpl::Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdDeleteQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Select(std::string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdSelectS, std::forward<std::string_view>(query), result, ctx);
}
Error SyncCoroReindexerImpl::Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdSelectQ, query, result, ctx);
}
Error SyncCoroReindexerImpl::Commit(std::string_view nsName) {
	return sendCommand<Error>(DbCmdCommit, std::forward<std::string_view>(nsName));
}
Item SyncCoroReindexerImpl::NewItem(std::string_view nsName) {
	return sendCommand<Item>(DbCmdNewItem, std::forward<std::string_view>(nsName));
}

Error SyncCoroReindexerImpl::GetMeta(std::string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdGetMeta, std::forward<std::string_view>(nsName), key, data, ctx);
}
Error SyncCoroReindexerImpl::PutMeta(std::string_view nsName, const std::string &key, std::string_view data,
									 const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdPutMeta, std::forward<std::string_view>(nsName), key, data, ctx);
}
Error SyncCoroReindexerImpl::EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx) {
	return sendCommand<Error>(DbCmdEnumMeta, std::forward<std::string_view>(nsName), keys, ctx);
}
Error SyncCoroReindexerImpl::GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions) {
	return sendCommand<Error>(DbCmdGetSqlSuggestions, std::forward<std::string_view>(sqlQuery), std::forward<int>(pos), suggestions);
}
Error SyncCoroReindexerImpl::Status(const InternalRdxContext &ctx) { return sendCommand<Error>(DbCmdStatus, ctx); }

SyncCoroTransaction SyncCoroReindexerImpl::NewTransaction(std::string_view nsName, const InternalRdxContext &ctx) {
	CoroTransaction tx = sendCommand<CoroTransaction>(DbCmdNewTransaction, std::forward<std::string_view>(nsName), ctx);
	if (tx.Status().ok()) {
		return SyncCoroTransaction(std::move(tx), this);
	}
	return SyncCoroTransaction(tx.Status(), this);
}
Error SyncCoroReindexerImpl::CommitTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	Error err = sendCommand<Error>(DbCmdCommitTransaction, tr.tr_, ctx);
	tr = SyncCoroTransaction(errOK, this);
	return err;
}

Error SyncCoroReindexerImpl::RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	Error err = sendCommand<Error>(DbCmdRollBackTransaction, tr, ctx);
	tr = SyncCoroTransaction(errOK, this);
	return err;
}

Error SyncCoroReindexerImpl::fetchResults(int flags, SyncCoroQueryResults &result) {
	return sendCommand<Error>(DbCmdFetchResults, std::forward<int>(flags), result);
}

Error SyncCoroReindexerImpl::addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode) {
	return sendCommand<Error>(DbCmdAddTxItem, tr.tr_, std::move(item), std::forward<ItemModifyMode>(mode));
}

Error SyncCoroReindexerImpl::modifyTx(SyncCoroTransaction &tr, Query &&q) {
	return sendCommand<Error>(DbCmdModifyTx, tr.tr_, std::move(q));
}

Item SyncCoroReindexerImpl::newItemTx(CoroTransaction &tr) { return sendCommand<Item>(DbCmdNewItemTx, tr); }

void SyncCoroReindexerImpl::threadLoopFun(std::promise<Error> &&isRunning, const std::string &dsn, const client::ConnectOpts &opts) {
	coroutine::channel<DatabaseCommandBase *> chCommand;

	commandAsync_.set(loop_);
	commandAsync_.set([this, &chCommand](net::ev::async &) {
		loop_.spawn([this, &chCommand]() {
			std::vector<DatabaseCommandBase *> q;
			commandsQueue_.Get(q);
			for (auto c : q) {
				chCommand.push(c);
			}
		});
	});
	commandAsync_.start();

	closeAsync_.set(loop_);
	closeAsync_.set([this, &chCommand](net::ev::async &) { loop_.spawn([&chCommand]() { chCommand.close(); }); });
	closeAsync_.start();

	auto coro = [this, &chCommand, dsn, opts, &isRunning]() {
		reindexer::client::CoroRPCClient rx(conf_);
		auto err = rx.Connect(dsn, loop_, opts);
		isRunning.set_value(err);
		coroutine::wait_group wg;
		wg.add(conf_.rxClientCoroCount);
		for (unsigned n = 0; n < conf_.rxClientCoroCount; n++) {
			loop_.spawn(std::bind(&SyncCoroReindexerImpl::coroInterpreter, this, std::ref(rx), std::ref(chCommand), std::ref(wg)));
		}
		wg.wait();
	};
	loop_.spawn(coro);

	loop_.run();

	commandAsync_.stop();
	closeAsync_.stop();
}

void SyncCoroReindexerImpl::coroInterpreter(reindexer::client::CoroRPCClient &rx, coroutine::channel<DatabaseCommandBase *> &chCommand,
											coroutine::wait_group &wg) {
	using namespace std::placeholders;
	coroutine::wait_group_guard wgg(wg);
	for (std::pair<DatabaseCommandBase *, bool> v = chCommand.pop(); v.second == true; v = chCommand.pop()) {
		switch (v.first->id_) {
			case DbCmdOpenNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &, const StorageOpts &)> f =
					std::bind(&client::CoroRPCClient::OpenNamespace, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdAddNamespace: {
				std::function<Error(const NamespaceDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::AddNamespace, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdCloseNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::CloseNamespace, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDropNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::DropNamespace, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdTruncateNamespace: {
				std::function<Error(std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::TruncateNamespace, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdRenameNamespace: {
				std::function<Error(std::string_view, const std::string &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::RenameNamespace, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}

			case DbCmdAddIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::AddIndex, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpdateIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::UpdateIndex, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDropIndex: {
				std::function<Error(std::string_view, const IndexDef &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::DropIndex, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdSetSchema: {
				std::function<Error(std::string_view, std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::SetSchema, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumNamespaces: {
				std::function<Error(std::vector<NamespaceDef> &, EnumNamespacesOpts, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumNamespaces, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumDatabases: {
				std::function<Error(std::vector<std::string> &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumDatabases, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdInsert: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::Insert, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpdate: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpsert: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::Upsert, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdUpdateQ: {
				std::function<Error(const Query &, CoroQueryResults &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Update),
							  &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDelete: {
				std::function<Error(std::string_view, Item &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(std::string_view, Item &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdDeleteQ: {
				std::function<Error(const Query &, CoroQueryResults &, const InternalRdxContext &)> f =
					std::bind(static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								  &client::CoroRPCClient::Delete),
							  &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}

			case DbCmdNewItem: {
				std::function<Item(std::string_view)> f = std::bind(&client::CoroRPCClient::NewItem, &rx, _1);
				execCommand(v.first, f);
				break;
			}
			case DbCmdSelectS: {
				auto cd =
					dynamic_cast<DatabaseCommand<Error, std::string_view, SyncCoroQueryResults &, const InternalRdxContext &> *>(v.first);
				assertrx(cd);
				Error err = rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments).results_, std::get<2>(cd->arguments));
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdSelectQ: {
				auto cd =
					dynamic_cast<DatabaseCommand<Error, const Query &, SyncCoroQueryResults &, const InternalRdxContext &> *>(v.first);
				assertrx(cd);
				Error err = rx.Select(std::get<0>(cd->arguments), std::get<1>(cd->arguments).results_, std::get<2>(cd->arguments));
				cd->ret.set_value(std::move(err));
				break;
			}
			case DbCmdCommit: {
				std::function<Error(std::string_view)> f = std::bind(&client::CoroRPCClient::Commit, &rx, _1);
				execCommand(v.first, f);
				break;
			}
			case DbCmdGetMeta: {
				std::function<Error(std::string_view, const std::string &, std::string &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::GetMeta, &rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdPutMeta: {
				std::function<Error(std::string_view, const std::string &, std::string_view, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::PutMeta, &rx, _1, _2, _3, _4);
				execCommand(v.first, f);
				break;
			}
			case DbCmdEnumMeta: {
				std::function<Error(std::string_view, std::vector<std::string> &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::EnumMeta, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdGetSqlSuggestions: {
				std::function<Error(std::string_view, int, std::vector<std::string> &)> f =
					std::bind(&client::CoroRPCClient::GetSqlSuggestions, &rx, _1, _2, _3);
				execCommand(v.first, f);
				break;
			}
			case DbCmdStatus: {
				std::function<Error(const InternalRdxContext &)> f = std::bind(&client::CoroRPCClient::Status, &rx, _1);
				execCommand(v.first, f);
				break;
			}
			case DbCmdNewTransaction: {
				auto *cd = dynamic_cast<DatabaseCommand<CoroTransaction, std::string_view, const InternalRdxContext &> *>(v.first);
				assertrx(cd);
				CoroTransaction coroTrans = rx.NewTransaction(std::get<0>(cd->arguments), std::get<1>(cd->arguments));
				cd->ret.set_value(std::move(coroTrans));
				break;
			}
			case DbCmdCommitTransaction: {
				std::function<Error(CoroTransaction &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::CommitTransaction, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}
			case DbCmdRollBackTransaction: {
				std::function<Error(CoroTransaction &, const InternalRdxContext &)> f =
					std::bind(&client::CoroRPCClient::RollBackTransaction, &rx, _1, _2);
				execCommand(v.first, f);
				break;
			}

			case DbCmdFetchResults: {
				auto cd = dynamic_cast<DatabaseCommand<Error, int, SyncCoroQueryResults &> *>(v.first);
				assertrx(cd);
				CoroQueryResults &coroResults = std::get<1>(cd->arguments).results_;
				auto ret = coroResults.conn_->Call(
					{reindexer::net::cproto::kCmdFetchResults, coroResults.requestTimeout_, milliseconds(0), nullptr},
					coroResults.queryID_.main, std::get<0>(cd->arguments), coroResults.queryParams_.count + coroResults.fetchOffset_,
					coroResults.fetchAmount_, coroResults.queryID_.uid);
				if (!ret.Status().ok()) {
					cd->ret.set_value(Error(errLogic));
					break;
				}

				auto args = ret.GetArgs(2);

				coroResults.fetchOffset_ += coroResults.queryParams_.count;

				std::string_view rawResult = p_string(args[0]);
				ResultSerializer ser(rawResult);

				ser.GetRawQueryParams(coroResults.queryParams_, nullptr, ResultSerializer::AggsFlag::DontClearAggregations);

				coroResults.rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());

				Error err;
				cd->ret.set_value(err);

				break;
			}
			case DbCmdNewItemTx: {
				auto cd = dynamic_cast<DatabaseCommand<Item, CoroTransaction &> *>(v.first);
				assertrx(cd);
				Item item = execNewItemTx(std::get<0>(cd->arguments));
				cd->ret.set_value(std::move(item));
				break;
			}
			case DbCmdAddTxItem: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, Item, ItemModifyMode> *>(v.first);
				assertrx(cd);
				Error err = std::get<0>(cd->arguments).addTxItem(std::move(std::get<1>(cd->arguments)), std::get<2>(cd->arguments));
				cd->ret.set_value(err);
				break;
			}
			case DbCmdModifyTx: {
				auto cd = dynamic_cast<DatabaseCommand<Error, CoroTransaction &, Query> *>(v.first);
				assertrx(cd);
				CoroTransaction tr = std::get<0>(cd->arguments);
				Error err(errLogic, "Connection pointer in transaction is nullptr.");
				if (tr.conn_) {
					WrSerializer ser;
					std::get<1>(cd->arguments).Serialize(ser);
					err = tr.conn_->Call({cproto::kCmdUpdateQueryTx, tr.RequestTimeout_, tr.execTimeout_, nullptr}, ser.Slice(), tr.txId_)
							  .Status();
				}
				cd->ret.set_value(err);
				break;
			}
			case DbCmdNone:
				break;
		}
	}
}

Item SyncCoroReindexerImpl::execNewItemTx(CoroTransaction &tr) {
	if (!tr.rpcClient_) return Item(Error(errLogic, "rpcClient not set for client transaction"));
	return tr.rpcClient_->NewItem(tr.nsName_);
}

void SyncCoroReindexerImpl::CommandsQueue::Push(net::ev::async &ev, DatabaseCommandBase *cmd) {
	{
		std::unique_lock<std::mutex> lock(mtx_);
		queue_.emplace_back(cmd);
	}
	ev.send();
}
void SyncCoroReindexerImpl::CommandsQueue::Get(std::vector<DatabaseCommandBase *> &cmds) {
	std::unique_lock<std::mutex> lock(mtx_);
	cmds.swap(queue_);
}

}  // namespace client
}  // namespace reindexer
