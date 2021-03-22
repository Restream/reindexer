#include "synccororeindexerimpl.h"
#include <chrono>
#include <functional>
#include <future>
#include "client/coroqueryresults.h"
#include "client/cororeindexer.h"
#include "client/cororpcclient.h"
#include "client/itemimpl.h"
#include "client/syncorotransaction.h"
#include "coroutine/channel.h"
#include "net/cproto/coroclientconnection.h"
#include "net/cproto/cproto.h"
#include "net/ev/ev.h"

namespace reindexer {
namespace client {

using std::chrono::milliseconds;

SyncCoroReindexerImpl::SyncCoroReindexerImpl(const CoroReindexerConfig &conf) : conf_(conf) {}

SyncCoroReindexerImpl::~SyncCoroReindexerImpl() {
	if (loopThread_ && loopThread_->joinable()) {
		closeAsync_.send();
		loopThread_->join();
	}
}

Error SyncCoroReindexerImpl::Connect(const string &dsn, const client::ConnectOpts &opts) {
	std::promise<Error> isRunningPromise;
	auto isRunningFuture = isRunningPromise.get_future();
	loopThread_.reset(
		new std::thread([this, &isRunningPromise, dsn, opts]() { this->threadLoopFun(std::move(isRunningPromise), dsn, opts); }));
	isRunningFuture.wait();
	auto isRunning = isRunningFuture.get();
	return isRunning;
}

Error SyncCoroReindexerImpl::Stop() {
	if (loopThread_->joinable()) {
		closeAsync_.send();
		loopThread_->join();
	}
	return errOK;
}
Error SyncCoroReindexerImpl::OpenNamespace(string_view nsName, const InternalRdxContext &ctx, const StorageOpts &opts) {
	return sendCoomand<Error, string_view, const InternalRdxContext &, const StorageOpts &>(cmdNameOpenNamespace, nsName, ctx, opts);
}
Error SyncCoroReindexerImpl::AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx) {
	return sendCoomand<Error, const NamespaceDef &, const InternalRdxContext &>(cmdNameAddNamespace, nsDef, ctx);
}
Error SyncCoroReindexerImpl::CloseNamespace(string_view nsName, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const InternalRdxContext &>(cmdNameCloseNamespace, nsName, ctx);
}
Error SyncCoroReindexerImpl::DropNamespace(string_view nsName, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const InternalRdxContext &>(cmdNameDropNamespace, nsName, ctx);
}
Error SyncCoroReindexerImpl::TruncateNamespace(string_view nsName, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const InternalRdxContext &>(cmdNameTruncateNamespace, nsName, ctx);
}
Error SyncCoroReindexerImpl::RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const std::string &, const InternalRdxContext &>(cmdNameRenameNamespace, srcNsName, dstNsName,
																							ctx);
}
Error SyncCoroReindexerImpl::AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const IndexDef &, const InternalRdxContext &>(cmdNameAddIndex, nsName, index, ctx);
}
Error SyncCoroReindexerImpl::UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const IndexDef &, const InternalRdxContext &>(cmdNameUpdateIndex, nsName, index, ctx);
}
Error SyncCoroReindexerImpl::DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const IndexDef &, const InternalRdxContext &>(cmdNameDropIndex, nsName, index, ctx);
}
Error SyncCoroReindexerImpl::SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, string_view, const InternalRdxContext &>(cmdNameSetSchema, nsName, schema, ctx);
}
Error SyncCoroReindexerImpl::EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx) {
	return sendCoomand<Error, vector<NamespaceDef> &, EnumNamespacesOpts, const InternalRdxContext &>(cmdNameEnumNamespaces, defs, opts,
																									  ctx);
}
Error SyncCoroReindexerImpl::EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx) {
	return sendCoomand<Error, vector<string> &, const InternalRdxContext &>(cmdNameEnumDatabases, dbList, ctx);
}
Error SyncCoroReindexerImpl::Insert(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, Item &, const InternalRdxContext &>(cmdNameInsert, nsName, item, ctx);
}
Error SyncCoroReindexerImpl::Update(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, Item &, const InternalRdxContext &>(cmdNameUpdate, nsName, item, ctx);
}
Error SyncCoroReindexerImpl::Upsert(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, Item &, const InternalRdxContext &>(cmdNameUpsert, nsName, item, ctx);
}
Error SyncCoroReindexerImpl::Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCoomand<Error, const Query &, CoroQueryResults &, const InternalRdxContext &>(cmdNameUpdateQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Delete(string_view nsName, Item &item, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, Item &, const InternalRdxContext &>(cmdNameDelete, nsName, item, ctx);
}
Error SyncCoroReindexerImpl::Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCoomand<Error, const Query &, CoroQueryResults &, const InternalRdxContext &>(cmdNameDeleteQ, query, result.results_, ctx);
}
Error SyncCoroReindexerImpl::Select(string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, SyncCoroQueryResults &, const InternalRdxContext &>(cmdNameSelectS, query, result, ctx);
}
Error SyncCoroReindexerImpl::Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx) {
	return sendCoomand<Error, const Query &, SyncCoroQueryResults &, const InternalRdxContext &>(cmdNameSelectQ, query, result, ctx);
}
Error SyncCoroReindexerImpl::Commit(string_view nsName) { return sendCoomand<Error, string_view>(cmdNameCommit, nsName); }
Item SyncCoroReindexerImpl::NewItem(string_view nsName) { return sendCoomand<Item, string_view>(cmdNameNewItem, nsName); }

Error SyncCoroReindexerImpl::GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const string &, string &, const InternalRdxContext &>(cmdNameGetMeta, nsName, key, data, ctx);
}
Error SyncCoroReindexerImpl::PutMeta(string_view nsName, const string &key, const string_view &data, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, const string &, const string_view &, const InternalRdxContext &>(cmdNamePutMeta, nsName, key,
																											data, ctx);
}
Error SyncCoroReindexerImpl::EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx) {
	return sendCoomand<Error, string_view, vector<string> &, const InternalRdxContext &>(cmdNameEnumMeta, nsName, keys, ctx);
}
Error SyncCoroReindexerImpl::SubscribeUpdates(IUpdatesObserver *, const UpdatesFilters &, SubscriptionOpts) {
	assert(false);
	return errOK;
}
Error SyncCoroReindexerImpl::UnsubscribeUpdates(IUpdatesObserver *) {
	assert(false);
	return errOK;
}
Error SyncCoroReindexerImpl::GetSqlSuggestions(const string_view sqlQuery, int pos, vector<string> &suggestions) {
	return sendCoomand<Error, const string_view, int, vector<string> &>(cmdNameGetSqlSuggestions, sqlQuery, pos, suggestions);
}
Error SyncCoroReindexerImpl::Status(const InternalRdxContext &ctx) {
	return sendCoomand<Error, const InternalRdxContext &>(cmdNameStatus, ctx);
}

SyncCoroTransaction SyncCoroReindexerImpl::NewTransaction(string_view nsName, const InternalRdxContext &ctx) {
	std::unique_ptr<CoroTransaction> trPtr =
		sendCoomand<std::unique_ptr<CoroTransaction>, string_view, const InternalRdxContext &>(cmdNameNewTransaction, nsName, ctx);
	if (trPtr->Status().ok()) {
		return SyncCoroTransaction(*this, std::move(*trPtr.get()));
	}
	return SyncCoroTransaction(trPtr->Status(), *this);
}
Error SyncCoroReindexerImpl::CommitTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	Error err = sendCoomand<Error, CoroTransaction &, const InternalRdxContext &>(cmdNameCommitTransaction, tr.tr_, ctx);
	tr.isFree = true;
	return err;
}

Error SyncCoroReindexerImpl::RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx) {
	Error err = sendCoomand<Error, SyncCoroTransaction &, const InternalRdxContext &>(cmdNameRollBackTransaction, tr, ctx);
	tr.isFree = true;
	return err;
}

Error SyncCoroReindexerImpl::fetchResults(int flags, SyncCoroQueryResults &result) {
	return sendCoomand<Error, int, SyncCoroQueryResults &>(cmdNameFetchResults, flags, result);
}

Error SyncCoroReindexerImpl::addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode, lsn_t lsn) {
	return sendCoomand<Error, CoroTransaction &, Item &, ItemModifyMode, lsn_t>(cmdNameAddTxItem, tr.tr_, item, mode, lsn);
}

Error SyncCoroReindexerImpl::modifyTx(SyncCoroTransaction &tr, Query &&q, lsn_t lsn) {
	return sendCoomand<Error, CoroTransaction &, Query &, lsn_t>(cmdNameModifyTx, tr.tr_, q, lsn);
}

Item SyncCoroReindexerImpl::newItemTx(CoroTransaction &tr) { return sendCoomand<Item, CoroTransaction &>(cmdNameNewItemTx, tr); }

void SyncCoroReindexerImpl::threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts) {
	coroutine::channel<std::unique_ptr<commandDataBase>> chCommand;

	commandAsync.set(loop);
	commandAsync.set([this, &chCommand](net::ev::async &) {
		loop.spawn([this, &chCommand]() {
			std::unique_ptr<commandDataBase> d;
			while (commandQuery_.Pop(d)) {
				chCommand.push(std::move(d));
			}
		});
	});
	commandAsync.start();

	closeAsync_.set(loop);
	closeAsync_.set([this, &chCommand](net::ev::async &) { loop.spawn([&chCommand]() { chCommand.close(); }); });
	closeAsync_.start();

	auto coro = [this, &chCommand, dsn, opts, &isRunning]() {
		reindexer::client::CoroRPCClient rx(conf_);
		auto err = rx.Connect(dsn, loop, opts);
		isRunning.set_value(err);
		coroutine::wait_group wg;
		wg.add(conf_.syncRxCoroCount);
		for (unsigned n = 0; n < conf_.syncRxCoroCount; n++) {
			loop.spawn(std::bind(&SyncCoroReindexerImpl::coroInterpreter, this, std::ref(rx), std::ref(chCommand), std::ref(wg)));
		}
		wg.wait();
	};
	loop.spawn(coro);

	loop.run();
}

void SyncCoroReindexerImpl::coroInterpreter(reindexer::client::CoroRPCClient &rx,
											coroutine::channel<std::unique_ptr<commandDataBase>> &chCommand, coroutine::wait_group &wg) {
	using namespace std::placeholders;
	coroutine::wait_group_guard wgg(wg);
	while (true) {
		std::pair<std::unique_ptr<commandDataBase>, bool> v = chCommand.pop();
		if (v.second) {
			switch (v.first->id_) {
				case cmdNameOpenNamespace: {
					execCommand<Error, string_view, const InternalRdxContext &, const StorageOpts &>(
						v.first.get(), std::bind(&client::CoroRPCClient::OpenNamespace, &rx, _1, _2, _3));
					break;
				}
				case cmdNameAddNamespace: {
					execCommand<Error, const NamespaceDef &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::AddNamespace, &rx, _1, _2));
					break;
				}
				case cmdNameCloseNamespace: {
					execCommand<Error, string_view, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::CloseNamespace, &rx, _1, _2));
					break;
				}
				case cmdNameDropNamespace: {
					execCommand<Error, string_view, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::DropNamespace, &rx, _1, _2));
					break;
				}
				case cmdNameTruncateNamespace: {
					execCommand<Error, string_view, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::TruncateNamespace, &rx, _1, _2));
					break;
				}
				case cmdNameRenameNamespace: {
					execCommand<Error, string_view, const std::string &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::RenameNamespace, &rx, _1, _2, _3));
					break;
				}

				case cmdNameAddIndex: {
					execCommand<Error, string_view, const IndexDef &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::AddIndex, &rx, _1, _2, _3));
					break;
				}
				case cmdNameUpdateIndex: {
					execCommand<Error, string_view, const IndexDef &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::UpdateIndex, &rx, _1, _2, _3));
					break;
				}
				case cmdNameDropIndex: {
					execCommand<Error, string_view, const IndexDef &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::DropIndex, &rx, _1, _2, _3));
					break;
				}
				case cmdNameSetSchema: {
					execCommand<Error, string_view, string_view, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::SetSchema, &rx, _1, _2, _3));
					break;
				}
				case cmdNameEnumNamespaces: {
					execCommand<Error, vector<NamespaceDef> &, EnumNamespacesOpts, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::EnumNamespaces, &rx, _1, _2, _3));
					break;
				}
				case cmdNameEnumDatabases: {
					execCommand<Error, vector<string> &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::EnumDatabases, &rx, _1, _2));
					break;
				}
				case cmdNameInsert: {
					execCommand<Error, string_view, Item &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::Insert, &rx, _1, _2, _3));
					break;
				}
				case cmdNameUpdate: {
					execCommand<Error, string_view, Item &, const InternalRdxContext &>(
						v.first.get(),
						std::bind(static_cast<Error (client::CoroRPCClient::*)(string_view, Item &, const InternalRdxContext &)>(
									  &client::CoroRPCClient::Update),
								  &rx, _1, _2, _3));
					break;
				}
				case cmdNameUpsert: {
					execCommand<Error, string_view, Item &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::Upsert, &rx, _1, _2, _3));
					break;
				}
				case cmdNameUpdateQ: {
					execCommand<Error, const Query &, CoroQueryResults &, const InternalRdxContext &>(
						v.first.get(),
						std::bind(
							static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								&client::CoroRPCClient::Update),
							&rx, _1, _2, _3));
					break;
				}
				case cmdNameDelete: {
					execCommand<Error, string_view, Item &, const InternalRdxContext &>(
						v.first.get(),
						std::bind(static_cast<Error (client::CoroRPCClient::*)(string_view, Item &, const InternalRdxContext &)>(
									  &client::CoroRPCClient::Delete),
								  &rx, _1, _2, _3));
					break;
				}
				case cmdNameDeleteQ: {
					execCommand<Error, const Query &, CoroQueryResults &, const InternalRdxContext &>(
						v.first.get(),
						std::bind(
							static_cast<Error (client::CoroRPCClient::*)(const Query &, CoroQueryResults &, const InternalRdxContext &)>(
								&client::CoroRPCClient::Delete),
							&rx, _1, _2, _3));
					break;
				}

				case cmdNameNewItem: {
					execCommand<Item, string_view>(v.first.get(), std::bind(&client::CoroRPCClient::NewItem, &rx, _1));
					break;
				}
				case cmdNameSelectS: {
					commandData<Error, string_view, SyncCoroQueryResults &, const InternalRdxContext &> *cd =
						dynamic_cast<commandData<Error, string_view, SyncCoroQueryResults &, const InternalRdxContext &> *>(v.first.get());
					Error err = rx.Select(cd->p1_, cd->p2_.results_, cd->p3_);
					cd->ret->set_value(std::move(err));
					break;
				}
				case cmdNameSelectQ: {
					commandData<Error, const Query &, SyncCoroQueryResults &, const InternalRdxContext &> *cd =
						dynamic_cast<commandData<Error, const Query &, SyncCoroQueryResults &, const InternalRdxContext &> *>(
							v.first.get());
					Error err = rx.Select(cd->p1_, cd->p2_.results_, cd->p3_);
					cd->ret->set_value(std::move(err));
					break;
				}
				case cmdNameCommit: {
					execCommand<Error, string_view>(v.first.get(), std::bind(&client::CoroRPCClient::Commit, &rx, _1));
					break;
				}
				case cmdNameGetMeta: {
					execCommand<Error, string_view, const string &, string &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::GetMeta, &rx, _1, _2, _3, _4));
					break;
				}
				case cmdNamePutMeta: {
					execCommand<Error, string_view, const string &, const string_view &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::PutMeta, &rx, _1, _2, _3, _4));
					break;
				}
				case cmdNameEnumMeta: {
					execCommand<Error, string_view, vector<string> &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::EnumMeta, &rx, _1, _2, _3));
					break;
				}
				case cmdNameGetSqlSuggestions: {
					execCommand<Error, const string_view, int, vector<string> &>(
						v.first.get(), std::bind(&client::CoroRPCClient::GetSqlSuggestions, &rx, _1, _2, _3));
					break;
				}
				case cmdNameStatus: {
					execCommand<Error, const InternalRdxContext &>(v.first.get(), std::bind(&client::CoroRPCClient::Status, &rx, _1));
					break;
				}
				case cmdNameNewTransaction: {
					commandData<std::unique_ptr<CoroTransaction>, string_view, const InternalRdxContext &> *cd =
						dynamic_cast<commandData<std::unique_ptr<CoroTransaction>, string_view, const InternalRdxContext &> *>(
							v.first.get());
					if (cd != nullptr) {
						CoroTransaction coroTrans = rx.NewTransaction(cd->p1_, cd->p2_);
						std::unique_ptr<CoroTransaction> r(new CoroTransaction(std::move(coroTrans)));
						cd->ret->set_value(std::move(r));
					} else {
						assert(false);
					}
					break;
				}
				case cmdNameCommitTransaction: {
					execCommand<Error, CoroTransaction &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::CommitTransaction, &rx, _1, _2));
					break;
				}
				case cmdNameRollBackTransaction: {
					execCommand<Error, CoroTransaction &, const InternalRdxContext &>(
						v.first.get(), std::bind(&client::CoroRPCClient::RollBackTransaction, &rx, _1, _2));
					break;
				}

				case cmdNameFetchResults: {
					commandData<Error, int, SyncCoroQueryResults &> *cd =
						dynamic_cast<commandData<Error, int, SyncCoroQueryResults &> *>(v.first.get());
					CoroQueryResults &coroResults = cd->p2_.results_;
					auto ret = coroResults.conn_->Call(
						{reindexer::net::cproto::kCmdFetchResults, coroResults.requestTimeout_, milliseconds(0), lsn_t(), int(-1), nullptr},
						coroResults.queryID_, cd->p1_, coroResults.queryParams_.count + coroResults.fetchOffset_, coroResults.fetchAmount_);
					if (!ret.Status().ok()) {
						cd->ret->set_value(Error(errLogic));
						break;
					}

					auto args = ret.GetArgs(2);

					coroResults.fetchOffset_ += coroResults.queryParams_.count;

					string_view rawResult = p_string(args[0]);
					ResultSerializer ser(rawResult);

					ser.GetRawQueryParams(coroResults.queryParams_, nullptr);

					coroResults.rawResult_.assign(rawResult.begin() + ser.Pos(), rawResult.end());

					Error err;
					cd->ret->set_value(err);

					break;
				}
				case cmdNameNewItemTx: {
					commandData<Item, CoroTransaction &> *cd = dynamic_cast<commandData<Item, CoroTransaction &> *>(v.first.get());
					Item item = execNewItemTx(cd->p1_);
					cd->ret->set_value(std::move(item));
					break;
				}
				case cmdNameAddTxItem: {
					commandData<Error, CoroTransaction &, Item &, ItemModifyMode, lsn_t> *cd =
						dynamic_cast<commandData<Error, CoroTransaction &, Item &, ItemModifyMode, lsn_t> *>(v.first.get());
					Error err = execAddTxItem(cd->p1_, cd->p2_, cd->p3_, cd->p4_);
					cd->ret->set_value(err);
					break;
				}
				case cmdNameModifyTx: {
					commandData<Error, CoroTransaction &, Query &, lsn_t> *cd =
						dynamic_cast<commandData<Error, CoroTransaction &, Query &, lsn_t> *>(v.first.get());
					CoroTransaction &tr = cd->p1_;
					Error err(errLogic, "Connection pointer in transaction is nullptr.");
					if (tr.conn_) {
						WrSerializer ser;
						cd->p2_.Serialize(ser);
						err = tr.conn_
								  ->Call({cproto::kCmdUpdateQueryTx, tr.requestTimeout_, tr.execTimeout_, cd->p3_, int(-1), nullptr},
										 ser.Slice(), tr.txId_)
								  .Status();
					}
					cd->ret->set_value(err);
					break;
				}
				default:
					break;
			}
		} else {
			return;
		}
	}
}

Item SyncCoroReindexerImpl::execNewItemTx(CoroTransaction &tr) {
	if (!tr.rpcClient_) return Item(Error(errLogic, "rpcClient not set for client transaction"));
	return tr.rpcClient_->NewItem(tr.nsName_);
}

Error SyncCoroReindexerImpl::execAddTxItem(CoroTransaction &tr, Item &item, ItemModifyMode mode, lsn_t lsn) {
	auto itData = item.GetJSON();
	p_string itemData(&itData);
	if (tr.conn_) {
		for (int tryCount = 0;; tryCount++) {
			auto ret = tr.conn_->Call({net::cproto::kCmdAddTxItem, tr.requestTimeout_, tr.execTimeout_, lsn, int(-1), nullptr}, FormatJson,
									  itemData, mode, "", 0, tr.txId_);

			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated || tryCount > 2) return ret.Status();

				CoroQueryResults qr;
				InternalRdxContext ctx;
				ctx = ctx.WithTimeout(tr.execTimeout_);
				auto err = tr.rpcClient_->Select(Query(tr.nsName_).Limit(0), qr, ctx);
				if (!err.ok()) return Error(errLogic, "Can't update TagsMatcher");

				auto newItem = tr.NewItem();
				char *endp = nullptr;
				err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
				if (!err.ok()) return err;
				item = std::move(newItem);
			} else {
				break;
			}
		}
		return errOK;
	}
	return Error(errLogic, "Connection pointer in transaction is nullptr.");
}

void SyncCoroReindexerImpl::CommandQuery::Push(net::ev::async &ev, std::unique_ptr<commandDataBase> cmd) {
	{
		std::unique_lock<std::mutex> lock(mtx_);
		queue_.push(std::move(cmd));
	}
	ev.send();
}

bool SyncCoroReindexerImpl::CommandQuery::Pop(std::unique_ptr<commandDataBase> &c) {
	std::unique_lock<std::mutex> lock(mtx_);
	if (!queue_.empty()) {
		c = std::move(queue_.front());
		queue_.pop();
		return true;
	}
	return false;
}

}  // namespace client

}  // namespace reindexer
