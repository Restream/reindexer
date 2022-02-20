#pragma once

#include <deque>
#include <future>
#include <thread>
#include "client/cororeindexer.h"
#include "client/reindexerconfig.h"
#include "client/synccoroqueryresults.h"
#include "client/synccorotransaction.h"
#include "core/shardedmeta.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"

namespace reindexer {
namespace client {

struct ConnectionsPoolData;
template <typename CmdT>
class Connection;
template <typename CmdT>
class ConnectionsPool;

class SyncCoroReindexerImpl {
public:
	typedef SyncCoroQueryResults QueryResultsT;

	/// Create Reindexer database object
	SyncCoroReindexerImpl(const CoroReindexerConfig & = CoroReindexerConfig(), size_t connCount = 0);
	/// Destrory Reindexer database object
	~SyncCoroReindexerImpl();
	SyncCoroReindexerImpl(const SyncCoroReindexerImpl &) = delete;
	SyncCoroReindexerImpl &operator=(const SyncCoroReindexerImpl &) = delete;

	Error Connect(const std::string &dsn, const client::ConnectOpts &opts = client::ConnectOpts());
	Error Stop();
	Error OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx, const StorageOpts &opts, const NsReplicationOpts &replOpts);
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx, const NsReplicationOpts &replOpts);
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(std::vector<std::string> &dbList, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Commit(std::string_view nsName, const InternalRdxContext &ctx);
	Item NewItem(std::string_view nsName, const InternalRdxContext &ctx);
	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx);
	Error GetMeta(std::string_view nsName, const string &key, std::vector<ShardedMeta> &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx);
	Error GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions);
	Error Status(bool forceCheck, const InternalRdxContext &ctx);
	CoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(SyncCoroTransaction &tr, SyncCoroQueryResults &results, const InternalRdxContext &ctx);
	Error RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx);
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx);

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroTransaction;
	Error fetchResults(int flags, SyncCoroQueryResults &result);
	Error closeResults(SyncCoroQueryResults &result);
	Error addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode, lsn_t lsn);
	Error putTxMeta(SyncCoroTransaction &tr, std::string_view key, std::string_view value, lsn_t lsn);
	Error setTxTm(SyncCoroTransaction &tr, TagsMatcher &&tm, lsn_t lsn);
	Error modifyTx(SyncCoroTransaction &tr, Query &&q, lsn_t lsn);
	Item newItemTx(CoroTransaction &tr);
	void threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts);
	ConnectionsPoolData &getConnData();

	enum CmdName {
		DbCmdNone = 0,
		DbCmdOpenNamespace = 1,
		DbCmdAddNamespace,
		DbCmdCloseNamespace,
		DbCmdDropNamespace,
		DbCmdTruncateNamespace,
		DbCmdRenameNamespace,
		DbCmdAddIndex,
		DbCmdUpdateIndex,
		DbCmdDropIndex,
		DbCmdSetSchema,
		DbCmdGetSchema,
		DbCmdEnumNamespaces,
		DbCmdEnumDatabases,
		DbCmdInsert,
		DbCmdInsertQR,
		DbCmdUpdate,
		DbCmdUpdateQR,
		DbCmdUpsert,
		DbCmdUpsertQR,
		DbCmdUpdateQ,
		DbCmdDelete,
		DbCmdDeleteQR,
		DbCmdDeleteQ,
		DbCmdNewItem,
		DbCmdSelectS,
		DbCmdSelectQ,
		DbCmdCommit,
		DbCmdGetMeta,
		DbCmdGetShardedMeta,
		DbCmdPutMeta,
		DbCmdEnumMeta,
		DbCmdGetSqlSuggestions,
		DbCmdStatus,
		DbCmdNewTransaction,
		DbCmdCommitTransaction,
		DbCmdRollBackTransaction,
		DbCmdFetchResults,
		DbCmdCloseResults,
		DbCmdNewItemTx,
		DbCmdAddTxItem,
		DbCmdPutTxMeta,
		DbCmdModifyTx,
		DbCmdGetReplState,
		DbCmdSetTxTagsMatcher,
	};

	struct DatabaseCommandDataBase {
		DatabaseCommandDataBase(CmdName _id, const InternalRdxContext &_ctx) noexcept : id(_id), ctx(_ctx) {}
		CmdName id;
		InternalRdxContext ctx;
		virtual ~DatabaseCommandDataBase() = default;
	};

	template <typename R, typename... P>
	struct DatabaseCommandData : public DatabaseCommandDataBase {
		std::promise<R> ret;
		std::tuple<P...> arguments;

		DatabaseCommandData(CmdName _id, const InternalRdxContext &_ctx, P &&...p)
			: DatabaseCommandDataBase(_id, _ctx), arguments(std::forward<P>(p)...) {}
		DatabaseCommandData(CmdName _id, const InternalRdxContext &_ctx, std::promise<R> &&r, P &&...p)
			: DatabaseCommandDataBase(_id, _ctx), ret(std::move(r)), arguments(std::forward<P>(p)...) {}
		DatabaseCommandData(const DatabaseCommandData &) = delete;
		DatabaseCommandData(DatabaseCommandData &&) = default;
	};

	class DatabaseCommand {
	public:
		DatabaseCommand(DatabaseCommandDataBase *cmd = nullptr) : cmd_(cmd) {}
		template <typename DatabaseCommandDataT>
		DatabaseCommand(DatabaseCommandDataT &&cmd) : owns_(true), cmd_(new DatabaseCommandData(std::forward<DatabaseCommandDataT>(cmd))) {}
		DatabaseCommand(DatabaseCommand &&r) noexcept : owns_(r.owns_), cmd_(r.cmd_) { r.owns_ = false; }
		DatabaseCommand(const DatabaseCommand &r) = delete;
		DatabaseCommand &operator=(DatabaseCommand &&r) {
			if (this != &r) {
				if (owns_) {
					delete cmd_;
				}
				owns_ = r.owns_;
				cmd_ = r.cmd_;
				r.owns_ = false;
			}
			return *this;
		}
		DatabaseCommand &operator=(const DatabaseCommand &r) = delete;
		~DatabaseCommand() {
			if (owns_) {
				delete cmd_;
			}
		}

		DatabaseCommandDataBase *Data() noexcept { return cmd_; }

	private:
		bool owns_ = false;
		DatabaseCommandDataBase *cmd_;
	};

	template <typename R, typename... Args>
	R sendCommand(CmdName c, const InternalRdxContext &ctx, Args &&...args) {
		if (!ctx.cmpl()) {
			std::promise<R> promise;
			std::future<R> future = promise.get_future();
			DatabaseCommandData<R, Args...> cmd(c, ctx, std::move(promise), std::forward<Args>(args)...);
			auto err = commandsQueue_.Push(
				commandAsync_,
				DatabaseCommand(static_cast<DatabaseCommandDataBase *>(&cmd)));	 // pointer to stack data. 'wait' in this function!
			if (!err.ok()) {
				return R(Error(errTerminated, "Client is not connected"));
			}
			return future.get();
		}
		if constexpr (std::is_same_v<R, Error>) {
			DatabaseCommandData<R, Args...> cmd(c, ctx, std::forward<Args>(args)...);
			auto err = commandsQueue_.Push(commandAsync_, DatabaseCommand(std::move(cmd)));
			if (!err.ok()) {
				return R(Error(errTerminated, "Client is not connected: %s", err.what()));
			}
			return R();
		}
		return R(Error(errForbidden, "Unable to use this method with completion callback"));
	}

	bool isNetworkError(const Error &err) { return err.code() == errNetwork; }
	bool isNetworkError(const CoroTransaction &tx) { return isNetworkError(tx.Status()); }
	bool isNetworkError(const Item &item) { return isNetworkError(item.Status()); }

	template <typename R, typename... Args>
	void execCommand(DatabaseCommandDataBase *cmd, std::function<R(Args...)> &&fun) {
		auto cd = dynamic_cast<DatabaseCommandData<R, Args...> *>(cmd);
		if (cd) {
			R r = std::apply(std::move(fun), cd->arguments);
			if constexpr (std::is_same_v<R, Error>) {
				if (cd->ctx.cmpl()) {
					cd->ctx.cmpl()(r);
				} else {
					cd->ret.set_value(std::move(r));
				}
			} else {
				cd->ret.set_value(std::move(r));
			}
		} else {
			assert(false);
		}
	}

	class CommandsQueue {
	public:
		Error Push(net::ev::async &ev, DatabaseCommand &&cmd);
		void Get(std::vector<DatabaseCommand> &c);
		void Invalidate(std::vector<DatabaseCommand> &c);
		void Init();

	private:
		bool isValid_ = false;
		std::vector<DatabaseCommand> queue_;
		std::mutex mtx_;
	};
	CommandsQueue commandsQueue_;
	net::ev::dynamic_loop loop_;
	std::thread loopThread_;
	std::mutex loopThreadMtx_;
	net::ev::async commandAsync_;
	net::ev::async closeAsync_;
	const CoroReindexerConfig conf_;
	const size_t connCount_ = 1;
	std::unique_ptr<ConnectionsPoolData> connData_;

	void coroInterpreter(Connection<DatabaseCommand> &, ConnectionsPool<DatabaseCommand> &) noexcept;
};
}  // namespace client
}  // namespace reindexer
