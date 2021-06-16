#pragma once

#include <future>
#include <thread>
#include "client/cororeindexer.h"
#include "client/synccoroqueryresults.h"
#include "client/synccorotransaction.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"

namespace reindexer {
namespace client {
class SyncCoroTransaction;
class SyncCoroReindexerImpl {
public:
	/// Create Reindexer database object
	SyncCoroReindexerImpl(const ReindexerConfig & = ReindexerConfig());
	/// Destrory Reindexer database object
	~SyncCoroReindexerImpl();
	SyncCoroReindexerImpl(const SyncCoroReindexerImpl &) = delete;
	SyncCoroReindexerImpl &operator=(const SyncCoroReindexerImpl &) = delete;

	Error Connect(const std::string &dsn, const client::ConnectOpts &opts = client::ConnectOpts());
	Error Stop();
	Error OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(std::vector<std::string> &dbList, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Update(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, SyncCoroQueryResults &result, const InternalRdxContext &ctx);
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName);
	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx);
	Error GetSqlSuggestions(const std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions);
	Error Status(const InternalRdxContext &ctx);
	SyncCoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(SyncCoroTransaction &tr, const InternalRdxContext &ctx);

private:
	friend class SyncCoroQueryResults;
	friend class SyncCoroTransaction;
	Error fetchResults(int flags, SyncCoroQueryResults &result);
	Error addTxItem(SyncCoroTransaction &tr, Item &&item, ItemModifyMode mode);
	Error modifyTx(SyncCoroTransaction &tr, Query &&q);
	Item execNewItemTx(CoroTransaction &tr);
	Item newItemTx(CoroTransaction &tr);
	Error execAddTxItem(CoroTransaction &tr, Item &item, ItemModifyMode mode);
	void threadLoopFun(std::promise<Error> &&isRunning, const string &dsn, const client::ConnectOpts &opts);

	bool exit_ = false;
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
		DbCmdEnumNamespaces,
		DbCmdEnumDatabases,
		DbCmdInsert,
		DbCmdUpdate,
		DbCmdUpsert,
		DbCmdUpdateQ,
		DbCmdDelete,
		DbCmdDeleteQ,
		DbCmdNewItem,
		DbCmdSelectS,
		DbCmdSelectQ,
		DbCmdCommit,
		DbCmdGetMeta,
		DbCmdPutMeta,
		DbCmdEnumMeta,
		DbCmdGetSqlSuggestions,
		DbCmdStatus,
		DbCmdNewTransaction,
		DbCmdCommitTransaction,
		DbCmdRollBackTransaction,
		DbCmdFetchResults,
		DbCmdNewItemTx,
		DbCmdAddTxItem,
		DbCmdModifyTx,
	};

	struct DatabaseCommandBase {
		DatabaseCommandBase(CmdName id) : id_(id) {}
		CmdName id_;
		virtual ~DatabaseCommandBase() {}
	};

	template <typename R, typename... P>
	struct DatabaseCommand : public DatabaseCommandBase {
		std::promise<R> ret;
		std::tuple<P...> arguments;
		DatabaseCommand(CmdName id, std::promise<R> r, P &&... p)
			: DatabaseCommandBase(id), ret(std::move(r)), arguments(std::forward<P>(p)...) {}
	};

	template <typename R, typename... Args>
	R sendCommand(CmdName c, Args &&... args) {
		std::promise<R> promise;
		std::future<R> future = promise.get_future();
		DatabaseCommand<R, Args...> cmd(c, std::move(promise), std::forward<Args>(args)...);
		commandsQueue_.Push(commandAsync_, &cmd);
		future.wait();
		return future.get();
	}

	template <typename R, typename... Args>
	void execCommand(DatabaseCommandBase *cmd, const std::function<R(Args...)> &fun) {
		auto cd = dynamic_cast<DatabaseCommand<R, Args...> *>(cmd);
		if (cd) {
			R r = std::apply(fun, cd->arguments);
			cd->ret.set_value(std::move(r));
		} else {
			assert(false);
		}
	}

	class CommandsQueue {
	public:
		CommandsQueue() {}
		void Push(net::ev::async &ev, DatabaseCommandBase *cmd);
		void Get(std::vector<DatabaseCommandBase *> &cmds);

	private:
		std::vector<DatabaseCommandBase *> queue_;
		std::mutex mtx_;
	};

	CommandsQueue commandsQueue_;
	net::ev::dynamic_loop loop_;
	std::unique_ptr<std::thread> loopThread_;
	std::mutex loopThreadMtx_;
	net::ev::async commandAsync_;
	net::ev::async closeAsync_;
	const ReindexerConfig conf_;
	void coroInterpreter(reindexer::client::CoroRPCClient &rx, coroutine::channel<DatabaseCommandBase *> &chCommand,
						 coroutine::wait_group &wg);
};

}  // namespace client
}  // namespace reindexer
