#pragma once

#include <deque>
#include <future>
#include <thread>
#include "client/cororeindexer.h"
#include "client/inamespaces.h"
#include "client/reindexerconfig.h"
#include "client/synccoroqueryresults.h"
#include "client/synccorotransaction.h"
#include "core/shardedmeta.h"
#include "coroutine/channel.h"
#include "coroutine/waitgroup.h"
#include "estl/fast_hash_map.h"
#include "estl/recycle_list.h"
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
	SyncCoroReindexerImpl(const CoroReindexerConfig &, uint32_t connCount, uint32_t threadsCount);
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
	void threadLoopFun(uint32_t tid, std::promise<Error> &isRunning, const string &dsn, const client::ConnectOpts &opts);
	void stop();

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

	template <typename T>
	struct ResultType {
		T data;
		bool ready = false;
		std::mutex mtx;
		std::condition_variable cv;
	};

	template <typename T>
	class Future {
	public:
		Future(ResultType<T> *res) noexcept : res_(res) {}

		void wait() {
			if (res_) {
				std::unique_lock lck(res_->mtx);
				res_->cv.wait(lck, [this]() noexcept { return res_->ready; });
			}
		}

	private:
		ResultType<T> *res_ = nullptr;
	};

	template <typename T>
	class Promise {
	public:
		Promise() noexcept = default;
		Promise(ResultType<T> &res) noexcept : res_(&res) {}

		void set_value(T &&v) {
			if (res_) {
				std::lock_guard lck(res_->mtx);
				res_->data = std::move(v);
				res_->ready = true;
				res_->cv.notify_one();
			}
		}
		Future<T> get_future() noexcept { return Future(res_); }

	private:
		ResultType<T> *res_ = nullptr;
	};

	struct DatabaseCommandDataBase {
		DatabaseCommandDataBase(CmdName _id, const InternalRdxContext &_ctx) noexcept : id(_id), ctx(_ctx) {}
		CmdName id;
		InternalRdxContext ctx;
		virtual ~DatabaseCommandDataBase() = default;
	};

	template <typename R, typename... P>
	struct DatabaseCommandData : public DatabaseCommandDataBase {
		Promise<R> ret;
		std::tuple<P...> arguments;

		DatabaseCommandData(CmdName _id, const InternalRdxContext &_ctx, P &&...p)
			: DatabaseCommandDataBase(_id, _ctx), arguments(std::forward<P>(p)...) {}
		DatabaseCommandData(CmdName _id, const InternalRdxContext &_ctx, Promise<R> &&r, P &&...p)
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
		return sendCommand<false, R, Args...>(nullptr, c, ctx, std::forward<Args>(args)...);
	}

	template <bool withClient, typename R, typename... Args>
	R sendCommand(const void *conn, CmdName c, const InternalRdxContext &ctx, Args &&...args) {
		if (!ctx.cmpl()) {
			ResultType<R> res;
			Promise<R> promise(res);
			auto future = promise.get_future();

			DatabaseCommandData<R, Args...> cmd(c, ctx, std::move(promise), std::forward<Args>(args)...);
			Error err;
			if constexpr (withClient) {
				err = commandsQueue_.Push(
					conn,
					DatabaseCommand(static_cast<DatabaseCommandDataBase *>(&cmd)));	 // pointer to stack data. 'wait' in this function!
			} else {
				(void)conn;
				err = commandsQueue_.Push(
					DatabaseCommand(static_cast<DatabaseCommandDataBase *>(&cmd)));	 // pointer to stack data. 'wait' in this function!
			}
			if (err.ok()) {
				future.wait();
				return std::move(res.data);
			}
			if constexpr (withClient) {
				if (err.code() == errParams) {
					return R(Error(errNetwork, "Request for invalid connection (probably this connection was broken and invalidated)"));
				}
			}
			return R(Error(errTerminated, "Client is not connected"));
		}
		if constexpr (std::is_same_v<R, Error>) {
			DatabaseCommandData<R, Args...> cmd(c, ctx, std::forward<Args>(args)...);
			Error err;
			if constexpr (withClient) {
				err = commandsQueue_.Push(conn, DatabaseCommand(std::move(cmd)));
			} else {
				(void)conn;
				err = commandsQueue_.Push(DatabaseCommand(std::move(cmd)));
			}
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
			assertrx(false);
		}
	}

	struct WorkerThread {
		WorkerThread(uint32_t _connCount);
		~WorkerThread();

		std::thread th;
		std::unique_ptr<ConnectionsPoolData> connData;
		net::ev::async commandAsync;
		net::ev::async closeAsync;
		net::ev::dynamic_loop loop;
		const uint32_t connCount;
	};
	class CommandsQueue {
	public:
		Error Push(DatabaseCommand &&cmd) {
			uint32_t minReq = std::numeric_limits<uint32_t>::max();
			ThreadData *targetTh = nullptr;
			std::unique_lock<std::mutex> lck(mtx_);
			if (!isValid_) {
				return Error(errNotValid, "SyncCoroReindexer command queue is invalidated");
			}
			for (auto &th : thData_) {
				const auto reqCnt = th.reqCnt.load(std::memory_order_acquire);
				if (reqCnt < minReq) {
					minReq = reqCnt;
					targetTh = &th;
					if (minReq == 0) {
						break;
					}
				}
			}
			const bool notify = !targetTh->isReading;
			targetTh->isReading = true;
			sharedQueue_.emplace_back(std::move(cmd));
			lck.unlock();

			if (notify) {
				targetTh->commandAsync.send();
			}
			return Error();
		}
		Error Push(const void *conn, DatabaseCommand &&cmd) {
			std::unique_lock<std::mutex> lck(mtx_);
			if (!isValid_) {
				return Error(errNotValid, "SyncCoroReindexer command queue is invalidated");
			}
			auto found = thByConns_.find(conn);
			if (found == thByConns_.end()) {
				return Error(errParams, "Request for incorrect connection");
			}
			auto &th = *found->second;
			const bool notify = !th.isReading && (th.personalQueue.empty());
			th.isReading = true;
			th.personalQueue.emplace_back(std::move(cmd));
			lck.unlock();

			if (notify) {
				th.commandAsync.send();
			}
			return Error();
		}
		void Get(uint32_t tid, h_vector<DatabaseCommand, 16> &cmds);
		void OnCmdDone(uint32_t tid);
		void Invalidate(uint32_t tid, h_vector<DatabaseCommand, 16> &cmds);
		void Init(std::deque<WorkerThread> &threads);
		void SetValid();
		void ClearConnectionsMapping();
		void RegisterConn(uint32_t tid, const void *conn);

	private:
		struct ThreadData {
			ThreadData(net::ev::async &async) noexcept : commandAsync(async) {}
			std::atomic<uint32_t> reqCnt = 0;
			h_vector<DatabaseCommand, 16> personalQueue;
			net::ev::async &commandAsync;
			bool isReading = false;
		};

		bool isValid_ = false;
		RecyclingList<DatabaseCommand, 64> sharedQueue_;
		fast_hash_map<const void *, ThreadData *> thByConns_;
		std::deque<ThreadData> thData_;
		std::mutex mtx_;
	};
	struct ThreadSafeError {
		void Set(Error e) {
			std::lock_guard lck(mtx);
			err = std::move(e);
		}
		Error Get() {
			std::lock_guard lck(mtx);
			return err;
		}

		std::mutex mtx;
		Error err;
	};

	CommandsQueue commandsQueue_;
	std::mutex workersMtx_;
	const CoroReindexerConfig conf_;
	std::deque<WorkerThread> workers_;
	std::atomic<uint32_t> runningWorkers_ = {0};
	std::mutex errorMtx_;
	ThreadSafeError lastError_;
	INamespaces::PtrT sharedNamespaces_;

	void coroInterpreter(Connection<DatabaseCommand> &, ConnectionsPool<DatabaseCommand> &, uint32_t tid) noexcept;
};
}  // namespace client
}  // namespace reindexer
