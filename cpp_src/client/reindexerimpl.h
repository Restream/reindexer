#pragma once

#include <deque>
#include <future>
#include <thread>
#include "client/cororeindexer.h"
#include "client/inamespaces.h"
#include "client/queryresults.h"
#include "client/rpcformat.h"
#include "client/transaction.h"
#include "core/shardedmeta.h"
#include "estl/condition_variable.h"
#include "estl/fast_hash_map.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "estl/recycle_list.h"
#include "net/ev/ev.h"

namespace reindexer {
namespace client {

struct ConnectionsPoolData;
template <typename CmdT>
class Connection;
template <typename CmdT>
class ConnectionsPool;

class [[nodiscard]] ReindexerImpl : public std::enable_shared_from_this<ReindexerImpl> {
public:
	typedef QueryResults QueryResultsT;

	/// Create Reindexer database object
	ReindexerImpl(const ReindexerConfig&, uint32_t connCount, uint32_t threadsCount);
	/// Destrory Reindexer database object
	~ReindexerImpl();
	ReindexerImpl(const ReindexerImpl&) = delete;
	ReindexerImpl& operator=(const ReindexerImpl&) = delete;

	Error Connect(const DSN& dsn, const client::ConnectOpts& opts = client::ConnectOpts());
	void Stop();
	Error OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx, const StorageOpts& opts, const NsReplicationOpts& replOpts);
	Error AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx, const NsReplicationOpts& replOpts);
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const InternalRdxContext& ctx);
	Error AddIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error DropIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx);
	Error GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx);
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx);
	Error EnumDatabases(std::vector<std::string>& dbList, const InternalRdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx);
	Error Update(const Query& query, QueryResults& result, const InternalRdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, QueryResults& result, const InternalRdxContext& ctx);
	Error Delete(const Query& query, QueryResults& result, const InternalRdxContext& ctx);
	Error ExecSQL(std::string_view query, QueryResults& result, const InternalRdxContext& ctx);
	Error Select(const Query& query, QueryResults& result, const InternalRdxContext& ctx);
	Item NewItem(std::string_view nsName, const InternalRdxContext& ctx) noexcept;
	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx);
	Error GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data, const InternalRdxContext& ctx);
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx);
	Error DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx);
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions);
	Error Status(bool forceCheck, const InternalRdxContext& ctx);
	CoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) noexcept;
	Error CommitTransaction(Transaction& tr, QueryResults& results, const InternalRdxContext& ctx);
	Error RollBackTransaction(Transaction& tr, const InternalRdxContext& ctx);
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state, const InternalRdxContext& ctx);

	Error SaveNewShardingConfig(std::string_view config, int64_t sourceId, const InternalRdxContext& ctx) noexcept;
	Error ResetOldShardingConfig(int64_t sourceId, const InternalRdxContext& ctx) noexcept;
	Error ResetShardingConfigCandidate(int64_t sourceId, const InternalRdxContext& ctx) noexcept;
	Error RollbackShardingConfigCandidate(int64_t sourceId, const InternalRdxContext& ctx) noexcept;
	Error ApplyNewShardingConfig(int64_t sourceId, const InternalRdxContext& ctx) noexcept;

	Error Version(std::string& version, const InternalRdxContext& ctx);

private:
	friend class QueryResults;
	friend class Transaction;
	Error fetchResults(int flags, int offset, int limit, QueryResults& result);
	Error fetchResults(int flags, QueryResults& result);
	Error closeResults(QueryResults& result);
	Error addTxItem(Transaction& tr, Item&& item, ItemModifyMode mode, const InternalRdxContext& ctx);
	Error putTxMeta(Transaction& tr, std::string_view key, std::string_view value, const InternalRdxContext& ctx);
	Error setTxTm(Transaction& tr, TagsMatcher&& tm, const InternalRdxContext& ctx);
	Error modifyTx(Transaction& tr, Query&& q, const InternalRdxContext& ctx);
	Item newItemTx(CoroTransaction& tr);
	void threadLoopFun(uint32_t tid, std::promise<Error>& isRunning, const DSN& dsn, const client::ConnectOpts& opts);
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
		DbCmdExecSQL,
		DbCmdSelect,
		DbCmdGetMeta,
		DbCmdGetShardedMeta,
		DbCmdPutMeta,
		DbCmdEnumMeta,
		DbCmdDeleteMeta,
		DbCmdGetSqlSuggestions,
		DbCmdStatus,
		DbCmdNewTransaction,
		DbCmdCommitTransaction,
		DbCmdRollBackTransaction,
		DbCmdFetchResults,
		DbCmdFetchResultsParametrized,
		DbCmdCloseResults,
		DbCmdNewItemTx,
		DbCmdAddTxItem,
		DbCmdPutTxMeta,
		DbCmdModifyTx,
		DbCmdGetReplState,
		DbCmdSetTxTagsMatcher,
		DbCmdSaveNewShardingCfg,
		DbCmdResetOldShardingCfg,
		DbCmdResetConfigCandidate,
		DbCmdRollbackConfigCandidate,
		DbCmdApplyNewShardingCfg,
		DbCmdVersion,
	};

	template <typename T>
	struct [[nodiscard]] ResultType {
		T data RX_GUARDED_BY(mtx);
		bool ready RX_GUARDED_BY(mtx) = false;
		mutex mtx;
		condition_variable cv;
	};

	template <typename T>
	class [[nodiscard]] Future {
	public:
		Future(ResultType<T>* res) noexcept : res_(res) {}

		void wait() {
			if (res_) {
				unique_lock lck(res_->mtx);
				res_->cv.wait(lck, [this]() RX_REQUIRES(res_->mtx) noexcept { return res_->ready; });
			}
		}

	private:
		ResultType<T>* res_ = nullptr;
	};

	template <typename T>
	class [[nodiscard]] Promise {
	public:
		Promise() noexcept = default;
		Promise(ResultType<T>& res) noexcept : res_(&res) {}

		void set_value(T&& v) noexcept {
			if (res_) {
				lock_guard lck(res_->mtx);
				res_->data = std::move(v);
				res_->ready = true;
				res_->cv.notify_one();
			}
		}
		Future<T> get_future() noexcept { return Future(res_); }

	private:
		ResultType<T>* res_ = nullptr;
	};

	struct [[nodiscard]] DatabaseCommandDataBase {
		DatabaseCommandDataBase(CmdName _id, const InternalRdxContext& _ctx) noexcept : id(_id), ctx(_ctx) {}
		CmdName id;
		InternalRdxContext ctx;
		virtual ~DatabaseCommandDataBase() = default;
	};

	template <typename R, typename... P>
	struct [[nodiscard]] DatabaseCommandData : public DatabaseCommandDataBase {
		Promise<R> ret;
		std::tuple<P...> arguments;

		DatabaseCommandData(CmdName _id, const InternalRdxContext& _ctx, P&&... p)
			: DatabaseCommandDataBase(_id, _ctx), arguments(std::forward<P>(p)...) {}
		DatabaseCommandData(CmdName _id, const InternalRdxContext& _ctx, Promise<R>&& r, P&&... p)
			: DatabaseCommandDataBase(_id, _ctx), ret(std::move(r)), arguments(std::forward<P>(p)...) {}
		DatabaseCommandData(const DatabaseCommandData&) = delete;
		DatabaseCommandData(DatabaseCommandData&&) = default;
	};

	class [[nodiscard]] DatabaseCommand {
	public:
		DatabaseCommand(DatabaseCommandDataBase* cmd = nullptr) noexcept : cmd_(cmd) {}
		template <typename DatabaseCommandDataT>
		DatabaseCommand(DatabaseCommandDataT&& cmd) : owns_(true), cmd_(new DatabaseCommandData(std::forward<DatabaseCommandDataT>(cmd))) {}
		DatabaseCommand(DatabaseCommand&& r) noexcept : connIdx(r.connIdx), owns_(r.owns_), cmd_(r.cmd_) { r.owns_ = false; }
		DatabaseCommand(const DatabaseCommand& r) = delete;
		DatabaseCommand& operator=(DatabaseCommand&& r) {
			if (this != &r) {
				if (owns_) {
					delete cmd_;
				}
				owns_ = r.owns_;
				connIdx = r.connIdx;
				cmd_ = r.cmd_;
				r.owns_ = false;
			}
			return *this;
		}
		DatabaseCommand& operator=(const DatabaseCommand& r) = delete;
		~DatabaseCommand() {
			if (owns_) {
				delete cmd_;
			}
		}

		DatabaseCommandDataBase* Data() noexcept { return cmd_; }

		int32_t connIdx = -1;

	private:
		bool owns_ = false;
		DatabaseCommandDataBase* cmd_ = nullptr;
	};

	template <typename R, typename... Args>
	R sendCommand(CmdName c, const InternalRdxContext& ctx, Args&&... args) {
		return sendCommand<false, R, Args...>(nullptr, c, ctx, std::forward<Args>(args)...);
	}

	template <bool withClient, typename R, typename... Args>
	R sendCommand(const void* conn, CmdName c, const InternalRdxContext& ctx, Args&&... args) {
		if (!ctx.cmpl()) {
			ResultType<R> res;
			Promise<R> promise(res);
			auto future = promise.get_future();

			DatabaseCommandData<R, Args...> cmd(c, ctx, std::move(promise), std::forward<Args>(args)...);
			Error err;
			if constexpr (withClient) {
				err = commandsQueue_.Push(
					conn,
					DatabaseCommand(static_cast<DatabaseCommandDataBase*>(&cmd)));	// pointer to stack data. 'wait' in this function!
			} else {
				(void)conn;
				err = commandsQueue_.Push(
					DatabaseCommand(static_cast<DatabaseCommandDataBase*>(&cmd)));	// pointer to stack data. 'wait' in this function!
			}
			if (err.ok()) {
				future.wait();
				return RX_GET_WITHOUT_MUTEX_ANALYSIS { return std::move(res.data); }
				();
			}
			if constexpr (withClient) {
				if (err.code() == errParams) {
					return R(Error(errNetwork, "Request for invalid connection (probably this connection was broken and invalidated)"));
				}
			}
			return R(Error(errTerminated, "Client is not connected: {}", err.what()));
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
				return R(Error(errTerminated, "Client is not connected: {}", err.what()));
			}
			return R();
		}
		return R(Error(errForbidden, "Unable to use this method with completion callback"));
	}

	bool isNetworkError(const Error& err) { return err.code() == errNetwork; }
	bool isNetworkError(const CoroTransaction& tx) { return isNetworkError(tx.Status()); }
	bool isNetworkError(const Item& item) { return isNetworkError(item.Status()); }

	template <typename>
	struct CalculateCommandType;
	template <typename R, typename C, typename... Args>
	struct [[nodiscard]] CalculateCommandType<R (C::*)(Args...) const> {
		using type = DatabaseCommandData<R, Args...>;
	};

	// cmd may be owned by another thread and will be invalidated after this function
	template <typename F>
	void execCommand(DatabaseCommandDataBase* cmd, F&& fun) noexcept {
		auto cd = dynamic_cast<typename CalculateCommandType<decltype(&F::operator())>::type*>(cmd);
		if (cd) {
			auto r = std::apply(std::move(fun), cd->arguments);
			if constexpr (std::is_same_v<decltype(r), Error>) {
				if (cd->ctx.cmpl()) {
					try {
						cd->ctx.cmpl()(r);
					} catch (std::exception& e) {
						fprintf(stderr, "reindexer error: unexpected exception in user's completion: %s\n", e.what());
						assertrx_dbg(false);
					} catch (...) {
						fprintf(stderr, "reindexer error: unexpected exception in user's completion\n");
						assertrx_dbg(false);
					}
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

	struct [[nodiscard]] WorkerThread {
		WorkerThread(uint32_t _connCount);
		~WorkerThread();

		std::thread th;
		std::unique_ptr<ConnectionsPoolData> connData;
		net::ev::async commandAsync;
		net::ev::async closeAsync;
		net::ev::dynamic_loop loop;
		const uint32_t connCount;
	};
	class [[nodiscard]] CommandsQueue {
	public:
		Error Push(DatabaseCommand&& cmd) RX_REQUIRES(!mtx_) {
			uint32_t minReq = std::numeric_limits<uint32_t>::max();
			ThreadData* targetTh = nullptr;
			unique_lock lck(mtx_);
			if (!isValid_) {
				return Error(errNotValid, "Reindexer command queue is invalidated");
			}
			for (auto& th : thData_) {
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
			cmd.connIdx = -1;
			sharedQueue_.emplace_back(std::move(cmd));
			lck.unlock();

			if (notify) {
				targetTh->commandAsync.send();
			}
			return Error();
		}
		Error Push(const void* conn, DatabaseCommand&& cmd) RX_REQUIRES(!mtx_) {
			unique_lock lck(mtx_);
			if (!isValid_) {
				return Error(errNotValid, "Reindexer command queue is invalidated");
			}
			auto found = thByConns_.find(conn);
			if (found == thByConns_.end()) {
				return Error(errParams, "Request for incorrect connection");
			}
			auto connMeta = found->second;
			cmd.connIdx = connMeta.connIdx;
			auto& th = *connMeta.threadData;
			const bool notify = !th.isReading && (th.personalQueue.empty());
			th.isReading = true;
			th.personalQueue.emplace_back(std::move(cmd));
			lck.unlock();

			if (notify) {
				th.commandAsync.send();
			}
			return Error();
		}
		void Get(uint32_t tid, h_vector<DatabaseCommand, 16>& cmds) RX_REQUIRES(!mtx_);
		void OnCmdDone(uint32_t tid) RX_REQUIRES(!mtx_);
		void Invalidate(uint32_t tid, h_vector<DatabaseCommand, 16>& cmds) RX_REQUIRES(!mtx_);
		void Init(std::deque<WorkerThread>& threads) RX_REQUIRES(!mtx_);
		void SetValid() RX_REQUIRES(!mtx_);
		void ClearConnectionsMapping() RX_REQUIRES(!mtx_);
		void RegisterConn(uint32_t tid, const void* conn, uint32_t connIdx) RX_REQUIRES(!mtx_);

	private:
		struct [[nodiscard]] ThreadData {
			ThreadData(net::ev::async& async) noexcept : commandAsync(async) {}
			std::atomic<uint32_t> reqCnt = 0;
			h_vector<DatabaseCommand, 16> personalQueue;
			net::ev::async& commandAsync;
			bool isReading = false;
		};
		struct [[nodiscard]] ConnMeta {
			ThreadData* threadData = nullptr;
			uint32_t connIdx = std::numeric_limits<uint32_t>::max();
		};

		bool isValid_ RX_GUARDED_BY(mtx_) = false;
		RecyclingList<DatabaseCommand, 64> sharedQueue_ RX_GUARDED_BY(mtx_);
		fast_hash_map<const void*, ConnMeta> thByConns_ RX_GUARDED_BY(mtx_);
		std::deque<ThreadData> thData_ RX_GUARDED_BY(mtx_);
		mutex mtx_;
	};

	class [[nodiscard]] ThreadSafeError {
	public:
		void Set(Error e) noexcept RX_REQUIRES(!mtx_) {
			lock_guard lck(mtx_);
			err_ = std::move(e);
		}
		Error Get() noexcept RX_REQUIRES(!mtx_) {
			lock_guard lck(mtx_);
			return err_;
		}

	private:
		mutex mtx_;
		Error err_ RX_GUARDED_BY(mtx_);
	};

	CommandsQueue commandsQueue_;
	mutex workersMtx_;
	const ReindexerConfig conf_;
	std::deque<WorkerThread> workers_;
	std::atomic<uint32_t> runningWorkers_ = {0};
	ThreadSafeError lastError_;
	INamespaces::PtrT sharedNamespaces_;
	std::atomic<bool> requiresStatusCheck_ = {true};

	void coroInterpreter(Connection<DatabaseCommand>&, ConnectionsPool<DatabaseCommand>&, uint32_t tid) noexcept;
	void fetchResultsImpl(int flags, int offset, int limit, CoroQueryResults& qr);
};

}  // namespace client
}  // namespace reindexer
