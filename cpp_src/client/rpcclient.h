#pragma once

#include "client/coroqueryresults.h"
#include "client/corotransaction.h"
#include "client/inamespaces.h"
#include "client/internalrdxcontext.h"
#include "client/namespace.h"
#include "client/reindexerconfig.h"
#include "client/rpcformat.h"
#include "cluster/config.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "core/shardedmeta.h"
#include "coroutine/mutex.h"
#include "estl/lock.h"
#include "net/cproto/coroclientconnection.h"

namespace reindexer {

struct ReplicationStateV2;
struct ClusterOperationStatus;
class SnapshotChunk;
struct SnapshotOpts;
struct ClusterControlRequestData;

namespace sharding {
struct ShardingControlRequestData;
struct ShardingControlResponseData;
}  // namespace sharding
namespace client {

class ConnectOpts;
class Snapshot;

template <typename MtxT>
class [[nodiscard]] NamespacesImpl final : public INamespaces::IntrusiveT {
public:
	using MapT = fast_hash_map<std::string, std::shared_ptr<Namespace>, nocase_hash_str, nocase_equal_str, nocase_less_str>;
	void Add(const std::string& name) override {
		auto ns = std::make_shared<Namespace>(name);

		lock_guard ulck(mtx_);
		namespaces_.emplace(name, std::move(ns));
	}
	void Erase(std::string_view name) override {
		lock_guard ulck(mtx_);
		namespaces_.erase(name);
	}
	std::shared_ptr<Namespace> Get(std::string_view name) override RX_REQUIRES(!mtx_) {
		shared_lock slck(mtx_);
		auto nsIt = namespaces_.find(name);
		if (nsIt == namespaces_.end()) {
			slck.unlock();

			std::string nsName(name);
			auto nsPtr = std::make_shared<Namespace>(nsName);
			lock_guard ulck(mtx_);
			nsIt = namespaces_.find(name);
			if (nsIt == namespaces_.end()) {
				nsIt = namespaces_.emplace(std::move(nsName), std::move(nsPtr)).first;
			}
			return nsIt->second;
		}
		return nsIt->second;
	}

private:
	MtxT mtx_;
	MapT namespaces_;
};

using namespace net;
class [[nodiscard]] RPCClient {
public:
	using ConnectionStateHandlerT = std::function<void(const Error&)>;
	using NodeData = cluster::NodeData;
	using RaftInfo = cluster::RaftInfo;
	typedef std::function<void(const Error& err)> Completion;
	RPCClient(const ReindexerConfig& config, INamespaces::PtrT sharedNamespaces);
	RPCClient(const RPCClient&) = delete;
	RPCClient(RPCClient&&) = delete;
	RPCClient& operator=(const RPCClient&) = delete;
	RPCClient& operator=(RPCClient&&) = delete;
	~RPCClient();

	Error Connect(const DSN& dsn, ev::dynamic_loop& loop, const ConnectOpts& opts);
	void Stop();

	Error OpenNamespace(std::string_view nsName, const InternalRdxContext& ctx,
						const StorageOpts& opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts& replOpts = NsReplicationOpts());
	Error AddNamespace(const NamespaceDef& nsDef, const InternalRdxContext& ctx, const NsReplicationOpts& replOpts = NsReplicationOpts());
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const InternalRdxContext& ctx,
								   const StorageOpts& opts = StorageOpts().Enabled(), lsn_t version = lsn_t());
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext& ctx);
	Error RenameNamespace(std::string_view srcNsName, std::string_view dstNsName, const InternalRdxContext& ctx);
	Error AddIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error DropIndex(std::string_view nsName, const IndexDef& index, const InternalRdxContext& ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext& ctx);
	Error GetSchema(std::string_view nsName, int format, std::string& schema, const InternalRdxContext& ctx);
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const InternalRdxContext& ctx);
	Error EnumDatabases(std::vector<std::string>& dbList, const InternalRdxContext& ctx);
	Error Insert(std::string_view nsName, client::Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Insert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Insert(std::string_view nsName, std::string_view cjson, const InternalRdxContext& ctx) {
		return modifyItemRaw(nsName, cjson, ModeInsert, config_.NetTimeout, ctx);
	}
	Error Update(std::string_view nsName, client::Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Update(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Update(std::string_view nsName, std::string_view cjson, const InternalRdxContext& ctx) {
		return modifyItemRaw(nsName, cjson, ModeUpdate, config_.NetTimeout, ctx);
	}
	Error Upsert(std::string_view nsName, client::Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Upsert(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Upsert(std::string_view nsName, std::string_view cjson, const InternalRdxContext& ctx) {
		return modifyItemRaw(nsName, cjson, ModeUpsert, config_.NetTimeout, ctx);
	}
	Error Delete(std::string_view nsName, client::Item& item, RPCDataFormat format, const InternalRdxContext& ctx);
	Error Delete(std::string_view nsName, client::Item& item, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Delete(std::string_view nsName, std::string_view cjson, const InternalRdxContext& ctx) {
		return modifyItemRaw(nsName, cjson, ModeDelete, config_.NetTimeout, ctx);
	}
	Error Delete(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Update(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error ExecSQL(std::string_view query, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error Select(const Query& query, CoroQueryResults& result, const InternalRdxContext& ctx) {
		return selectImpl(query, result, config_.NetTimeout, ctx);
	}
	// This method must be noexcept for public API
	template <typename... Args>
	Item NewItem(std::string_view nsName, Args&&... args) noexcept {
		try {
			return getNamespace(nsName)->NewItem(std::forward<Args>(args)...);
		} catch (std::exception& err) {
			return Item(std::move(err));
		} catch (...) {
			return Item(Error(errSystem, "Unknow exception in Reindexer client"));
		}
	}
	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data, const InternalRdxContext& ctx);
	Error GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data, const InternalRdxContext& ctx);
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const InternalRdxContext& ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const InternalRdxContext& ctx);
	Error DeleteMeta(std::string_view nsName, const std::string& key, const InternalRdxContext& ctx);
	Error GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string>& suggests);
	Error Status(bool forceCheck, const InternalRdxContext& ctx);
	Error Version(std::string& version, const InternalRdxContext& ctx);
	bool RequiresStatusCheck() const noexcept { return conn_.IsRunning() && conn_.RequiresStatusCheck(); }

	// This method must be noexcept for public API
	CoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext& ctx) noexcept;
	Error CommitTransaction(CoroTransaction& tr, CoroQueryResults& result, const InternalRdxContext& ctx);
	Error RollBackTransaction(CoroTransaction& tr, const InternalRdxContext& ctx);
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state, const InternalRdxContext& ctx);
	Error SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const InternalRdxContext& ctx);
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const InternalRdxContext& ctx);
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const InternalRdxContext& ctx);
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const InternalRdxContext& ctx);

	Error SuggestLeader(const NodeData& suggestion, NodeData& response, const InternalRdxContext& ctx);
	Error LeadersPing(const NodeData& leader, const InternalRdxContext& ctx);
	Error GetRaftInfo(RaftInfo& info, const InternalRdxContext& ctx);
	Error ClusterControlRequest(const ClusterControlRequestData& request, const InternalRdxContext& ctx);

	int64_t AddConnectionStateObserver(ConnectionStateHandlerT callback);
	Error RemoveConnectionStateObserver(int64_t id);

	const cproto::CoroClientConnection* GetConnPtr() const noexcept { return &conn_; }

	typedef CoroQueryResults QueryResultsT;

	Error ShardingControlRequest(const sharding::ShardingControlRequestData& request, sharding::ShardingControlResponseData& response,
								 const InternalRdxContext& ctx) noexcept;

protected:
	Error selectImpl(const Query& query, CoroQueryResults& result, milliseconds netTimeout, const InternalRdxContext& ctx);
	Error modifyItemCJSON(std::string_view nsName, Item& item, CoroQueryResults* results, int mode, milliseconds netTimeout,
						  const InternalRdxContext& ctx);
	Error modifyItemFormat(std::string_view nsName, Item& item, RPCDataFormat format, int mode, milliseconds netTimeout,
						   const InternalRdxContext& ctx);
	Error modifyItemRaw(std::string_view nsName, std::string_view cjson, int mode, milliseconds netTimeout, const InternalRdxContext& ctx);
	std::shared_ptr<Namespace> getNamespace(std::string_view nsName);

	void onConnectionState(Error err) noexcept {
		const auto observers = observers_;
		for (auto& obs : observers) {
			obs.second(err);
		}
	}

	cproto::CommandParams mkCommand(cproto::CmdCode cmd, const InternalRdxContext* ctx = nullptr) const noexcept;
	cproto::CommandParams mkCommand(cproto::CmdCode cmd, cproto::CoroClientConnection::TimePointT requiredTs,
									const InternalRdxContext* ctx) const noexcept;
	static cproto::CommandParams mkCommand(cproto::CmdCode cmd, milliseconds netTimeout, const InternalRdxContext* ctx) noexcept;

	INamespaces::PtrT namespaces_;
	ReindexerConfig config_;
	cproto::CoroClientConnection conn_;
	bool terminate_ = false;
	ev::dynamic_loop* loop_ = nullptr;
	coroutine::mutex mtx_;
	fast_hash_map<int64_t, ConnectionStateHandlerT> observers_;

	friend class CoroTransaction;
};

void vec2pack(const h_vector<int32_t, 4>& vec, WrSerializer& ser);

}  // namespace client
}  // namespace reindexer
