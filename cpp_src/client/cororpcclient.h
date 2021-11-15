#pragma once

#include <functional>
#include "client/coroqueryresults.h"
#include "client/cororeindexerconfig.h"
#include "client/corotransaction.h"
#include "client/internalrdxcontext.h"
#include "client/namespace.h"
#include "cluster/config.h"
#include "core/keyvalue/p_string.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "coroutine/mutex.h"
#include "coroutine/waitgroup.h"
#include "estl/fast_hash_map.h"
#include "net/cproto/coroclientconnection.h"
#include "urlparser/urlparser.h"

namespace reindexer {

struct ReplicationStateV2;
struct ClusterizationStatus;
class SnapshotChunk;
struct SnapshotOpts;
struct ClusterControlRequestData;

namespace client {

class Snapshot;

using namespace net;
class CoroRPCClient {
public:
	using ConnectionStateHandlerT = std::function<void(const Error &)>;
	using NodeData = cluster::NodeData;
	using RaftInfo = cluster::RaftInfo;
	typedef std::function<void(const Error &err)> Completion;
	CoroRPCClient(const CoroReindexerConfig &config);
	CoroRPCClient(const CoroRPCClient &) = delete;
	CoroRPCClient(CoroRPCClient &&) = delete;
	CoroRPCClient &operator=(const CoroRPCClient &) = delete;
	CoroRPCClient &operator=(CoroRPCClient &&) = delete;
	~CoroRPCClient();

	Error Connect(const string &dsn, ev::dynamic_loop &loop, const ConnectOpts &opts);
	Error Stop();

	Error OpenNamespace(std::string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts &replOpts = NsReplicationOpts());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx, const NsReplicationOpts &replOpts = NsReplicationOpts());
	Error CloseNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const InternalRdxContext &ctx,
								   const StorageOpts &opts = StorageOpts().Enabled(), lsn_t version = lsn_t());
	Error TruncateNamespace(std::string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const InternalRdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Insert(std::string_view nsName, client::Item &item, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, client::Item &item, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, client::Item &item, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, client::Item &item, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Delete(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx) {
		return selectImpl(query, result, config_.NetTimeout, ctx);
	}
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName);
	template <typename C>
	Item NewItem(std::string_view nsName, C &client, std::chrono::milliseconds execTimeout) {
		try {
			return getNamespace(nsName)->NewItem(client, execTimeout);
		} catch (const Error &err) {
			return Item(err);
		}
	}
	Error GetMeta(std::string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);
	Error GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string> &suggests);
	Error Status(bool forceCheck, const InternalRdxContext &ctx);

	CoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(CoroTransaction &tr, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error RollBackTransaction(CoroTransaction &tr, const InternalRdxContext &ctx);
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const InternalRdxContext &ctx);
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const InternalRdxContext &ctx);
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const InternalRdxContext &ctx);
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const InternalRdxContext &ctx);

	Error SuggestLeader(const NodeData &suggestion, NodeData &response, const InternalRdxContext &ctx);
	Error LeadersPing(const NodeData &leader, const InternalRdxContext &ctx);
	Error GetRaftInfo(RaftInfo &info, const InternalRdxContext &ctx);
	Error ClusterControlRequest(const ClusterControlRequestData &request, const InternalRdxContext &ctx);

	int64_t AddConnectionStateObserver(ConnectionStateHandlerT callback);
	Error RemoveConnectionStateObserver(int64_t id);

	typedef CoroQueryResults QueryResultsT;

protected:
	Error selectImpl(const Query &query, CoroQueryResults &result, milliseconds netTimeout, const InternalRdxContext &ctx);
	Error modifyItem(std::string_view nsName, Item &item, CoroQueryResults *results, int mode, milliseconds netTimeout,
					 const InternalRdxContext &ctx);
	Namespace *getNamespace(std::string_view nsName);

	void onConnectionState(Error err) noexcept {
		if (!err.ok()) {
			subscribed_ = false;
		}
		const auto observers = observers_;
		for (auto &obs : observers) {
			obs.second(err);
		}
	}

	cproto::CommandParams mkCommand(cproto::CmdCode cmd, const InternalRdxContext *ctx = nullptr) const noexcept;
	static cproto::CommandParams mkCommand(cproto::CmdCode cmd, milliseconds netTimeout, const InternalRdxContext *ctx) noexcept;

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	CoroReindexerConfig config_;
	cproto::CoroClientConnection conn_;
	bool subscribed_ = false;
	bool terminate_ = false;
	coroutine::wait_group resubWg_;
	ev::dynamic_loop *loop_ = nullptr;
	coroutine::mutex mtx_;
	fast_hash_map<int64_t, ConnectionStateHandlerT> observers_;

	friend class CoroTransaction;
};

void vec2pack(const h_vector<int32_t, 4> &vec, WrSerializer &ser);

}  // namespace client
}  // namespace reindexer
