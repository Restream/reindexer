#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/namespace.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
#include "client/transaction.h"
#include "core/keyvalue/p_string.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "estl/fast_hash_map.h"
#include "estl/shared_mutex.h"
#include "net/cproto/clientconnection.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "urlparser/urlparser.h"

namespace reindexer {

namespace client {

using std::string;
using std::atomic_bool;
using std::shared_ptr;
using std::chrono::seconds;

using namespace net;
class RPCClient {
public:
	typedef std::function<void(const Error &err)> Completion;
	RPCClient(const ReindexerConfig &config);
	RPCClient(const RPCClient &) = delete;
	RPCClient(RPCClient &&) = delete;
	RPCClient &operator=(const RPCClient &) = delete;
	RPCClient &operator=(RPCClient &&) = delete;
	~RPCClient();

	Error Connect(const string &dsn, const client::ConnectOpts &opts);
	Error Connect(const vector<pair<string, client::ConnectOpts>> &connectData);
	Error Stop();

	Error OpenNamespace(string_view nsName, const InternalRdxContext &ctx,
						const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef, const InternalRdxContext &ctx);
	Error CloseNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error DropNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error TruncateNamespace(string_view nsName, const InternalRdxContext &ctx);
	Error RenameNamespace(string_view srcNsName, const std::string &dstNsName, const InternalRdxContext &ctx);
	Error AddIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error UpdateIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error DropIndex(string_view nsName, const IndexDef &index, const InternalRdxContext &ctx);
	Error SetSchema(string_view nsName, string_view schema, const InternalRdxContext &ctx);
	Error EnumNamespaces(vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const InternalRdxContext &ctx);
	Error EnumDatabases(vector<string> &dbList, const InternalRdxContext &ctx);
	Error Insert(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Update(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Upsert(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, QueryResults &result, const InternalRdxContext &ctx);
	Error Select(string_view query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx);
	}
	Error Select(const Query &query, QueryResults &result, const InternalRdxContext &ctx, cproto::ClientConnection *conn = nullptr) {
		return selectImpl(query, result, conn, config_.RequestTimeout, ctx);
	}
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName);
	Error GetMeta(string_view nsName, const string &key, string &data, const InternalRdxContext &ctx);
	Error PutMeta(string_view nsName, const string &key, const string_view &data, const InternalRdxContext &ctx);
	Error EnumMeta(string_view nsName, vector<string> &keys, const InternalRdxContext &ctx);
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts = SubscriptionOpts());
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	Error GetSqlSuggestions(string_view query, int pos, std::vector<std::string> &suggests);
	Error Status();

	Transaction NewTransaction(string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(Transaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(Transaction &tr, const InternalRdxContext &ctx);

protected:
	struct worker {
		worker() : running(false) {}
		ev::dynamic_loop loop_;
		std::thread thread_;
		ev::async stop_;
		atomic_bool running;
	};
	Error selectImpl(string_view query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout,
					 const InternalRdxContext &ctx);
	Error selectImpl(const Query &query, QueryResults &result, cproto::ClientConnection *, seconds netTimeout,
					 const InternalRdxContext &ctx);
	Error modifyItem(string_view nsName, Item &item, int mode, seconds netTimeout, const InternalRdxContext &ctx);
	Error modifyItemAsync(string_view nsName, Item *item, int mode, cproto::ClientConnection *, seconds netTimeout,
						  const InternalRdxContext &ctx);
	Error subscribeImpl(bool subscribe);
	Namespace *getNamespace(string_view nsName);
	Error startWorkers();
	Error addConnectEntry(const string &dsn, const client::ConnectOpts &opts, size_t idx);
	void run(int thIdx);
	void onUpdates(net::cproto::RPCAnswer &ans, cproto::ClientConnection *conn);
	bool onConnectionFail(int failedDsnIndex);

	void checkSubscribes();

	net::cproto::ClientConnection *getConn();

	std::vector<std::unique_ptr<net::cproto::ClientConnection>> connections_;

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	shared_timed_mutex nsMutex_;
	std::vector<worker> workers_;
	std::atomic<unsigned> curConnIdx_;
	ReindexerConfig config_;
	UpdatesObservers observers_;
	std::atomic<net::cproto::ClientConnection *> updatesConn_;
	vector<net::cproto::RPCAnswer> delayedUpdates_;
	cproto::ClientConnection::ConnectData connectData_;
};

void vec2pack(const h_vector<int32_t, 4> &vec, WrSerializer &ser);

}  // namespace client
}  // namespace reindexer
