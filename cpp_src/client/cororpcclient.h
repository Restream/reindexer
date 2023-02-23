#pragma once

#include <functional>
#include <string>
#include "client/coroqueryresults.h"
#include "client/corotransaction.h"
#include "client/internalrdxcontext.h"
#include "client/item.h"
#include "client/namespace.h"
#include "client/reindexerconfig.h"
#include "core/keyvalue/p_string.h"
#include "core/namespacedef.h"
#include "core/query/query.h"
#include "coroutine/waitgroup.h"
#include "estl/fast_hash_map.h"
#include "net/cproto/coroclientconnection.h"
#include "replicator/updatesobserver.h"
#include "tools/errors.h"
#include "urlparser/urlparser.h"

namespace reindexer {

namespace client {

using std::chrono::seconds;

using namespace net;
class CoroRPCClient {
public:
	typedef std::function<void(const Error &err)> Completion;
	CoroRPCClient(const ReindexerConfig &config);
	CoroRPCClient(const CoroRPCClient &) = delete;
	CoroRPCClient(CoroRPCClient &&) = delete;
	CoroRPCClient &operator=(const CoroRPCClient &) = delete;
	CoroRPCClient &operator=(CoroRPCClient &&) = delete;
	~CoroRPCClient();

	Error Connect(const std::string &dsn, ev::dynamic_loop &loop, const client::ConnectOpts &opts);
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
	Error Insert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Update(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Upsert(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(std::string_view nsName, client::Item &item, const InternalRdxContext &ctx);
	Error Delete(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Update(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx);
	Error Select(std::string_view query, CoroQueryResults &result, const InternalRdxContext &ctx) {
		return selectImpl(query, result, config_.RequestTimeout, ctx);
	}
	Error Select(const Query &query, CoroQueryResults &result, const InternalRdxContext &ctx) {
		return selectImpl(query, result, config_.RequestTimeout, ctx);
	}
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName);
	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const InternalRdxContext &ctx);
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const InternalRdxContext &ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const InternalRdxContext &ctx);
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts = SubscriptionOpts());
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	Error GetSqlSuggestions(std::string_view query, int pos, std::vector<std::string> &suggests);
	Error Status(const InternalRdxContext &ctx);

	CoroTransaction NewTransaction(std::string_view nsName, const InternalRdxContext &ctx);
	Error CommitTransaction(CoroTransaction &tr, const InternalRdxContext &ctx);
	Error RollBackTransaction(CoroTransaction &tr, const InternalRdxContext &ctx);

protected:
	Error selectImpl(std::string_view query, CoroQueryResults &result, seconds netTimeout, const InternalRdxContext &ctx);
	Error selectImpl(const Query &query, CoroQueryResults &result, seconds netTimeout, const InternalRdxContext &ctx);
	Error modifyItem(std::string_view nsName, Item &item, int mode, seconds netTimeout, const InternalRdxContext &ctx);
	Error subscribeImpl(bool subscribe);
	Namespace *getNamespace(std::string_view nsName);
	Error addConnectEntry(const std::string &dsn, const client::ConnectOpts &opts, size_t idx);
	void onUpdates(const net::cproto::CoroRPCAnswer &ans);
	void startResubRoutine();

	void resubRoutine();
	void onConnFatalError(const Error &) noexcept { subscribed_ = false; }

	cproto::CommandParams mkCommand(cproto::CmdCode cmd, const InternalRdxContext *ctx = nullptr) const noexcept;
	static cproto::CommandParams mkCommand(cproto::CmdCode cmd, seconds reqTimeout, const InternalRdxContext *ctx) noexcept;

	fast_hash_map<std::string, Namespace::Ptr, nocase_hash_str, nocase_equal_str, nocase_less_str> namespaces_;

	ReindexerConfig config_;
	UpdatesObservers observers_;
	cproto::CoroClientConnection conn_;
	bool subscribed_ = false;
	bool terminate_ = false;
	coroutine::wait_group resubWg_;
	ev::dynamic_loop *loop_ = nullptr;
};

void vec2pack(const h_vector<int32_t, 4> &vec, WrSerializer &ser);

}  // namespace client
}  // namespace reindexer
