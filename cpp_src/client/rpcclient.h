#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "client/item.h"
#include "client/namespace.h"
#include "client/queryresults.h"
#include "client/reindexerconfig.h"
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
using std::shared_ptr;
using namespace net;
class RPCClient {
public:
	typedef std::function<void(const Error &err)> Completion;
	RPCClient(const ReindexerConfig &config);
	~RPCClient();

	Error Connect(const string &dsn);
	Error Stop();

	Error OpenNamespace(string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef);
	Error CloseNamespace(string_view nsName);
	Error DropNamespace(string_view nsName);
	Error AddIndex(string_view nsName, const IndexDef &index);
	Error UpdateIndex(string_view nsName, const IndexDef &index);
	Error DropIndex(string_view nsName, const IndexDef &index);
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	Error Insert(string_view nsName, client::Item &item, Completion completion = nullptr);
	Error Update(string_view nsName, client::Item &item, Completion completion = nullptr);
	Error Upsert(string_view nsName, client::Item &item, Completion completion = nullptr);
	Error Delete(string_view nsName, client::Item &item, Completion completion = nullptr);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(string_view query, QueryResults &result, Completion clientCompl = nullptr, cproto::ClientConnection * = nullptr);
	Error Select(const Query &query, QueryResults &result, Completion clientCompl = nullptr, cproto::ClientConnection * = nullptr);
	Error Commit(string_view nsName);
	Item NewItem(string_view nsName);
	Error GetMeta(string_view nsName, const string &key, string &data);
	Error PutMeta(string_view nsName, const string &key, const string_view &data);
	Error EnumMeta(string_view nsName, vector<string> &keys);
	Error SubscribeUpdates(IUpdatesObserver *observer, bool subscribe);

private:
	Error modifyItem(string_view nsName, Item &item, int mode, Completion);
	Error modifyItemAsync(string_view nsName, Item *item, int mode, Completion, cproto::ClientConnection * = nullptr);
	Namespace *getNamespace(string_view nsName);
	void run(int thIdx);
	void onUpdates(const net::cproto::RPCAnswer &ans, cproto::ClientConnection *conn);
	void checkSubscribes();

	net::cproto::ClientConnection *getConn();

	std::vector<std::unique_ptr<net::cproto::ClientConnection>> connections_;

	fast_hash_map<string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	shared_timed_mutex nsMutex_;
	httpparser::UrlParser uri_;
	struct worker {
		ev::dynamic_loop loop_;
		std::thread thread_;
		ev::async stop_;
		bool running = false;
	};
	std::vector<worker> workers_;
	std::atomic<unsigned> curConnIdx_;
	ReindexerConfig config_;
	UpdatesObservers observers_;
	std::atomic<net::cproto::ClientConnection *> updatesConn_;
};

}  // namespace client
}  // namespace reindexer
