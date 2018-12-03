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

	Error OpenNamespace(const string &_namespace, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing());
	Error AddNamespace(const NamespaceDef &nsDef);
	Error CloseNamespace(const string &_namespace);
	Error DropNamespace(const string &_namespace);
	Error AddIndex(const string &_namespace, const IndexDef &index);
	Error UpdateIndex(const string &_namespace, const IndexDef &index);
	Error DropIndex(const string &_namespace, const string &index);
	Error EnumNamespaces(vector<NamespaceDef> &defs, bool bEnumAll);
	Error Insert(const string &_namespace, client::Item &item, Completion completion = nullptr);
	Error Update(const string &_namespace, client::Item &item, Completion completion = nullptr);
	Error Upsert(const string &_namespace, client::Item &item, Completion completion = nullptr);
	Error Delete(const string &_namespace, client::Item &item, Completion completion = nullptr);
	Error Delete(const Query &query, QueryResults &result);
	Error Select(const string_view &query, QueryResults &result, Completion clientCompl = nullptr);
	Error Select(const Query &query, QueryResults &result, Completion clientCompl = nullptr);
	Error Commit(const string &namespace_);
	Item NewItem(const string &_namespace);
	Error GetMeta(const string &_namespace, const string &key, string &data);
	Error PutMeta(const string &_namespace, const string &key, const string_view &data);
	Error EnumMeta(const string &_namespace, vector<string> &keys);

private:
	Error modifyItem(const string &_namespace, Item &item, int mode, Completion);
	Error modifyItemAsync(const string &_namespace, Item *item, int mode, Completion);
	Namespace *getNamespace(const string &nsName);
	void run(int thIdx);

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
};

}  // namespace client
}  // namespace reindexer
