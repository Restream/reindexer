#include "client/rpcclient.h"
#include <stdio.h>
#include "client/itemimpl.h"
#include "core/namespacedef.h"
#include "gason/gason.h"
#include "tools/errors.h"

using std::string;
using std::vector;

namespace reindexer {
namespace client {

RPCClient::RPCClient(const ReindexerConfig& config) : config_(config) {
	stop_.set(loop_);
	curConnIdx_ = -1;
}

RPCClient::~RPCClient() { Stop(); }

Error RPCClient::Connect(const string& dsn) {
	if (worker_.joinable()) {
		return Error(errLogic, "Client is already started");
	}

	if (!uri_.parse(dsn)) {
		return Error(errParams, "%s is not valid uri", dsn.c_str());
	}
	if (uri_.scheme() != "cproto") {
		return Error(errParams, "Scheme must be cproto");
	}

	curConnIdx_ = -1;
	worker_ = std::thread([&]() { this->run(); });

	while (curConnIdx_ == -1) {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	return errOK;
}

Error RPCClient::Stop() {
	stop_.send();
	if (worker_.joinable()) {
		worker_.join();
	}
	return errOK;
}

void RPCClient::run() {
	bool terminate = false;

	stop_.set([&](ev::async& sig) {
		terminate = true;
		sig.loop.break_loop();
	});
	stop_.start();
	for (int i = 0; i < config_.ConnPoolSize; i++) {
		connections_.push_back(std::unique_ptr<cproto::ClientConnection>(new cproto::ClientConnection(loop_)));
	}

	while (!terminate) {
		checkConnections();
		if (curConnIdx_ == -1) curConnIdx_ = 0;
		loop_.run();
	}
	connections_.clear();
}

void RPCClient::checkConnections() {
	for (auto& c : connections_) {
		if (!c->IsValid()) {
			string port = uri_.port().length() ? uri_.port() : string("6534");
			string dbName = uri_.path();
			if (dbName[0] == '/') dbName = dbName.substr(1);

			c->Connect(uri_.hostname() + ":" + port, uri_.username(), uri_.password(), dbName);
		}
	}
}

Error RPCClient::AddNamespace(const NamespaceDef& nsDef) {
	WrSerializer ser;
	nsDef.GetJSON(ser);
	auto status = getConn()->Call(cproto::kCmdOpenNamespace, ser.Slice()).Status();

	if (!status.ok()) return status;

	{
		std::unique_lock<shared_timed_mutex> lock(nsMutex_);
		namespaces_.emplace(nsDef.name, Namespace::Ptr(new Namespace(nsDef.name)));
	}

	QueryResults qr;
	return Select(Query(nsDef.name).Limit(1), qr);
}

Error RPCClient::OpenNamespace(const string& name, const StorageOpts& sopts) {
	NamespaceDef nsDef(name, sopts);
	return AddNamespace(nsDef);
}

Error RPCClient::DropNamespace(const string& name) { return getConn()->Call(cproto::kCmdDropNamespace, name).Status(); }
Error RPCClient::CloseNamespace(const string& name) { return getConn()->Call(cproto::kCmdCloseNamespace, name).Status(); }
Error RPCClient::Insert(const string& ns, Item& item) { return modifyItem(ns, item, ModeInsert); }
Error RPCClient::Update(const string& ns, Item& item) { return modifyItem(ns, item, ModeUpdate); }
Error RPCClient::Upsert(const string& ns, Item& item) { return modifyItem(ns, item, ModeUpsert); }
Error RPCClient::Delete(const string& ns, Item& item) { return modifyItem(ns, item, ModeDelete); }

Error RPCClient::modifyItem(const string& ns, Item& item, int mode) {
	WrSerializer ser;
	ser.PutVString(ns);
	ser.PutVarUint(FormatCJson);
	ser.PutSlice(item.GetCJSON());
	ser.PutVarUint(item.impl_->GetPrecepts().size());
	for (auto& p : item.impl_->GetPrecepts()) {
		ser.PutVString(p);
	}
	auto conn = getConn();
	auto ret = conn->Call(cproto::kCmdModifyItem, ser.Slice(), mode);

	auto args = ret.GetArgs();
	if (ret.Status().ok()) {
		if (args.size() < 2) {
			return Error(errParams, "Server returned %d args, but expected %d", int(args.size()), 1);
		}
		NSArray nsArray{getNamespace(ns)};
		QueryResults(conn, nsArray, p_string(args[0]), int(args[1]));
	}
	return ret.Status();
}

Item RPCClient::NewItem(const string& nsName) {
	try {
		auto ns = getNamespace(nsName);
		return ns->NewItem();
	} catch (const Error& err) {
		return Item(err);
	}
}

Error RPCClient::GetMeta(const string& ns, const string& key, string& data) {
	auto ret = getConn()->Call(cproto::kCmdGetMeta, ns, key);
	if (ret.Status().ok()) {
		data = ret.GetArgs()[0].As<string>();
	}
	return ret.Status();
}

Error RPCClient::PutMeta(const string& ns, const string& key, const string_view& data) {
	return getConn()->Call(cproto::kCmdPutMeta, ns, key, data).Status();
}

Error RPCClient::EnumMeta(const string& ns, vector<string>& keys) {
	auto ret = getConn()->Call(cproto::kCmdEnumMeta, ns);
	if (ret.Status().ok()) {
		auto args = ret.GetArgs();
		keys.clear();
		keys.reserve(args.size());
		for (auto& k : args) {
			keys.push_back(k.As<string>());
		}
	}
	return ret.Status();
}

Error RPCClient::Delete(const Query& query, QueryResults& result) {
	WrSerializer ser;
	query.Serialize(ser);
	auto ret = getConn()->Call(cproto::kCmdSelect, ser.Slice());

	(void)result;
	return ret.Status();
}

Error RPCClient::Select(const string& query, QueryResults& result) {
	int flags = kResultsWithPayloadTypes | kResultsWithCJson;
	auto ret = getConn()->Call(cproto::kCmdSelectSQL, query, flags, INT_MAX, int64_t(-1));

	(void)result;
	return ret.Status();
}

void vec2pack(const h_vector<int32_t, 4>& vec, WrSerializer& ser) {
	// Get array of payload Type Versions

	ser.PutVarUint(vec.size());
	for (auto v : vec) ser.PutVarUint(v);
	return;
}

Error RPCClient::Select(const Query& query, QueryResults& result) {
	try {
		int flags = kResultsWithPayloadTypes | kResultsWithCJson;

		WrSerializer qser, pser;
		query.Serialize(qser);

		NSArray nsArray;
		query.WalkNested(true, true, [this, &nsArray](const Query q) { nsArray.push_back(getNamespace(q._namespace)); });

		h_vector<int32_t, 4> vers;
		for (auto& ns : nsArray) vers.push_back(ns->tagsMatcher_.version() ^ ns->tagsMatcher_.cacheToken());
		vec2pack(vers, pser);

		auto conn = getConn();
		auto ret = conn->Call(cproto::kCmdSelect, qser.Slice(), flags, 100, int64_t(-1), pser.Slice());

		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			if (args.size() < 2) {
				return Error(errParams, "Server returned %d args, but expected %d", int(args.size()), 1);
			}
			result = QueryResults(conn, nsArray, p_string(args[0]), int(args[1]));
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::Commit(const string& ns) { return getConn()->Call(cproto::kCmdCommit, ns).Status(); }

Error RPCClient::ConfigureIndex(const string& ns, const string& index, const string& config) {
	return getConn()->Call(cproto::kCmdConfigureIndex, ns, index, config).Status();
}

Error RPCClient::AddIndex(const string& ns, const IndexDef& iDef) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return getConn()->Call(cproto::kCmdAddIndex, ns, ser.Slice()).Status();
}

Error RPCClient::DropIndex(const string& ns, const string& idx) { return getConn()->Call(cproto::kCmdDropIndex, ns, idx).Status(); }

Error RPCClient::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) {
	auto ret = getConn()->Call(cproto::kCmdEnumNamespaces, bEnumAll ? 1 : 0);
	if (ret.Status().ok()) {
		if (ret.GetArgs().size() < 1) {
			return Error(errParams, "Server returned %d args, but expected %d", int(ret.GetArgs().size()), 1);
		}

		auto json = ret.GetArgs()[0].As<string>();
		JsonAllocator jalloc;
		JsonValue jvalue;
		char* endp;

		int status = jsonParse(const_cast<char*>(json.c_str()), &endp, &jvalue, jalloc);

		if (status != JSON_OK) {
			return Error(errParseJson, "Malformed JSON with namespace indexes");
		}
		for (auto elem : jvalue) {
			if (!strcmp("items", elem->key) && elem->value.getTag() == JSON_ARRAY) {
				for (auto nselem : elem->value) {
					NamespaceDef def;
					if (def.FromJSON(nselem->value).ok()) {
						defs.push_back(def);
					}
				}
			}
		}
	}

	return ret.Status();
}

shared_ptr<Namespace> RPCClient::getNamespace(const string& nsName) {
	nsMutex_.lock();
	auto nsIt = namespaces_.find(nsName);

	if (nsIt == namespaces_.end()) {
		nsIt = namespaces_.emplace(nsName, Namespace::Ptr(new Namespace(nsName))).first;
		nsMutex_.unlock();
		QueryResults qr;
		Select(Query(nsName).Limit(1), qr);
	} else {
		nsMutex_.unlock();
	}
	assert(nsIt->second);
	return nsIt->second;
}

net::cproto::ClientConnection* RPCClient::getConn() {
	assert(connections_.size());
	int count = connections_.size();
	net::cproto::ClientConnection* conn = nullptr;
	do {
		conn = connections_.at(curConnIdx_++ % connections_.size()).get();
	} while (!conn->IsValid() && count--);
	return conn;
}

}  // namespace client
}  // namespace reindexer
