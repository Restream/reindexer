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

using reindexer::net::cproto::RPCAnswer;

RPCClient::RPCClient(const ReindexerConfig& config) : workers_(config.WorkerThreads), config_(config), updatesConn_(nullptr) {
	curConnIdx_ = 0;
}

RPCClient::~RPCClient() { Stop(); }

Error RPCClient::Connect(const string& dsn) {
	if (connections_.size()) {
		return Error(errLogic, "Client is already started");
	}

	if (!uri_.parse(dsn)) {
		return Error(errParams, "%s is not valid uri", dsn);
	}
	if (uri_.scheme() != "cproto") {
		return Error(errParams, "Scheme must be cproto");
	}

	connections_.resize(config_.ConnPoolSize);
	for (unsigned i = 0; i < workers_.size(); i++) {
		workers_[i].thread_ = std::thread([this](int i) { this->run(i); }, i);
		while (!workers_[i].running) std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	return errOK;
}

Error RPCClient::Stop() {
	if (!connections_.size()) return errOK;
	for (auto& worker : workers_) {
		worker.stop_.send();

		if (worker.thread_.joinable()) {
			worker.thread_.join();
		}
	}
	connections_.clear();
	return errOK;
}

void RPCClient::run(int thIdx) {
	bool terminate = false;

	workers_[thIdx].stop_.set(workers_[thIdx].loop_);
	workers_[thIdx].stop_.set([&](ev::async& sig) {
		terminate = true;
		sig.loop.break_loop();
	});

	workers_[thIdx].stop_.start();

	for (int i = thIdx; i < config_.ConnPoolSize; i += config_.WorkerThreads) {
		connections_[i].reset(new cproto::ClientConnection(workers_[thIdx].loop_, &uri_));
	}

	ev::periodic checker;
	if (thIdx == 0) {
		checker.set(workers_[thIdx].loop_);
		checker.set([this](ev::periodic&, int) { checkSubscribes(); });
		checker.start(5, 5);
	}

	workers_[thIdx].running = true;
	for (;;) {
		workers_[thIdx].loop_.run();
		bool doTerminate = terminate;
		if (doTerminate) {
			for (int i = thIdx; i < config_.ConnPoolSize; i += config_.WorkerThreads) {
				if (connections_[i]->PendingCompletions()) {
					doTerminate = false;
				}
			}
		}
		if (doTerminate) break;
	}
	for (int i = thIdx; i < config_.ConnPoolSize; i += config_.WorkerThreads) {
		connections_[i].reset();
	}
}

Error RPCClient::AddNamespace(const NamespaceDef& nsDef) {
	WrSerializer ser;
	nsDef.GetJSON(ser);
	auto status = getConn()->Call(cproto::kCmdOpenNamespace, ser.Slice()).Status();

	if (!status.ok()) return status;

	std::unique_lock<shared_timed_mutex> lock(nsMutex_);
	namespaces_.emplace(nsDef.name, Namespace::Ptr(new Namespace(nsDef.name)));

	return errOK;
}

Error RPCClient::OpenNamespace(string_view nsName, const StorageOpts& sopts) {
	NamespaceDef nsDef(nsName.ToString(), sopts);
	return AddNamespace(nsDef);
}

Error RPCClient::DropNamespace(string_view nsName) { return getConn()->Call(cproto::kCmdDropNamespace, nsName).Status(); }
Error RPCClient::CloseNamespace(string_view nsName) { return getConn()->Call(cproto::kCmdCloseNamespace, nsName).Status(); }
Error RPCClient::Insert(string_view nsName, Item& item, Completion cmpl) { return modifyItem(nsName, item, ModeInsert, cmpl); }
Error RPCClient::Update(string_view nsName, Item& item, Completion cmpl) { return modifyItem(nsName, item, ModeUpdate, cmpl); }
Error RPCClient::Upsert(string_view nsName, Item& item, Completion cmpl) { return modifyItem(nsName, item, ModeUpsert, cmpl); }
Error RPCClient::Delete(string_view nsName, Item& item, Completion cmpl) { return modifyItem(nsName, item, ModeDelete, cmpl); }

Error RPCClient::modifyItem(string_view nsName, Item& item, int mode, Completion cmpl) {
	if (cmpl) {
		return modifyItemAsync(nsName, &item, mode, cmpl);
	}

	WrSerializer ser;
	if (item.impl_->GetPrecepts().size()) {
		ser.PutVarUint(item.impl_->GetPrecepts().size());
		for (auto& p : item.impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}

	for (int tryCount = 0;; tryCount++) {
		auto conn = getConn();
		auto ret =
			conn->Call(cproto::kCmdModifyItem, nsName, int(FormatCJson), item.GetCJSON(), mode, ser.Slice(), item.GetStateToken(), 0);
		if (!ret.Status().ok()) {
			if (ret.Status().code() != errStateInvalidated || tryCount > 2) return ret.Status();
			QueryResults qr;
			Select(Query(nsName.ToString()).Limit(0), qr);
			auto newItem = NewItem(nsName);
			char* endp = nullptr;
			Error err = newItem.FromJSON(item.impl_->GetJSON(), &endp);
			if (!err.ok()) return err;

			item = std::move(newItem);
			continue;
		}
		try {
			auto args = ret.GetArgs(2);
			NSArray nsArray{getNamespace(nsName)};
			return QueryResults(conn, std::move(nsArray), nullptr, p_string(args[0]), int(args[1])).Status();
		} catch (const Error& err) {
			return err;
		}
	}
}

Error RPCClient::modifyItemAsync(string_view nsName, Item* item, int mode, Completion clientCompl, cproto::ClientConnection* conn) {
	WrSerializer ser;
	if (item->impl_->GetPrecepts().size()) {
		ser.PutVarUint(item->impl_->GetPrecepts().size());
		for (auto& p : item->impl_->GetPrecepts()) {
			ser.PutVString(p);
		}
	}
	if (!conn) conn = getConn();

	string ns = nsName.ToString();
	conn->Call(
		[this, ns, mode, item, clientCompl](const net::cproto::RPCAnswer& ret, cproto::ClientConnection* conn) -> void {
			if (!ret.Status().ok()) {
				if (ret.Status().code() != errStateInvalidated) return clientCompl(ret.Status());
				// State invalidated - make select to update state
				QueryResults* qr = new QueryResults;
				Select(Query(ns).Limit(0), *qr,
					   [=](const Error& ret) {
						   delete qr;
						   if (!ret.ok()) return clientCompl(ret);

						   // Rebuild item with new state
						   auto newItem = NewItem(ns);

						   Error err = newItem.FromJSON(item->impl_->GetJSON());
						   newItem.SetPrecepts(item->impl_->GetPrecepts());
						   *item = std::move(newItem);
						   modifyItemAsync(ns, item, mode, clientCompl, conn);
					   },
					   conn);
			} else
				try {
					auto args = ret.GetArgs(2);
					clientCompl(QueryResults(conn, {getNamespace(ns)}, nullptr, p_string(args[0]), int(args[1])).Status());
				} catch (const Error& err) {
					clientCompl(err);
				}
		},
		cproto::kCmdModifyItem, ns, int(FormatCJson), item->GetCJSON(), mode, ser.Slice(), item->GetStateToken(), 0);
	return errOK;
}

Item RPCClient::NewItem(string_view nsName) {
	try {
		auto ns = getNamespace(nsName);
		return ns->NewItem();
	} catch (const Error& err) {
		return Item(err);
	}
}

Error RPCClient::GetMeta(string_view nsName, const string& key, string& data) {
	try {
		auto ret = getConn()->Call(cproto::kCmdGetMeta, nsName, key);
		if (ret.Status().ok()) {
			data = ret.GetArgs(1)[0].As<string>();
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::PutMeta(string_view nsName, const string& key, const string_view& data) {
	return getConn()->Call(cproto::kCmdPutMeta, nsName, key, data).Status();
}

Error RPCClient::EnumMeta(string_view nsName, vector<string>& keys) {
	try {
		auto ret = getConn()->Call(cproto::kCmdEnumMeta, nsName);
		if (ret.Status().ok()) {
			auto args = ret.GetArgs();
			keys.clear();
			keys.reserve(args.size());
			for (auto& k : args) {
				keys.push_back(k.As<string>());
			}
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

Error RPCClient::Delete(const Query& query, QueryResults& result) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	result = QueryResults(conn, {}, nullptr);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	auto ret = conn->Call(cproto::kCmdDeleteQuery, ser.Slice());
	icompl(ret, conn);
	return ret.Status();
}

Error RPCClient::Update(const Query& query, QueryResults& result) {
	WrSerializer ser;
	query.Serialize(ser);
	auto conn = getConn();

	result = QueryResults(conn, {}, nullptr);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection*) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	auto ret = conn->Call(cproto::kCmdUpdateQuery, ser.Slice());
	icompl(ret, conn);
	return ret.Status();
}

void vec2pack(const h_vector<int32_t, 4>& vec, WrSerializer& ser) {
	// Get array of payload Type Versions

	ser.PutVarUint(vec.size());
	for (auto v : vec) ser.PutVarUint(v);
	return;
}

Error RPCClient::Select(string_view query, QueryResults& result, Completion clientCompl, cproto::ClientConnection* conn) {
	int flags = result.fetchFlags_ ? result.fetchFlags_ : kResultsJson;
	WrSerializer pser;
	h_vector<int32_t, 4> vers;
	vec2pack(vers, pser);

	if (!conn) conn = getConn();

	result = QueryResults(conn, {}, clientCompl, result.fetchFlags_);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}

			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!clientCompl) {
		auto ret = conn->Call(cproto::kCmdSelectSQL, query, flags, INT_MAX, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, cproto::kCmdSelectSQL, query, flags, INT_MAX, pser.Slice());
		return errOK;
	}
}

Error RPCClient::Select(const Query& query, QueryResults& result, Completion clientCompl, cproto::ClientConnection* conn) {
	WrSerializer qser, pser;
	int flags = result.fetchFlags_ ? result.fetchFlags_ : (kResultsWithPayloadTypes | kResultsCJson);
	NSArray nsArray;
	query.Serialize(qser);
	query.WalkNested(true, true, [this, &nsArray](const Query q) { nsArray.push_back(getNamespace(q._namespace)); });
	h_vector<int32_t, 4> vers;
	for (auto& ns : nsArray) {
		shared_lock<shared_timed_mutex> lck(ns->lck_);
		vers.push_back(ns->tagsMatcher_.version() ^ ns->tagsMatcher_.stateToken());
	}
	vec2pack(vers, pser);

	if (!conn) conn = getConn();

	result = QueryResults(conn, std::move(nsArray), clientCompl, result.fetchFlags_);

	auto icompl = [&result](const RPCAnswer& ret, cproto::ClientConnection* /*conn*/) {
		try {
			if (ret.Status().ok()) {
				auto args = ret.GetArgs(2);
				result.Bind(p_string(args[0]), int(args[1]));
			}
			result.completion(ret.Status());
		} catch (const Error& err) {
			result.completion(err);
		}
	};

	if (!clientCompl) {
		auto ret = conn->Call(cproto::kCmdSelect, qser.Slice(), flags, 100, pser.Slice());
		icompl(ret, conn);
		return ret.Status();
	} else {
		conn->Call(icompl, cproto::kCmdSelect, qser.Slice(), flags, 100, pser.Slice());
		return errOK;
	}
}

Error RPCClient::Commit(string_view nsName) { return getConn()->Call(cproto::kCmdCommit, nsName).Status(); }

Error RPCClient::AddIndex(string_view nsName, const IndexDef& iDef) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return getConn()->Call(cproto::kCmdAddIndex, nsName, ser.Slice()).Status();
}

Error RPCClient::UpdateIndex(string_view nsName, const IndexDef& iDef) {
	WrSerializer ser;
	iDef.GetJSON(ser);
	return getConn()->Call(cproto::kCmdUpdateIndex, nsName, ser.Slice()).Status();
}

Error RPCClient::DropIndex(string_view nsName, const IndexDef& idx) {
	return getConn()->Call(cproto::kCmdDropIndex, nsName, idx.name_).Status();
}

Error RPCClient::EnumNamespaces(vector<NamespaceDef>& defs, bool bEnumAll) {
	try {
		auto ret = getConn()->Call(cproto::kCmdEnumNamespaces, bEnumAll ? 1 : 0);
		if (ret.Status().ok()) {
			auto json = ret.GetArgs(1)[0].As<string>();
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
	} catch (const Error& err) {
		return err;
	}
}
Error RPCClient::SubscribeUpdates(IUpdatesObserver* observer, bool subscribe) {
	if (subscribe) {
		observers_.Add(observer);
	} else {
		observers_.Delete(observer);
	}
	subscribe = !observers_.empty();
	Error err;
	auto updatesConn = updatesConn_.load();
	if (subscribe && !updatesConn) {
		auto conn = getConn();
		err = conn->Call(cproto::kCmdSubscribeUpdates, 1).Status();
		if (err.ok()) {
			updatesConn_ = conn;
		}
		conn->SetUpdatesHandler([this](const RPCAnswer& ans, cproto::ClientConnection* conn) { onUpdates(ans, conn); });
	} else if (!subscribe && updatesConn) {
		err = updatesConn->Call(cproto::kCmdSubscribeUpdates, 0).Status();
		updatesConn_ = nullptr;
	}
	return err;
}
Error RPCClient::GetSqlSuggestions(string_view query, int pos, std::vector<std::string>& suggests) {
	try {
		auto ret = getConn()->Call(cproto::kCmdGetSQLSuggestions, query, pos);
		if (ret.Status().ok()) {
			auto rargs = ret.GetArgs();
			suggests.clear();
			suggests.reserve(rargs.size());

			for (auto& rarg : rargs) suggests.push_back(rarg.As<string>());
		}
		return ret.Status();
	} catch (const Error& err) {
		return err;
	}
}

void RPCClient::checkSubscribes() {
	bool subscribe = !observers_.empty();

	auto updatesConn = updatesConn_.load();
	if (subscribe && !updatesConn_) {
		getConn()->Call(

			[this](const RPCAnswer& ans, cproto::ClientConnection* conn) {
				if (ans.Status().ok()) {
					updatesConn_ = conn;
					observers_.OnConnectionState(errOK);
					conn->SetUpdatesHandler([this](const RPCAnswer& ans, cproto::ClientConnection* conn) { onUpdates(ans, conn); });
				}
			},
			cproto::kCmdSubscribeUpdates, 1);
	} else if (!subscribe && updatesConn) {
		updatesConn->Call([](const RPCAnswer&, cproto::ClientConnection*) {}, cproto::kCmdSubscribeUpdates, 0);
		updatesConn_ = nullptr;
	}
}

Namespace* RPCClient::getNamespace(string_view nsName) {
	nsMutex_.lock_shared();
	auto nsIt = namespaces_.find(nsName);
	if (nsIt != namespaces_.end()) {
		nsMutex_.unlock_shared();
		return nsIt->second.get();
	}
	nsMutex_.unlock_shared();

	nsMutex_.lock();
	nsIt = namespaces_.find(nsName);
	if (nsIt == namespaces_.end()) {
		string nsNames = nsName.ToString();
		nsIt = namespaces_.emplace(nsNames, Namespace::Ptr(new Namespace(nsNames))).first;
	}
	nsMutex_.unlock();
	return nsIt->second.get();
}

net::cproto::ClientConnection* RPCClient::getConn() {
	assert(connections_.size());
	auto conn = connections_.at(curConnIdx_++ % connections_.size()).get();
	assert(conn);
	return conn;
}

void RPCClient::onUpdates(const net::cproto::RPCAnswer& ans, cproto::ClientConnection* conn) {
	if (!ans.Status().ok()) {
		updatesConn_ = nullptr;
		observers_.OnConnectionState(ans.Status());
		return;
	}

	auto args = ans.GetArgs(3);
	int64_t lsn(args[0]);
	string_view nsName(args[1]);
	string_view pwalRec(args[2]);
	WALRecord wrec(pwalRec);

	if (wrec.type == WalItemModify) {
		// Special process for Item Modify
		auto ns = getNamespace(nsName);

		// Check if cjson with bundled tagsMatcher
		bool bundledTagsMatcher = wrec.itemModify.itemCJson.length() > 0 && wrec.itemModify.itemCJson[0] == TAG_END;

		ns->lck_.lock_shared();
		auto tmVersion = ns->tagsMatcher_.version();
		ns->lck_.unlock_shared();

		if (tmVersion < wrec.itemModify.tmVersion && !bundledTagsMatcher) {
			// If tags matcher has been updated, but there are no bundled tags matcher in cjson
			// Then we need ask server to update
			// printf("%s need update from %d to %d %02x\n", ns->name_.c_str(), ns->tagsMatcher_.version(), wrec.itemModify.tmVersion,
			// 	   wrec.itemModify.itemCJson[0]);
			QueryResults* qr = new QueryResults;
			// keep strings, string_view's are pointing to rx ring buffer, and can be invalidated
			string nsNameStr = nsName.ToString(), itemCJsonStr = wrec.itemModify.itemCJson.ToString();

			Select(Query(nsNameStr).Limit(0), *qr,
				   [=](const Error& /*err*/) {
					   delete qr;
					   WALRecord wrec1 = wrec;
					   wrec1.itemModify.itemCJson = itemCJsonStr;
					   observers_.OnWALUpdate(lsn, nsNameStr, wrec1);
				   },
				   conn);
			return;
		} else {
			// We have bundled tagsMatcher
			if (bundledTagsMatcher) {
				// printf("%s bundled tm %d to %d\n", ns->name_.c_str(), ns->tagsMatcher_.version(), wrec.itemModify.tmVersion);
				Serializer rdser(wrec.itemModify.itemCJson);
				rdser.GetVarUint();
				uint32_t tmOffset = rdser.GetUInt32();
				// read tags matcher update
				rdser.SetPos(tmOffset);
				std::unique_lock<shared_timed_mutex> lck(ns->lck_);
				ns->tagsMatcher_ = TagsMatcher();
				ns->tagsMatcher_.deserialize(rdser, wrec.itemModify.tmVersion, ns->tagsMatcher_.stateToken());
			}
		}
	}

	observers_.OnWALUpdate(lsn, nsName, wrec);
}

}  // namespace client
}  // namespace reindexer
